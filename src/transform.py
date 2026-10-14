"""
Étape 02b - Préparation des données (nettoyage + contrôles qualité).

Ce script :
1. lit la réponse brute la plus récente dans data/raw/,
2. la transforme en tableau (DataFrame pandas),
3. applique les transformations documentées ci-dessous,
4. exécute les contrôles qualité et les écrit dans logs/pipeline.log,
5. sauvegarde le résultat propre dans data/processed/taux_propres.csv.

Transformations appliquées (à reprendre dans le README) :
- T1 Renommage des colonnes en français : date_taux, devise_base, devise, taux
- T2 Conversion des types : date_taux -> date, taux -> nombre décimal, devises -> texte en majuscules
- T3 Filtre des devises : on ne garde que USD, CNY, CHF, GBP (exclut une éventuelle ligne EUR -> EUR)
- T4 Filtre de période : on retire les dates antérieures au 01/01/2025
     (l'API renvoie le dernier taux connu avant la date demandée, ici le 31/12/2024)
- T5 Valeurs manquantes : les lignes sans date ou sans taux sont retirées et comptées
- T6 Doublons : une seule ligne par couple (date_taux, devise)
- T7 Ajout de colonnes de traçabilité : source, date_collecte

Lancement : python src/transform.py
"""

import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

# --- Paramètres ---------------------------------------------------------------
DEVISES = ["USD", "CNY", "CHF", "GBP"]
DATE_DEBUT = "2025-01-01"
SOURCE = "BCE via API Frankfurter"

DOSSIER_RAW = Path("data/raw")
DOSSIER_PROCESSED = Path("data/processed")
DOSSIER_LOGS = Path("logs")


def configurer_logs() -> None:
    DOSSIER_LOGS.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(DOSSIER_LOGS / "pipeline.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def lire_dernier_brut() -> list[dict]:
    """Lit le fichier brut le plus récent (les noms contiennent la date, donc le tri suffit)."""
    fichiers = sorted(DOSSIER_RAW.glob("taux_bruts_*.json"))
    if not fichiers:
        raise FileNotFoundError("Aucun fichier brut dans data/raw/ : lance d'abord extract.py")
    logging.info("Lecture du fichier brut : %s", fichiers[-1])
    return json.loads(fichiers[-1].read_text(encoding="utf-8"))


def transformer(donnees: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(donnees)
    logging.info("Lignes brutes : %d", len(df))

    # T1 - Renommage
    df = df.rename(columns={"date": "date_taux", "base": "devise_base", "quote": "devise", "rate": "taux"})

    # T2 - Types (errors="coerce" : une valeur illisible devient vide au lieu de faire planter)
    df["date_taux"] = pd.to_datetime(df["date_taux"], errors="coerce").dt.date
    df["taux"] = pd.to_numeric(df["taux"], errors="coerce")
    df["devise"] = df["devise"].astype(str).str.strip().str.upper()
    df["devise_base"] = df["devise_base"].astype(str).str.strip().str.upper()

    # T3 - Devises utiles uniquement
    avant = len(df)
    df = df[df["devise"].isin(DEVISES)]
    logging.info("T3 devises hors périmètre retirées : %d", avant - len(df))

    # T5 - Valeurs manquantes (avant le filtre de période, qui a besoin de dates valides)
    avant = len(df)
    df = df.dropna(subset=["date_taux", "taux"])
    logging.info("T5 lignes avec date ou taux manquant retirées : %d", avant - len(df))

    # T4 - Période
    avant = len(df)
    df = df[df["date_taux"] >= date.fromisoformat(DATE_DEBUT)]
    logging.info("T4 lignes antérieures au %s retirées : %d", DATE_DEBUT, avant - len(df))

    # T6 - Doublons
    avant = len(df)
    df = df.drop_duplicates(subset=["date_taux", "devise"], keep="last")
    logging.info("T6 doublons (date_taux, devise) retirés : %d", avant - len(df))

    # T7 - Traçabilité
    df["source"] = SOURCE
    df["date_collecte"] = datetime.now(timezone.utc).replace(microsecond=0)

    return df.sort_values(["date_taux", "devise"]).reset_index(drop=True)


def controler_qualite(df: pd.DataFrame) -> bool:
    """Contrôles bloquants (ERROR) et informatifs (WARNING). Renvoie False si un contrôle bloque."""
    ok = True

    if df.empty:
        logging.error("CQ1 tableau vide après nettoyage")
        return False

    # CQ1 - Taux strictement positifs
    negatifs = (df["taux"] <= 0).sum()
    if negatifs:
        logging.error("CQ1 %d taux nuls ou négatifs", negatifs)
        ok = False
    else:
        logging.info("CQ1 OK : tous les taux sont > 0")

    # CQ2 - Pas de date dans le futur
    futures = (df["date_taux"] > date.today()).sum()
    if futures:
        logging.error("CQ2 %d dates dans le futur", futures)
        ok = False
    else:
        logging.info("CQ2 OK : aucune date future")

    # CQ3 - Chaque date a bien les 4 devises
    par_date = df.groupby("date_taux")["devise"].nunique()
    incompletes = par_date[par_date < len(DEVISES)]
    if len(incompletes):
        logging.warning("CQ3 %d dates incomplètes : %s", len(incompletes), list(incompletes.index[:5]))
    else:
        logging.info("CQ3 OK : %d dates, chacune avec les %d devises", len(par_date), len(DEVISES))

    # CQ4 - Pas de cotation le week-end (sinon donnée suspecte)
    week_end = pd.to_datetime(df["date_taux"]).dt.dayofweek >= 5
    if week_end.sum():
        logging.warning("CQ4 %d lignes datées d'un samedi ou dimanche", week_end.sum())
    else:
        logging.info("CQ4 OK : aucune cotation le week-end (absence normale, non comblée)")

    # CQ5 - Unicité de la clé
    if df.duplicated(subset=["date_taux", "devise"]).any():
        logging.error("CQ5 la clé (date_taux, devise) n'est pas unique")
        ok = False
    else:
        logging.info("CQ5 OK : clé (date_taux, devise) unique")

    return ok


def sauvegarder(df: pd.DataFrame) -> Path:
    DOSSIER_PROCESSED.mkdir(parents=True, exist_ok=True)
    chemin = DOSSIER_PROCESSED / "taux_propres.csv"
    df.to_csv(chemin, index=False, encoding="utf-8")
    return chemin


def main() -> int:
    configurer_logs()
    logging.info("=== Début de la transformation ===")
    try:
        df = transformer(lire_dernier_brut())
        if not controler_qualite(df):
            raise ValueError("Contrôle qualité bloquant en échec")
        chemin = sauvegarder(df)
        logging.info(
            "Succès : %d lignes propres, du %s au %s -> %s",
            len(df), df["date_taux"].min(), df["date_taux"].max(), chemin,
        )
        logging.info("=== Fin de la transformation : OK ===")
        return 0
    except Exception as erreur:
        logging.error("Échec de la transformation : %s", erreur)
        logging.info("=== Fin de la transformation : ÉCHEC ===")
        return 1


if __name__ == "__main__":
    sys.exit(main())
