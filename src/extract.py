"""
Étape 02a - Extraction des taux de change BCE via l'API Frankfurter.

Ce script :
1. appelle l'API (taux EUR -> USD, CNY, CHF, GBP depuis une date donnée),
2. contrôle les erreurs de réponse (réseau, code HTTP, JSON invalide, réponse vide),
3. sauvegarde la réponse brute dans data/raw/ (traçabilité),
4. écrit une trace de chaque exécution dans logs/pipeline.log.

Lancement : python src/extract.py
"""

import json
import logging
import sys
from datetime import date
from pathlib import Path

import requests

# --- Paramètres ---------------------------------------------------------------
API_URL = "https://api.frankfurter.dev/v2/providers/ecb/rates"  # taux officiels BCE
DEVISES = ["USD", "CNY", "CHF", "GBP"]
DATE_DEBUT = "2025-01-01"   # chargement historique initial
TIMEOUT_SECONDES = 30

DOSSIER_RAW = Path("data/raw")
DOSSIER_LOGS = Path("logs")


def configurer_logs() -> None:
    """Écrit les traces à la fois dans la console et dans logs/pipeline.log."""
    DOSSIER_LOGS.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(DOSSIER_LOGS / "pipeline.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def extraire_taux(date_debut: str, date_fin: str | None = None) -> list[dict]:
    """Appelle l'API et renvoie la liste des taux. Lève une erreur si quelque chose cloche."""
    params = {"from": date_debut, "quotes": ",".join(DEVISES)}
    if date_fin:
        params["to"] = date_fin

    logging.info("Appel API : %s | paramètres : %s", API_URL, params)

    # 1) Erreurs réseau (pas de connexion, serveur qui ne répond pas)
    try:
        reponse = requests.get(API_URL, params=params, timeout=TIMEOUT_SECONDES)
    except requests.exceptions.Timeout:
        raise RuntimeError(f"L'API n'a pas répondu en moins de {TIMEOUT_SECONDES} s")
    except requests.exceptions.ConnectionError:
        raise RuntimeError("Impossible de joindre l'API (problème de connexion)")

    # 2) Erreurs HTTP (400 paramètre invalide, 404 introuvable, 422 devise inconnue, 5xx panne)
    if reponse.status_code != 200:
        try:
            message = reponse.json().get("message", "")
        except ValueError:
            message = reponse.text[:200]
        raise RuntimeError(f"Erreur HTTP {reponse.status_code} : {message}")

    # 3) Réponse illisible
    try:
        donnees = reponse.json()
    except ValueError:
        raise RuntimeError("La réponse de l'API n'est pas un JSON valide")

    # 4) Réponse vide ou de forme inattendue
    if not isinstance(donnees, list) or len(donnees) == 0:
        raise RuntimeError("Réponse vide ou de format inattendu (liste attendue)")

    return donnees


def sauvegarder_brut(donnees: list[dict]) -> Path:
    """Garde une copie de la réponse brute, datée du jour d'exécution."""
    DOSSIER_RAW.mkdir(parents=True, exist_ok=True)
    chemin = DOSSIER_RAW / f"taux_bruts_{date.today().isoformat()}.json"
    chemin.write_text(json.dumps(donnees, ensure_ascii=False, indent=2), encoding="utf-8")
    return chemin


def main() -> int:
    configurer_logs()
    logging.info("=== Début de l'extraction ===")
    try:
        donnees = extraire_taux(DATE_DEBUT)
        dates = sorted({ligne["date"] for ligne in donnees})
        logging.info(
            "Succès : %d lignes reçues, du %s au %s", len(donnees), dates[0], dates[-1]
        )
        chemin = sauvegarder_brut(donnees)
        logging.info("Réponse brute sauvegardée : %s", chemin)
        logging.info("=== Fin de l'extraction : OK ===")
        return 0
    except Exception as erreur:
        logging.error("Échec de l'extraction : %s", erreur)
        logging.info("=== Fin de l'extraction : ÉCHEC ===")
        return 1  # code de sortie 1 = l'automatisation saura que le lancement a échoué


if __name__ == "__main__":
    sys.exit(main())
