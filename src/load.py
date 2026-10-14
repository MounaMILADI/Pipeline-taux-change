"""
Étape 03 - Chargement des taux propres dans BigQuery, sans doublons.

Ce script :
1. récupère les réglages (projet, dataset, chemin de la clé) : valeurs par défaut,
   remplaçables par un fichier .env facultatif ou par les variables de l'automatisation,
2. crée la table bresse_fx.taux_change si elle n'existe pas encore,
3. lit data/processed/taux_propres.csv (produit par transform.py),
4. récupère dans BigQuery les clés déjà présentes (date_taux + devise),
5. n'envoie que les lignes NOUVELLES (anti-doublons),
6. écrit une trace de chaque exécution dans logs/pipeline.log.

Pourquoi pas un MERGE SQL ? Le mode gratuit de BigQuery (bac à sable) n'autorise pas
les instructions DML (INSERT, UPDATE, DELETE, MERGE). On filtre donc en Python,
puis on ajoute les lignes avec un "chargement par lot" (load job), qui est autorisé.

Lancement : python src/load.py
"""

import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# --- Paramètres ---------------------------------------------------------------
# Valeurs par défaut du projet. Un fichier .env (facultatif) ou les variables d'environnement
# de l'automatisation peuvent les remplacer sans modifier le code.
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "bresse-fx")
DATASET = os.getenv("BQ_DATASET", "bresse_fx")

# Emplacement de la clé du compte de service (fichier exclu de GitHub par le .gitignore)
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "cle_service_bigquery.json")
TABLE = "taux_change"

FICHIER_PROPRE = Path("data/processed/taux_propres.csv")
DOSSIER_LOGS = Path("logs")

# Structure de la table (le "schéma") : nom, type et caractère obligatoire de chaque colonne
SCHEMA = [
    bigquery.SchemaField("date_taux", "DATE", mode="REQUIRED", description="Date de publication du taux par la BCE"),
    bigquery.SchemaField("devise_base", "STRING", mode="REQUIRED", description="Devise de départ (EUR)"),
    bigquery.SchemaField("devise", "STRING", mode="REQUIRED", description="Devise d'arrivée (USD, CNY, CHF, GBP)"),
    bigquery.SchemaField("taux", "FLOAT64", mode="REQUIRED", description="Unités de devise pour 1 EUR"),
    bigquery.SchemaField("source", "STRING", description="Origine de la donnée"),
    bigquery.SchemaField("date_collecte", "TIMESTAMP", description="Date et heure (UTC) de la collecte"),
]


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


def verifier_configuration() -> None:
    """Arrête le script avec un message clair si un réglage manque."""
    if not PROJECT_ID:
        raise ValueError("ID du projet Google Cloud absent")
    cle = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not cle or not Path(cle).exists():
        raise FileNotFoundError(
            f"Clé du compte de service introuvable ({cle}) : place cle_service_bigquery.json "
            "à la racine du projet et lance le script depuis cette racine"
        )


def lire_donnees_propres() -> pd.DataFrame:
    """Lit le CSV propre et remet les bons types (un CSV ne garde que du texte)."""
    if not FICHIER_PROPRE.exists():
        raise FileNotFoundError(f"{FICHIER_PROPRE} introuvable : lance d'abord transform.py")
    df = pd.read_csv(FICHIER_PROPRE)
    df["date_taux"] = pd.to_datetime(df["date_taux"]).dt.date
    df["taux"] = pd.to_numeric(df["taux"])
    df["date_collecte"] = pd.to_datetime(df["date_collecte"], utc=True)
    return df


def creer_table_si_absente(client: bigquery.Client, table_id: str) -> None:
    """Crée la table vide au premier lancement ; ne fait rien si elle existe déjà."""
    table = bigquery.Table(table_id, schema=SCHEMA)
    table.description = "Taux de change de référence BCE (EUR -> USD, CNY, CHF, GBP) - Bresse Mécanique"
    client.create_table(table, exists_ok=True)


def lire_cles_existantes(client: bigquery.Client, table_id: str) -> set:
    """Renvoie l'ensemble des couples (date_taux, devise) déjà présents dans BigQuery."""
    requete = f"SELECT DISTINCT date_taux, devise FROM `{table_id}`"
    return {(ligne.date_taux, ligne.devise) for ligne in client.query(requete).result()}


def filtrer_nouvelles_lignes(df: pd.DataFrame, cles_existantes: set) -> pd.DataFrame:
    """Anti-doublons : ne garde que les lignes dont la clé (date_taux, devise) est absente de BigQuery."""
    deja_presente = [(d, dev) in cles_existantes for d, dev in zip(df["date_taux"], df["devise"])]
    return df[[not x for x in deja_presente]]


def charger(client: bigquery.Client, table_id: str, df: pd.DataFrame) -> None:
    """Ajoute les lignes à la fin de la table (WRITE_APPEND) via un chargement par lot."""
    config = bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, table_id, job_config=config)
    job.result()  # attend la fin du chargement ; lève une erreur en cas d'échec


def main() -> int:
    configurer_logs()
    logging.info("=== Début du chargement BigQuery ===")
    try:
        verifier_configuration()
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

        creer_table_si_absente(client, table_id)
        df = lire_donnees_propres()
        logging.info("Lignes propres disponibles : %d", len(df))

        cles = lire_cles_existantes(client, table_id)
        logging.info("Lignes déjà présentes dans %s : %d", table_id, len(cles))

        nouvelles = filtrer_nouvelles_lignes(df, cles)
        logging.info("Anti-doublons : %d lignes déjà connues ignorées", len(df) - len(nouvelles))

        if nouvelles.empty:
            logging.info("Aucune nouvelle ligne à charger (pas de nouveau taux publié)")
        else:
            charger(client, table_id, nouvelles)
            logging.info(
                "Chargées : %d nouvelles lignes (du %s au %s)",
                len(nouvelles), nouvelles["date_taux"].min(), nouvelles["date_taux"].max(),
            )

        total = client.get_table(table_id).num_rows
        logging.info("Total dans la table après chargement : %d lignes", total)
        logging.info("=== Fin du chargement : OK ===")
        return 0
    except Exception as erreur:
        logging.error("Échec du chargement : %s", erreur)
        logging.info("=== Fin du chargement : ÉCHEC ===")
        return 1


if __name__ == "__main__":
    sys.exit(main())
