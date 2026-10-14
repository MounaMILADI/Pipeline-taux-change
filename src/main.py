"""
Étape 04 - Point d'entrée du pipeline : enchaîne extraction, transformation et chargement.

C'est ce script que l'automatisation (GitHub Actions) lance chaque jour ouvré.
Si une étape échoue, les suivantes ne sont pas lancées et le script se termine
avec un code d'erreur (1) : GitHub Actions marque alors l'exécution en échec.

Lancement : python src/main.py
"""

import logging
import sys

import extract
import transform
import load


def main() -> int:
    for nom, etape in [("Extraction", extract), ("Transformation", transform), ("Chargement", load)]:
        code = etape.main()
        if code != 0:
            logging.error("Pipeline interrompu : échec de l'étape %s", nom)
            logging.info("##### PIPELINE : ÉCHEC #####")
            return code
    logging.info("##### PIPELINE : OK #####")
    return 0


if __name__ == "__main__":
    sys.exit(main())
