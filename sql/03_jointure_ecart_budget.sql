-- Requête 3 : jointure
-- Question métier : chaque mois, quel est l'écart entre le taux réel et le taux budget,
-- et quel est son impact en euros sur le résultat de Bresse Mécanique ?
--
-- Lecture de impact_resultat_eur : positif = favorable au résultat, négatif = défavorable.
--   achat : impact = coût au taux budget - coût au taux réel
--   vente : impact = recette au taux réel - recette au taux budget

WITH taux_mensuels AS (
  -- Étape 1 : taux réel moyen par mois et par devise
  SELECT
    DATE_TRUNC(date_taux, MONTH) AS mois,
    devise,
    AVG(taux) AS taux_reel_moyen
  FROM `bresse-fx.bresse_fx.taux_change`
  GROUP BY mois, devise
)

-- Étape 2 : comparaison avec le budget (jointure sur la devise et l'année)
SELECT
  t.mois,
  t.devise,
  b.sens,
  b.categorie,
  ROUND(t.taux_reel_moyen, 4) AS taux_reel_moyen,
  b.taux_budget,
  ROUND((t.taux_reel_moyen - b.taux_budget) / b.taux_budget * 100, 2) AS ecart_taux_pct,
  ROUND(b.montant_mensuel_devise / b.taux_budget, 0)     AS montant_eur_budget,
  ROUND(b.montant_mensuel_devise / t.taux_reel_moyen, 0) AS montant_eur_reel,
  ROUND(
    CASE
      WHEN b.sens = 'achat' THEN b.montant_mensuel_devise / b.taux_budget - b.montant_mensuel_devise / t.taux_reel_moyen
      ELSE                       b.montant_mensuel_devise / t.taux_reel_moyen - b.montant_mensuel_devise / b.taux_budget
    END, 0) AS impact_resultat_eur
FROM taux_mensuels AS t
JOIN `bresse-fx.bresse_fx.budget_devises` AS b
  ON  t.devise = b.devise
  AND EXTRACT(YEAR FROM t.mois) = b.annee
ORDER BY t.mois, t.devise;
