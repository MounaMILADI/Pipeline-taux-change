-- Requête 4 : agrégation avec fonction de fenêtre
-- Question métier : quelle devise présente le plus de risque de change ?
-- Mesure : volatilité = écart-type des variations quotidiennes du taux (en %).
-- Plus elle est élevée, plus le coût en euros des flux dans cette devise est imprévisible.

WITH variations AS (
  SELECT
    devise,
    date_taux,
    taux,
    -- Variation par rapport au jour coté précédent (LAG = valeur de la ligne précédente)
    taux / LAG(taux) OVER (PARTITION BY devise ORDER BY date_taux) - 1 AS variation_jour
  FROM `bresse-fx.bresse_fx.taux_change`
)

SELECT
  devise,
  ROUND(STDDEV(variation_jour) * 100, 3) AS volatilite_periode_pct,
  ROUND(STDDEV(IF(date_taux >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY), variation_jour, NULL)) * 100, 3)
                                         AS volatilite_30j_pct,
  ROUND(MIN(taux), 4)                    AS taux_min,
  ROUND(MAX(taux), 4)                    AS taux_max,
  ROUND((MAX(taux) - MIN(taux)) / AVG(taux) * 100, 2) AS amplitude_pct
FROM variations
GROUP BY devise
ORDER BY volatilite_30j_pct DESC;
