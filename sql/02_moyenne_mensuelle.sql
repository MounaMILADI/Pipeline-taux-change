-- Requête 2 : agrégation
-- Question métier : quel est le taux moyen, minimum et maximum
-- de chaque devise, mois par mois ? (maille du reporting de contrôle de gestion)

SELECT
  DATE_TRUNC(date_taux, MONTH) AS mois,
  devise,
  ROUND(AVG(taux), 4) AS taux_moyen,
  ROUND(MIN(taux), 4) AS taux_min,
  ROUND(MAX(taux), 4) AS taux_max,
  COUNT(*)            AS nb_jours_cotes
FROM `bresse-fx.bresse_fx.taux_change`
GROUP BY mois, devise
ORDER BY devise, mois;
