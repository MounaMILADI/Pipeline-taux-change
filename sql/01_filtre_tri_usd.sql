-- Requête 1 : filtre + tri
-- Question métier : quels ont été les 10 jours les plus favorables de 2026
-- pour acheter en dollars (taux EUR -> USD le plus élevé) ?
-- Plus le taux est élevé, plus 1 EUR achète de dollars : les achats en USD coûtent moins cher en euros.

SELECT
  date_taux,
  taux AS usd_pour_1_eur
FROM `bresse-fx.bresse_fx.taux_change`
WHERE devise = 'USD'
  AND date_taux >= '2026-01-01'
ORDER BY taux DESC
LIMIT 10;
