-- 06 : création des vues utilisées par le dashboard Data Studio
-- Une vue = une requête enregistrée dans BigQuery, qui se comporte comme une table.
-- Elle est recalculée à chaque lecture : le dashboard affiche donc toujours les dernières
-- données chargées par le pipeline automatisé.
-- (Pas de ORDER BY dans les vues : le tri se fait dans Data Studio.)

-- Vue 1 : écart au budget et impact en euros, par mois et par devise (reprend la requête 03)
CREATE OR REPLACE VIEW `bresse-fx.bresse_fx.v_ecart_budget_mensuel` AS
WITH taux_mensuels AS (
  SELECT
    DATE_TRUNC(date_taux, MONTH) AS mois,
    devise,
    AVG(taux) AS taux_reel_moyen
  FROM `bresse-fx.bresse_fx.taux_change`
  GROUP BY mois, devise
)
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
  AND EXTRACT(YEAR FROM t.mois) = b.annee;


-- Vue 2 : volatilité par devise (reprend la requête 04)
CREATE OR REPLACE VIEW `bresse-fx.bresse_fx.v_volatilite` AS
WITH variations AS (
  SELECT
    devise,
    date_taux,
    taux,
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
GROUP BY devise;


-- Vue 3 : taux réel du jour face au taux budget (reprend la requête 05)
CREATE OR REPLACE VIEW `bresse-fx.bresse_fx.v_taux_jour_budget` AS
SELECT
  t.date_taux,
  t.devise,
  b.sens,
  b.categorie,
  t.taux        AS taux_reel,
  b.taux_budget,
  ROUND((t.taux - b.taux_budget) / b.taux_budget * 100, 2) AS ecart_taux_pct,
  t.date_taux = MAX(t.date_taux) OVER () AS est_dernier_jour
FROM `bresse-fx.bresse_fx.taux_change` AS t
JOIN `bresse-fx.bresse_fx.budget_devises` AS b
  ON  t.devise = b.devise
  AND EXTRACT(YEAR FROM t.date_taux) = b.annee;
