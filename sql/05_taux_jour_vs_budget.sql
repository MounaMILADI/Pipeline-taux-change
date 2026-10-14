-- Requête 5 : jointure au jour le jour
-- Question métier : chaque jour, où se situe le taux réel par rapport au taux budget ?
-- Sert au graphique d'évolution (taux réel vs budget) et aux KPI du dernier jour du dashboard.

SELECT
  t.date_taux,
  t.devise,
  b.sens,
  b.categorie,
  t.taux        AS taux_reel,
  b.taux_budget,
  ROUND((t.taux - b.taux_budget) / b.taux_budget * 100, 2) AS ecart_taux_pct,
  -- TRUE pour la date la plus récente de la table (utile pour afficher "le taux du jour")
  t.date_taux = MAX(t.date_taux) OVER () AS est_dernier_jour
FROM `bresse-fx.bresse_fx.taux_change` AS t
JOIN `bresse-fx.bresse_fx.budget_devises` AS b
  ON  t.devise = b.devise
  AND EXTRACT(YEAR FROM t.date_taux) = b.annee
ORDER BY t.date_taux, t.devise;
