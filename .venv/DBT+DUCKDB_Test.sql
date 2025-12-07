-- 🔬 Tests de qualité des données

-- Ce projet met en place plusieurs tests personnalisés (custom tests) exécutés via dbt sur DuckDB afin de garantir l’intégrité des données, leur cohérence statistique et le respect des règles métier sur le modèle transform (dataset de trajets de taxis de NYC).

-- Contrairement aux tests standards de dbt (not_null, unique, etc.), ces contrôles visent à détecter des anomalies spécifiques au métier ainsi que des erreurs structurelles susceptibles de biaiser fortement les analyses en aval.



-- Objectif technique :

-- Garantir la présence des 12 mois de l’année

-- Éviter l’exploitation d’une série temporelle incomplète

-- Le test échoue si au moins un mois est manquant

WITH month AS (
    -- Sélection des mois distincts à partir de la date de prise en charge
    SELECT DISTINCT EXTRACT(MONTH FROM tpep_pickup_datetime) AS month
    FROM {{ ref('transform') }}
)
-- Comptage du nombre de mois distincts présents dans les données
SELECT COUNT(*)
FROM month
HAVING COUNT(*) <> 12;   -- La requête ne retourne un résultat que si le nombre de mois est différent de 12


-- Objectif technique :

-- Détecter :

-- les valeurs négatives ou nulles

-- les valeurs non entières

-- Appliquer une contrainte métier simple :
-- → un trajet doit comporter au moins un passager entier

SELECT *
FROM {{ ref('transform') }}
WHERE 
    passenger_count <= 0                         -- Détecte les valeurs négatives ou nulles (incohérentes)
    OR passenger_count != CAST(passenger_count AS BIGINT);  -- Vérifie que le nombre de passagers est bien un entier


-- Repérer les distances nulles ou négatives

-- Identifier des erreurs d’ingestion, de calcul GPS ou de capteur

SELECT *
FROM {{ ref('transform') }}
WHERE trip_distance <= 0;     -- Détecte les trajets avec une distance nulle ou négative (incohérente)


-- Détecter :

-- des inversions de timestamps

-- des données corrompues

-- des erreurs de calcul de durée

SELECT *
FROM {{ ref('transform') }}
WHERE trip_duration_minutes <= 0;   -- Détecte les durées de trajet nulles ou négatives (anomalies temporelles)


