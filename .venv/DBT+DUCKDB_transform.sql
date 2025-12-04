-- Objectif :

-- Ce traitement vise à :

-- Exclure les variables non pertinentes (VendorID, RatecodeID)

-- Nettoyer le jeu de données en supprimant :

-- Les courses avec anomalies temporelles

-- Les distances nulles ou négatives

-- Les montants incohérents

-- Filtrer uniquement les paiements valides (CB et cash)

-- Transformer certaines variables :

-- Encodage explicite du type de paiement

-- Calcul de la durée du trajet en minutes

-- Restreindre les données à l’année 2024

-- Exporter le résultat final sous format Parquet exploitable


-- Configuration dbt pour sauvegarder le résultat sous forme de fichier Parquet
{{config(
    materialized='external',                      -- Matérialisation en fichier externe
    location='output/trips_2024_transformed.parquet', -- Chemin du fichier de sortie
    format='parquet'                              -- Format du fichier
)}}

WITH source_data AS (
    -- Sélection de toutes les colonnes sauf VendorID et RatecodeID
    SELECT * EXCLUDE (VendorID, RatecodeID) 
    FROM {{ source('tlc_taxi_trips', 'raw_yellow_tripdata') }}
),

filtered_data AS (
    -- Filtrage des données pour nettoyer les données incohérentes ou inutiles
    SELECT *
    FROM source_data
    WHERE 
        passenger_count > 0                       -- Au moins 1 passager
        AND trip_distance > 0                     -- Distance strictement positive
        AND total_amount > 0                       -- Montant total positif
        AND tpep_pickup_datetime < tpep_dropoff_datetime  -- Cohérence temporelle
        AND store_and_fwd_flag = 'N'               -- On exclut les données "store and forward"
        AND tip_amount >= 0                        -- Pour exclure les valeurs aberrantes
        AND payment_type IN (1, 2)                 -- On garde seulement les paiements CB et cash
),

transformed_data AS (
    -- Transformation des variables
    SELECT
        CAST(passenger_count AS BIGINT) AS passenger_count,  -- Harmonisation de type

        -- Transformation du code de paiement en modalité explicite
        CASE 
            WHEN payment_type = 1 THEN 'Credit card'
            WHEN payment_type = 2 THEN 'cash'
        END AS payment_method,

        -- Calcul de la durée du trajet en minutes
        DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,

        -- On conserve toutes les autres colonnes sauf celles modifiées
        * EXCLUDE (passenger_count, payment_type)
    FROM filtered_data
),

final_data AS (
    -- Création de colonnes "date" à partir des datetime
    SELECT *,
        CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
        CAST(tpep_dropoff_datetime AS DATE) AS dropoff_date
    FROM transformed_data
    WHERE
        -- Filtre sur les courses de l’année 2024
        pickup_date >= '2024-01-01'
        AND pickup_date < '2025-01-01'
        AND dropoff_date >= '2024-01-01'
        AND dropoff_date < '2025-01-01'
)

-- Sélection finale : on enlève les colonnes intermédiaires de date
SELECT * EXCLUDE(pickup_date, dropoff_date)
FROM final_data
WHERE trip_duration_minutes > 0                  -- Sécurité contre durées nulles ou négatives
