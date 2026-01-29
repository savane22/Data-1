-- Delta Live Tables Project – Yellow Taxi Data Pipeline
-- Ce projet démontre la mise en place d’un pipeline de données moderne avec Delta Live Tables (DLT) dans Databricks.
-- À travers un cas concret basé sur les données Yellow Taxi de New York, il illustre comment appliquer les bonnes pratiques Data Engineering en mode Bronze → Silver → Gold.

-- Objectif : Passer de données brutes (raw) à des données agrégées et prêtes pour le reporting, via un pipeline scalable, fiable et automatisé.

-- Databricks notebook source
-- Table des trajets bruts alimentée par Auto Loader
CREATE OR REFRESH STREAMING LIVE TABLE raw_trips
COMMENT "Données brutes de Yellow Taxi ingérées de manière incrémentale à partir de fichiers Parquet, avec le nom du fichier et l’horodatage d’ingestion"
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported') 
AS
SELECT
  *, 
  substring_index(_metadata.file_path, '/', -1) AS file_name -- recuperer le nom de base du fichier source
FROM cloud_files(
  "dbfs:/raw_data_files/*.parquet",         
  "parquet",
  map(
    "cloudFiles.inferColumnTypes", "true",           -- Inférence du schéma (détermination automatique de la structure des données, types de colonnes, etc.) pour les fichiers Parquet
    "cloudFiles.schemaLocation",   "dbfs:/schemas/yellow_taxi_trips/",  -- enregistrer l’emplacement du schéma
    "cloudFiles.partitionColumns", "year,month"      -- Ajouter un partitionnement pour des lectures optimisées
  )
);

-- COMMAND ----------

-- Taxi Zones table
CREATE OR REFRESH LIVE TABLE raw_taxi_zones
COMMENT "Taxi Zones"
AS
SELECT
   *
FROM  read_files('dbfs:/raw_data_files/taxi_zone_lookup.csv', format => 'csv', header => true, inferSchema => true);


-- COMMAND ----------

-- Créer une table en temps réel pour les trajets traités dans la couche Silver
CREATE OR REFRESH STREAMING LIVE TABLE processed_trips
COMMENT "Données de trajets nettoyées et enrichies avec les informations des zones de prise en charge et de dépose, jointes depuis la couche Bronze."
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported') 
AS
WITH filtered_raw_trips AS (
  -- Appliquer des filtres de qualité des données au flux de trajets bruts
  SELECT *
  FROM STREAM(live.raw_trips)
  WHERE passenger_count > 0
    AND trip_distance > 0
    AND total_amount > 0
    AND tpep_pickup_datetime < tpep_dropoff_datetime
    AND tip_amount >= 0
    AND payment_type = 1
)
-- Joindre avec les informations des zones de taxi et ajouter des caractéristiques temporelles
SELECT
  s.*,
  pu_zone.Borough AS pickup_borough,
  do_zone.Borough AS dropoff_borough,
  pu_zone.Zone AS pickup_zone,
  do_zone.Zone AS dropoff_zone,
  DATE(s.tpep_pickup_datetime) AS pickup_date,
  HOUR(s.tpep_pickup_datetime) AS pickup_hour,
  HOUR(s.tpep_dropoff_datetime) AS dropoff_hour,
  DAYOFWEEK(s.tpep_pickup_datetime) AS pickup_day_of_week
FROM filtered_raw_trips s
LEFT JOIN live.raw_taxi_zones pu_zone 
  ON s.PULocationID = pu_zone.LocationID
LEFT JOIN live.raw_taxi_zones do_zone 
  ON s.DOLocationID = do_zone.LocationID;

-- COMMAND ----------

-- Créer la table Gold finale agrégée
CREATE OR REFRESH LIVE TABLE aggregated_data
COMMENT "Aggregated taxi trip data showing popular routes and revenue, for use in dashboards and reporting."
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported') 
AS
SELECT 
    pickup_date,
    pickup_borough,
    dropoff_borough,
    CONCAT(pickup_borough, ' - ', dropoff_borough) AS route,
    pickup_hour,          
    AVG(trip_distance) AS avg_trip_distance,  
    COUNT(*) AS number_trips,
    SUM(total_amount) AS total_revenue
FROM live.processed_trips
WHERE pickup_borough IS NOT NULL 
  AND dropoff_borough IS NOT NULL
  AND pickup_borough NOT IN ('N/A', 'Unknown')
  AND dropoff_borough NOT IN ('N/A', 'Unknown')
GROUP BY 
    pickup_date,
    pickup_borough,
    dropoff_borough,
    pickup_hour;
