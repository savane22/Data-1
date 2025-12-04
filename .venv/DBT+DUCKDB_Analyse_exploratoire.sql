-- 🎯 Objectif:
-- Mettre en place une analyse exploratoire et un contrôle qualité automatisé du dataset des courses de taxis (NYC)
-- afin d’identifier les incohérences (dates, distances, montants) et de sécuriser le pipeline de traitement avant modélisation ou visualisation.


-- 1) Afficher un échantillon de 10 lignes pour explorer la structure du dataset
SELECT * 
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet') 
LIMIT 10;


-- 2) Compter le nombre total d’observations (nombre total de courses) en Décembre 2024
SELECT COUNT(*)
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet');


-- 3) Nombre de courses par fournisseur (VendorID)
SELECT VendorID, COUNT(*) AS trips_count
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
GROUP BY VendorID;


-- 4) Nombre de courses par type de tarification (RatecodeID)
SELECT RatecodeID, COUNT(*) AS trips_count
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
GROUP BY RatecodeID;


-- 5) Nombre de courses selon le statut d’enregistrement différé (Store_and_fwd_flag)
SELECT Store_and_fwd_flag, COUNT(*) AS trips_count
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
GROUP BY Store_and_fwd_flag;


-- 6) Répartition des courses par type de paiement
SELECT payment_type, COUNT(*) AS trips_count
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
GROUP BY payment_type;


-- 7) Nombre de courses par zone de prise en charge (Pickup Location)
SELECT PULocationID, COUNT(*) AS trips_count
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
GROUP BY PULocationID;


-- 8) Nombre de courses par zone de dépôt (Dropoff Location)
SELECT DOLocationID, COUNT(*) AS trips_count
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
GROUP BY DOLocationID;


-- 9) Compter les trajets où la date de prise en charge est postérieure à la date de dépôt (anomalie temporelle)
SELECT COUNT(*) AS trips_count
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
WHERE tpep_pickup_datetime > tpep_dropoff_datetime;


-- 10) Afficher quelques exemples de ces trajets temporellement incohérents
SELECT *
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
WHERE tpep_pickup_datetime > tpep_dropoff_datetime 
LIMIT 10;


-- 11) Compter les trajets avec une distance nulle ou négative (anomalie de distance)
SELECT COUNT(*) AS trips_count
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
WHERE trip_distance <= 0;


-- 12) Afficher quelques exemples de trajets avec une distance négative (très anormal)
SELECT *
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
WHERE trip_distance < 0 
LIMIT 10;


-- 13) Visualiser des trajets avec une distance nulle (probable erreur ou course annulée)
SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
WHERE trip_distance = 0
LIMIT 10;


-- 14) Afficher quelques trajets avec un montant total négatif (incohérence financière)
SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, total_amount
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
WHERE total_amount < 0 
LIMIT 10;


-- 15) Compter tous les trajets avec un montant nul ou négatif
SELECT COUNT(*) AS trips_count
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
WHERE total_amount <= 0;


-- 16) Afficher 10 lignes en excluant certaines colonnes (VendorID et RatecodeID)
SELECT * EXCLUDE(VendorID, RatecodeID)
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet')
LIMIT 10;
