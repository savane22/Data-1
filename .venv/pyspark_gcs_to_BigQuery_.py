# PRESENTATION
# Cloud Dataproc est un service entièrement géré et évolutif permettant d'exécuter des tâches Apache Spark et Hadoop sur Google Cloud. Il simplifie la gestion de l'infrastructure et vous permet de vous concentrer sur l'écriture et le déploiement de vos pipelines de données.

# Dans ce projet, j'ai utilisé un script ETL PySpark et à l'exécuter en deux étapes :

# Localement, pour valider les transformations.
# Sur un cluster Cloud Dataproc, en utilisant les données stockées dans Cloud Storage et en écrivant le résultat dans BigQuery.
# Cette approche reflète un workflow réel où le développement démarre localement, puis est déployé dans le cloud.

# OBJECTIFS

# Créer et configurer un cluster Dataproc à l'aide de la CLI « gcloud ».
# Écrire et exécuter une tâche ETL PySpark localement sur la machine. * Importez les données d'entrée et les scripts PySpark dans Google Cloud Storage.
# Déployer et exécutez le pipeline ETL sur Dataproc.
# Charger les données nettoyées dans BigQuery.
# Valider la transformation en exécutant des requêtes SQL sur la table résultante.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Créer une session Spark
spark = SparkSession.builder \
    .appName('Spark ETL from GCS to BigQuery') \
    .getOrCreate()

# Paramètres
BUCKET_NAME = "dataproc-staging-us-central1-332295648972-hlfselj2"
SPARK_JOB_FOLDER = "formysparkjob"
GCS_PATH_INPUT = f"gs://{BUCKET_NAME}/{SPARK_JOB_FOLDER}/yellow_tripdata_2023-01.parquet"
BQ_TABLE = "trips.cleaned_trips"

# Lire le fichier Parquet depuis GCS
df = spark.read.parquet(GCS_PATH_INPUT)

# Sélectionner et nettoyer les données
df_cleaned = df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount"
).filter(col("trip_distance") > 0)

# Écrire dans BigQuery
df_cleaned.write.format("bigquery") \
    .option("temporaryGcsBucket", BUCKET_NAME) \
    .option("table", BQ_TABLE) \
    .mode("overwrite") \
    .save()

# Terminer la session
spark.stop()