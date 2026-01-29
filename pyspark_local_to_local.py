# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# %%
# 1. Créer une session Spark
spark = SparkSession.builder \
    .appName('Local ETL - Taxi Parquet') \
    .getOrCreate()
    
    
# Paramètres
BUCKET_NAME = "dataproc-staging-us-central1-332295648972-hlfselj2"
SPARK_JOB_FOLDER = "formysparkjob"
GCS_PATH_INPUT = f"gs://{BUCKET_NAME}/{SPARK_JOB_FOLDER}/yellow_tripdata_2023-01.parquet"
BQ_TABLE = "trips.cleaned_trips"

# %%
# 2. Lire le fichier parquet depuis local (Extract)
df = spark.read.parquet("yellow_tripdata_2023-01.parquet")
df.show(5)

# %%
# 3. Sélectionner et néttoyer les données
df_cleaned = df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount"
).filter(col("trip_distance") > 0)

df_cleaned.show(10)

# 4. Ecrire le resultat dans locale en format parquet (Load)
df_cleaned.write.mode("overwrite").parquet("cleaned_trips")

# 6. Fermeture de la session
spark.stop()