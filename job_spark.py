from pyspark.sql import SparkSession

# 1. Initialisation de la Session Spark
spark = SparkSession.builder \
    .appName("AtelierBigData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "21iFMh6sGj4mVjC0Ndob") \
    .config("spark.hadoop.fs.s3a.secret.key", "2eivPdLM1l3t0pqwV0Fh4FRjppxKB5ZMj5a1jMCk") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("\n>>> CONFIGURATION TERMINÉE - DÉBUT DE L'EXTRACTION")

try:
    df_labels = spark.read.csv("s3a://mydatalake/Network_logs.csv", header=True, inferSchema=True)
    print(f"\n>>> SUCCESS: Network_logs.csv contient {df_labels.count()} lignes.")
    df_labels.show(5)
except Exception as e:
    print(f"\n>>> ERREUR LECTURE CSV : {e}")

# 3. Lecture des Logs (Access Logs)
try:
    df_logs = spark.read.text("s3a://mydatalake/access.log.zip")
    print(f"\n>>> SUCCESS: access.log.zip contient {df_logs.count()} lignes.")
    df_logs.show(5, truncate=False)
except Exception as e:
    print(f"\n>>> ERREUR LECTURE LOGS : {e}")

import time
time.sleep(5)
spark.stop()