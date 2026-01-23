import subprocess
import sys
import os

def install(package):
    print(f">>> Installation de {package} en cours...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    import numpy
except ImportError:
    install('numpy')
    import numpy
    
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, split, col, coalesce, lit
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 1. Initialisation de la Session Spark
# 1. Initialisation de la Session Spark en mode LOCAL
spark = SparkSession.builder \
    .appName("AtelierBigData_Classification") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9010") \
    .config("spark.hadoop.fs.s3a.access.key", "EupSiVo1vYMys6n0vpnW") \
    .config("spark.hadoop.fs.s3a.secret.key", "ZSTRedxvCY0gUsX6fWw4hFOu0iAM8tlJyLEnPW2e") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

def ip_to_long(ip):
    parts = split(ip, "\.")
    return (parts[0].cast("long") * 16777216 + 
            parts[1].cast("long") * 65536 + 
            parts[2].cast("long") * 256 + 
            parts[3].cast("long"))

def load_s3_file(path, file_type="csv"):
    try:
        if file_type == "csv":
            df = spark.read.csv(path, header=True, inferSchema=True)
        else:
            df = spark.read.text(path)
        return df
    except Exception as e:
        print(f"\n>>> ERREUR sur {path}: {e}")
        return None

# --- ÉTAPE 1 : CHARGEMENT DES DONNÉES ---
print("\n>>> CHARGEMENT DES DONNÉES...")
df_network = load_s3_file("s3a://mydatalake/Network_logs.csv")
df_clients = load_s3_file("s3a://mydatalake/client_hostname.csv")
df_dbip = load_s3_file("s3a://mydatalake/dbip-country-lite-2026-01.csv")

# --- ÉTAPE 2 : PRÉPARATION ET NETTOYAGE (SILVER LAYER) ---

# Nettoyage DB-IP (Géo-localisation)
if df_dbip:
    df_dbip = df_dbip.toDF("ip_start", "ip_end", "country_code")
    df_dbip = df_dbip.withColumn("start_long", ip_to_long(col("ip_start"))) \
                     .withColumn("end_long", ip_to_long(col("ip_end")))

# Nettoyage Network Logs (Labels d'intrusion)
df_net_labels = df_network.select(
    col("Source_IP").alias("net_ip"),
    "Port", "Protocol", "Payload_Size", "Intrusion"
).dropDuplicates(["net_ip"])

# Chargement et Parsing des Logs Bruts (Access Logs)
path_access = "s3a://mydatalake/access.log.gz"
df_access_raw = load_s3_file(path_access, "text")

if df_access_raw:
    log_pattern = r'^(\S+) - - \[(.*?)\] "(.*?) (.*?) (.*?)" (\d{3}) (\S+)'
    df_access_struct = df_access_raw.select(
        regexp_extract(col("value"), log_pattern, 1).alias("ip"),
        regexp_extract(col("value"), log_pattern, 6).alias("status")
    ).withColumn("ip_long", ip_to_long(col("ip")))

    # --- ÉTAPE 3 : ENRICHISSEMENT (GOLD LAYER) ---

    # Jointure Géo-IP
    df_enriched_geo = df_access_struct.join(
        df_dbip, 
        (col("ip_long") >= col("start_long")) & (col("ip_long") <= col("end_long")), 
        "left"
    )

    # Jointure avec les labels d'intrusion
    df_gold = df_enriched_geo.join(df_net_labels, df_enriched_geo.ip == df_net_labels.net_ip, "left")

    # Sélection finale et gestion des valeurs nulles
    df_final_ml = df_gold.select(
        col("ip"),
        coalesce(col("country_code"), lit("ZZ")).alias("country_code"),
        coalesce(col("status"), lit(200)).cast("int").alias("status"),
        coalesce(col("Port"), lit(80)).cast("int").alias("port"),
        coalesce(col("Protocol"), lit("Unknown")).alias("protocol"),
        coalesce(col("Payload_Size"), lit(0)).cast("int").alias("payload"),
        coalesce(col("Intrusion"), lit(0)).cast("int").alias("label")
    )

    # Jointure avec les Hostnames (CORRIGÉ : on ne sélectionne que 2 colonnes)
    df_clients_clean = df_clients.select(col("client").alias("ip_client"), col("hostname"))
    
    df_final_complete = df_final_ml.join(
        df_clients_clean, df_final_ml.ip == df_clients_clean.ip_client, "left"
    ).select(
        coalesce(col("hostname"), lit("unknown-host")).alias("hostname"),
        "ip", "country_code", "status", "port", "protocol", "payload", "label"
    )

    print("\n>>> DONNÉES PRÊTES POUR LE MACHINE LEARNING :")
    df_final_complete.show(5)

    # --- ÉTAPE 4 : ARCHITECTURE DE PRÉDICTION (ML PIPELINE) ---

    # Transformation des catégories (Pays, Protocol) en index numériques
    indexer_country = StringIndexer(inputCol="country_code", outputCol="country_idx", handleInvalid="keep")
    indexer_proto = StringIndexer(inputCol="protocol", outputCol="proto_idx", handleInvalid="keep")

    assembler = VectorAssembler(
        inputCols=["status", "port", "payload", "country_idx", "proto_idx"],
        outputCol="features"
    )

    # Modèle de classification (Random Forest)
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20)

    # Construction du Pipeline
    pipeline = Pipeline(stages=[indexer_country, indexer_proto, assembler, rf])

    # Division Train (80%) / Test (20%)
    train_data, test_data = df_final_complete.randomSplit([0.8, 0.2], seed=42)

    print(">>> ENTRAÎNEMENT DU MODÈLE DE CLASSIFICATION EN COURS...")
    model = pipeline.fit(train_data)

    # Prédictions
    predictions = model.transform(test_data)

    # Évaluation
    evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    print(f"\n>>> PRÉCISION DU MODÈLE : {accuracy:.2%}")
    print("\n>>> EXEMPLES DE PRÉDICTIONS (0=Légitime, 1=Suspect) :")
    predictions.select("ip", "hostname", "label", "prediction").show(10)

    # Sauvegarde sur MinIO
    df_final_complete.write.mode("overwrite").parquet("s3a://mydatalake/gold_final_data.parquet")
    print(">>> SUCCESS: Données et modèle finalisés.")

else:
    print("Erreur : Impossible de traiter les access logs.")

spark.stop()