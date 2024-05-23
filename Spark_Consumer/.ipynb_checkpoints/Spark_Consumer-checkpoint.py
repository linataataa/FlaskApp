from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import split, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType,BooleanType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import GBTClassificationModel
import logging
from pyspark.sql.functions import from_json, col, when
from pymongo import MongoClient
from pyspark.ml.classification import LogisticRegressionModel
import pymongo
from pyspark.ml.linalg import DenseVector
import json

working_directory = 'jars/*'

# Configuration de la session Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TwitterSentimentAnalysis") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/churn.predictions") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Define the schema for the CSV data
schema = StructType([
    StructField("State", StringType()),
    StructField("Account_length", IntegerType()),
    StructField("Area_code", IntegerType()),
    StructField("International_plan", StringType()),
    StructField("Voice_mail_plan", StringType()),
    StructField("Number_vmail_messages", IntegerType()),
    StructField("Total_day_minutes", DoubleType()),
    StructField("Total_day_calls", IntegerType()),
    StructField("Total_day_charge", DoubleType()),
    StructField("Total_eve_minutes", DoubleType()),
    StructField("Total_eve_calls", IntegerType()),
    StructField("Total_eve_charge", DoubleType()),
    StructField("Total_night_minutes", DoubleType()),
    StructField("Total_night_calls", IntegerType()),
    StructField("Total_night_charge", DoubleType()),
    StructField("Total_intl_minutes", DoubleType()),
    StructField("Total_intl_calls", IntegerType()),
    StructField("Total_intl_charge", DoubleType()),
    StructField("Customer_service_calls", IntegerType())
])

# Configuration Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic_name = "test8"


feature_columns = ["State", "Account_length", "Area_code", "International_plan", "Voice_mail_plan",
                   "Number_vmail_messages", "Total_day_minutes", "Total_day_calls", "Total_day_charge",
                   "Total_eve_minutes", "Total_eve_calls", "Total_eve_charge", "Total_night_minutes",
                   "Total_night_calls", "Total_night_charge", "Total_intl_minutes", "Total_intl_calls",
                   "Total_intl_charge", "Customer_service_calls"]


# Lire les données de Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .load() \


value_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Séparer les champs CSV en colonnes distinctes
columns = ["State", "Account_length", "Area_code", "International_plan", "Voice_mail_plan", "Number_vmail_messages",
           "Total_day_minutes", "Total_day_calls", "Total_day_charge", "Total_eve_minutes", "Total_eve_calls",
           "Total_eve_charge", "Total_night_minutes", "Total_night_calls", "Total_night_charge", "Total_intl_minutes",
           "Total_intl_calls", "Total_intl_charge", "Customer_service_calls", "Churn"]

parsed_df = value_df.select(split(col("value"), ",").alias("split_value"))

# Appliquer le schéma et renommer les colonnes
for idx, column in enumerate(columns):
    parsed_df = parsed_df.withColumn(column, col("split_value").getItem(idx))

parsed_df = parsed_df.drop("split_value")

# Convertir les plans en valeurs binaires pour 'International_plan' et 'Voice_mail_plan'
parsed_df = parsed_df.withColumn('International_plan', when(parsed_df['International_plan'] == 'Yes', 1).otherwise(0))
parsed_df = parsed_df.withColumn('Voice_mail_plan', when(parsed_df['Voice_mail_plan'] == 'Yes', 1).otherwise(0))

# Convertir les colonnes nécessaires aux types appropriés
parsed_df = parsed_df.withColumn('Account_length', col('Account_length').cast(IntegerType())) \
                     .withColumn('Area_code', col('Area_code').cast(IntegerType())) \
                     .withColumn('Number_vmail_messages', col('Number_vmail_messages').cast(IntegerType())) \
                     .withColumn('Total_day_minutes', col('Total_day_minutes').cast(DoubleType())) \
                     .withColumn('Total_day_calls', col('Total_day_calls').cast(IntegerType())) \
                     .withColumn('Total_day_charge', col('Total_day_charge').cast(DoubleType())) \
                     .withColumn('Total_eve_minutes', col('Total_eve_minutes').cast(DoubleType())) \
                     .withColumn('Total_eve_calls', col('Total_eve_calls').cast(IntegerType())) \
                     .withColumn('Total_eve_charge', col('Total_eve_charge').cast(DoubleType())) \
                     .withColumn('Total_night_minutes', col('Total_night_minutes').cast(DoubleType())) \
                     .withColumn('Total_night_calls', col('Total_night_calls').cast(IntegerType())) \
                     .withColumn('Total_night_charge', col('Total_night_charge').cast(DoubleType())) \
                     .withColumn('Total_intl_minutes', col('Total_intl_minutes').cast(DoubleType())) \
                     .withColumn('Total_intl_calls', col('Total_intl_calls').cast(IntegerType())) \
                     .withColumn('Total_intl_charge', col('Total_intl_charge').cast(DoubleType())) \
                     .withColumn('Customer_service_calls', col('Customer_service_calls').cast(IntegerType())) \
                     .withColumn('Churn', col('Churn').cast(BooleanType()))

# Ajouter la colonne 'churn_integer' basée sur la colonne 'Churn'
parsed_df = parsed_df.withColumn('churn_integer', when(parsed_df['Churn'] == True, 1).otherwise(0))

# Vérifier le schéma pour s'assurer que toutes les colonnes sont correctement typées
parsed_df.printSchema()

# Remplir les valeurs nulles avec des valeurs par défaut (par exemple 0.0)
parsed_df = parsed_df.na.fill(0)

# Assembler les colonnes d'entrée en un seul vecteur de caractéristiques
assembler = VectorAssembler(inputCols=[
    'Account_length', 'Area_code', 'Total_eve_calls',
    'Total_day_minutes', 'Total_day_calls', 'Total_day_charge',
    'Customer_service_calls'
], outputCol='features')

transformed_df = assembler.transform(parsed_df)

# Charger le modèle sauvegardé
loaded_model = LogisticRegressionModel.load("modele.pkl")

# Faire des prédictions sur les nouvelles données
predictions = loaded_model.transform(transformed_df)
predictions.printSchema()
# Afficher les prédictions

# Définissez la fonction pour insérer les données dans MongoDB

def convert_vector_to_list(dense_vector):
    """Convert a DenseVector to a list."""
    return dense_vector.toArray().tolist() if dense_vector is not None else None

def save_mongodb(predictions,df):
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.churn
    collection = db.predictions

    # Convert Spark DataFrame to Pandas DataFrame
    predictions_pd = predictions.toPandas()

    # Convert DenseVector to list in the Pandas DataFrame
    for col_name in predictions_pd.columns:
        if predictions_pd[col_name].apply(lambda x: isinstance(x, DenseVector)).any():
            predictions_pd[col_name] = predictions_pd[col_name].apply(convert_vector_to_list)

    # Convert Pandas DataFrame to a list of dictionaries
    records = predictions_pd.to_dict("records")
    print(records)
    # Insert records into MongoDB
    # Insert records into MongoDB
    if records:
        try:
            collection.insert_many(records)
            print("Records successfully inserted.")
        except Exception as e:
            print("An error occurred while inserting records:", e)


 

def write_row_in_mongo(dataframe, df):
  try:
    dataframe.select('prediction', 'churn_integer', 'features').write \
      .format("com.mongodb.spark.sql.DefaultSource") \
      .option("uri", "mongodb://localhost:27017/ma_base_d.ma_base_d") \
      .mode("append") \
      .save()
    print("Data written to MongoDB successfully!")
  except Exception as e:
    print("Error writing to MongoDB:", e)
    #
# Load predictions in Mongo
query = predictions.writeStream \
      .foreachBatch(save_mongodb) \
        .outputMode("append") \
    .start()

query.awaitTermination()