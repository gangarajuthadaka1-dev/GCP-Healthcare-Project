from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

# Create Spark session
spark = SparkSession.builder \
                    .appName("Healthcare Claims Ingestion") \
                    .getOrCreate()

# configure variables
BUCKET_NAME = "healthcare-bucket-16072026"
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"
BQ_TABLE = "project-819f30a6-533e-44c8-a6e.bronze_dataset.claims"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# read claims from  source
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# adding hospital source for future reference
claims_df = (claims_df
                .withColumn("datasource", 
                              when(input_file_name().contains("hospital2"), "hosp2")
                             .when(input_file_name().contains("hospital1"), "hosp1").otherwise("None")))

# dropping dupplicates if any
claims_df = claims_df.dropDuplicates()

# write to bigquery
(claims_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())