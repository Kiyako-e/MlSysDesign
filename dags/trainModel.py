import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
from pyspark.ml.recommendation import ALS
from dotenv import load_dotenv
from utils.logger import logger

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")


def train_and_predict():
    logger.info("Start train_and_predict")
    spark = SparkSession.builder.appName("MyAwesomeSpark").master("spark://spark-master:7077").getOrCreate()

    logger.info("Configuring Spark S3 settings.")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark session created successfully.")

    schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", IntegerType(), True)
    ])

    logger.info("Loading train and test datasets from MinIO.")
    try:
        train_data = spark.read.csv(f"s3a://{BUCKET_NAME}/train_ratings.csv", header=True, schema=schema)
        test_data = spark.read.csv(f"s3a://{BUCKET_NAME}/test_ratings.csv", header=True, schema=schema)
    except Exception as e:
        logger.error(f"Failed to read datasets: {e}")
        spark.stop()
        return

    logger.info("Previewing train and test datasets.")
    train_data.show(5, False)
    test_data.show(5, False)

    logger.info("Training ALS model.")
    als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
    try:
        model = als.fit(train_data)
        logger.info("Model training completed successfully.")
    except Exception as e:
        logger.error(f"Model training failed: {e}")
        spark.stop()
        return

    logger.info("Saving the trained model.")
    try:
        model.save("/shared_data/als_model")
        logger.info("Model saved successfully.")
    except Exception as e:
        logger.error(f"Failed to save the model: {e}")

    logger.info("Making predictions on the test dataset.")
    try:
        predictions = model.transform(test_data)
        predictions.write.format("csv").mode("overwrite").save(f"s3a://{BUCKET_NAME}/predictions")
        logger.info("Predictions written to MinIO successfully.")
    except Exception as e:
        logger.error(f"Prediction or saving results failed: {e}")

    spark.stop()
    logger.info("Spark session stopped")
