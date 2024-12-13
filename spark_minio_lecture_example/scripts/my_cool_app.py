import os
import sys
sys.path.append('.')

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import year, col

minio_access_key = 'minioaccesskey'
minio_secret_key = 'miniosecretkey'
minio_endpoint = 'http://minio-server:9000'
minio_bucket = 'movielens'
minio_file_path = 'ratings.csv'
minio_out_file_path = "ratings_mod_csv"

spark = SparkSession.builder.appName("My awesome Spark").master("spark://spark-master:7077").getOrCreate()
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", minio_access_key))
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", minio_secret_key))
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", os.getenv("ENDPOINT", minio_endpoint))
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")
spark.sparkContext.setLogLevel("WARN")

ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", IntegerType(), True)

])

path = f"s3a://{minio_bucket}/{minio_file_path}"
ratings = spark.read.csv(path, header=True, schema=ratings_schema)
ratings.show(20, False)

ratings_mod = ratings.withColumn("year", year(col("timestamp").astype(TimestampType())))
ratings_mod.show(20, False)

output_path = f"s3a://{minio_bucket}/{minio_out_file_path}"
ratings_mod.coalesce(1).write.format("csv").mode("overwrite").save(output_path)

df3 = spark.read.csv(output_path)
df3.show(20, False)

spark.stop()