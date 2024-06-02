import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import from_json, col, from_unixtime, window, expr, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
import logging

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

logger = logging.getLogger(__name__)
logging.basicConfig(filename=f'{kafka_topic}.log', level=logging.INFO)
logger.info('Started')



spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

class SparkStreamingConsumer:
    def __init__(self):
        self.spark = self.spark_init()
        self.schema = self.schema()
    @staticmethod
    def spark_init():
        """
        spark_init
        :return:
        """
        sparkcontext = pyspark.SparkContext.getOrCreate(
            conf=(
                pyspark.SparkConf().setAppName("DibimbingStreaming")
                       .setMaster(spark_host))
                 )
        sparkcontext.setLogLevel("WARN")
        spark_session = pyspark.sql.SparkSession(sparkcontext.getOrCreate())
        return spark_session
    @staticmethod
    def schema():
        """
        Define the schema
        :return:StructType
        """
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("furniture", StringType(), True),
            StructField("color", StringType(), True),
            StructField("price", IntegerType(), True),
            StructField("ts", StringType(), True),
        ])

        return schema
    @staticmethod
    def write_console(df):
        write_stream_console = df.writeStream \
            .format('console') \
            .outputMode("complete") \
            .option("checkpointLocation", "...") \
            .trigger(processingTime="1 minute") \
            .start()

        logger.info('Write to console')
        write_stream_console.awaitTermination()

    def run(self):
        df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "latest")
            .load()
            .select(from_json(col("value").cast("string"), self.schema).alias("parsed_value"))
        )

        transform_df = df.select("parsed_value.*") \
            .withColumn("ts", from_unixtime(col("ts")).cast(TimestampType())) \
            .withColumn("price", expr("case when price is null then 0 else price end"))

        windowing_agg_df = transform_df \
            .withWatermark("ts", "60 minute") \
            .groupBy(window(col("ts"), "5 minute").alias('timestamp_window')) \
            .agg(sum("price").alias("total_price"))

        #60 Minutes delay threshold and agregate for every 5 minute between

        self.write_console(windowing_agg_df)


if __name__ == "__main__":
    spark_stream = SparkStreamingConsumer()
    spark_stream.run()
