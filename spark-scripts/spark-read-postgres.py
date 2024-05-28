from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import *

def init_spark() -> SparkSession:
    sparkcontext = pyspark.SparkContext.getOrCreate(
        conf=(pyspark.SparkConf().setAppName("Dibimbing"))
    )
    sparkcontext.setLogLevel("WARN")

    spark_session = pyspark.sql.SparkSession(sparkcontext.getOrCreate())
    return spark_session

def clean_data(retail_dataframe: DataFrame) -> DataFrame:
    retail_clean = retail_dataframe.dropDuplicates()
    retail_clean = retail_clean.na.drop()
    return retail_clean

def calculate_total_product_sales(retail_dataframe: DataFrame) -> DataFrame:
    total_product_sales = retail_dataframe.groupBy("StockCode") \
                                          .agg(sum("Quantity").alias("Total Quantity"),
                                               sum(col("Quantity") * col("UnitPrice")).alias("TotalSalesAmount"))
    return total_product_sales

def calculate_customer_performance(retail_dataframe: DataFrame) -> DataFrame:
    customer_performance = retail_dataframe.groupby("CustomerID", "InvoiceDate") \
                                        .agg(count("InvoiceNo").alias("TotalPurchases"),
                                             sum(col("Quantity") * col("UnitPrice")).alias("TotalAmountSpent"))
    return customer_performance

if __name__ == '__main__':
    #Create Spark Session
    spark = init_spark()

    jdbc_url = sys.argv[1]
    postgres_user = sys.argv[2]
    postgres_pwd = sys.argv[3]
    spark_driver = sys.argv[4]

    print("######################################")
    print("READING POSTGRES TABLES")
    print("######################################")

    df_retail = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.retail")
        .option("user", postgres_user)
        .option("password", postgres_pwd)
        .option("driver", spark_driver)
        .load()
    )

    df_retail_clean = clean_data(df_retail)

    print("######################################")
    print("TRANSFORM DATA TABLES")
    print("######################################")

    df_total_product_sales = calculate_total_product_sales(df_retail_clean)
    df_customer_activity = calculate_customer_performance(df_retail_clean)

    print("######################################")
    print("LOADING POSTGRES TABLES")
    print("######################################")

    df_total_product_sales \
                 .write \
                 .format("jdbc") \
                 .option("url", jdbc_url) \
                 .option("dbtable", "public.total_product_sales") \
                 .option("user", postgres_user) \
                 .option("password", postgres_pwd) \
                 .option("driver", spark_driver) \
                 .mode("overwrite") \
                 .save()


    df_customer_activity \
                 .write \
                 .format("jdbc") \
                 .option("url", jdbc_url) \
                 .option("dbtable", "public.customer_activity") \
                 .option("user", postgres_user) \
                 .option("password", postgres_pwd) \
                 .option("driver", spark_driver) \
                 .mode("overwrite") \
                 .save()

    print("######################################")
    print("STORING DATA TO POSTGRES SUCCESS")
    print("######################################")

