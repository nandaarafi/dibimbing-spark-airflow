from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
import os


postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_port = os.getenv('POSTGRES_PORT')
postgres_db = os.getenv('POSTGRES_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_dw_db = "warehouse"

spark_master = os.getenv('SPARK_MASTER_HOST_NAME')
spark_driver = "org.postgresql.Driver"

jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_dw_db}"


default_args = {
    "owner": "rafi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_postgres_dag",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="spark postgres tes",
    start_date=days_ago(1),
    catchup=False,

) as dag:
    start_task = EmptyOperator(task_id="start_task", )
    end_task   = EmptyOperator(task_id="end_task")


    spark_start = SparkSubmitOperator(
                application="/spark-scripts/spark-read-postgres.py",
                conn_id="spark_main",
                task_id="spark_submit_task",
                conf={"spark.master":spark_master},
                application_args=[jdbc_url,postgres_user,postgres_password, spark_driver],
                packages='org.postgresql:postgresql:42.2.18'
                )

    start_task >> spark_start >> end_task
