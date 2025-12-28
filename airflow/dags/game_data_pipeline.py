from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="game_pyspark_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["game", "pyspark", "etl"],
    default_args={"retries": 2, "retry_delay": 300}
) as dag:

    etl = BashOperator(
        task_id="etl_process",
        bash_command="""
        echo "Bắt đầu ETL với PySpark..." &&
        cd /opt/airflow/ETL-Process &&
        spark-submit \
          --master local[*] \
          --driver-class-path /opt/airflow/postgresql-42.7.4.jar \
          --jars /opt/airflow/postgresql-42.7.4.jar \
          --packages org.mongodb.spark:mongo-spark-connector_2.13:10.4.0,org.postgresql:postgresql:42.7.4 \
          PYSPARK.py &&
        echo "ETL hoàn thành!"
        """,
        cwd="/opt/airflow/ETL-Process"
    )

    ml = BashOperator(
        task_id="ml_clustering",
        bash_command="""
        echo "Bắt đầu ML Clustering với PySpark..." &&
        cd /opt/airflow/ML &&
        spark-submit \
          --master local[*] \
          --driver-class-path /opt/airflow/postgresql-42.7.4.jar \
          --jars /opt/airflow/postgresql-42.7.4.jar \
          --packages org.postgresql:postgresql:42.7.4 \
          game_clustering.py &&
        echo "ML Clustering hoàn thành!"
        """,
        cwd="/opt/airflow/ML"
    )

    etl >> ml