from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit \
    import SparkSubmitOperator
from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession

"""
def get_spark_session(app_name: str = 'Default app name', 
        master: str = 'spark://spark-master:7077') -> SparkSession:
    config = {
        'spark.executor.memory': '512m',
        'spark.executor.cores': '1',
        'spark.dynamicAllocation.enabled': 'true',
        'hive.metastore.warehouse.dir': 'false'
    }
    master = 'spark://spark-master:7077'
    spark = (
        SparkSession
            .builder 
            .appName(app_name) 
            .config(map=config) 
            .master(master) 
            .getOrCreate()
    )
    return spark
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    'spark',
    default_args=default_args,
    description='Test dag',
    schedule_interval=None,
) as dag:
    start = PythonOperator(
        task_id='start_spark_job',
        python_callable = lambda: print('Jobs started'),
        dag=dag
    )

    spark_job = SparkSubmitOperator(
        task_id='spark_job',
        conn_id='default_spark',
        application='jobs/spark_job.py',
        files='data/test.csv',
        dag=dag
    )
    
    end = PythonOperator(
        task_id='end_spark_job',
        python_callable = lambda: print('Jobs finished'),
        dag=dag
    )
    
start >> spark_job >> end