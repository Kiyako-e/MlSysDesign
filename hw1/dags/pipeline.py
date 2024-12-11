from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dataLoader import load_data
from dataSplit import split_data
from minioUpload import minio_upload
from trainModel import train_and_predict

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'pipeline',
    default_args=default_args,
    description='Pipeline for MovieLens dataset',
    schedule_interval=None,
)

download_task = PythonOperator(
    task_id='download_and_unwrap',
    python_callable=load_data,
    dag=dag,
)

split_task = PythonOperator(
    task_id='split',
    python_callable=split_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=minio_upload,
    dag=dag,
)

train_task = PythonOperator(
    task_id='train_and_predict',
    python_callable=train_and_predict,
    dag=dag,
)

download_task >> split_task >> upload_task >> train_task
