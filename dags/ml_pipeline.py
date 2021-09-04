from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import json


def _train():
    url = 'http://172.23.0.1:5000/train/'
    r = requests.get(url)
    print(r, r.text)


with DAG(dag_id='ml_pipeline', schedule_interval='@daily', start_date=datetime(2021,8,17), catchup=False) as dag:
    p0 = BashOperator(task_id='training_data', bash_command = "cd MLOps; git pull; dvc pull;")
    p1 = PythonOperator(task_id='model', python_callable = _train)
    p2 = BashOperator(task_id='repository', bash_command = "cd MLOps; dvc commit; dvc push; git commit data/.gitignore data/images.dvc -m 'latest model commit by ml_pipeline dag'; git push")
    # ideally p2 is a bash operator that takes run_id from the train output and executes: mlflow models serve -m runs:/<RUN_ID>/model, but I could not find how to send images for predictions instead of pandas dataframes
    p0 >> p1 >> p2
