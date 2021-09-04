from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import json
import os
import shutil


def _process():
    #function that takes first file from the /new_images folder and retrieves a prediction

    url = 'http://172.23.0.1:5000/predict/'
    if not len(os.listdir('MLOps/data/new_images')) == 0:
        data = os.listdir('MLOps/data/new_images')[0]
        j_data = json.dumps(data)
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = requests.post(url, data=j_data, headers=headers)
        return(json.loads(r.text))
    else:
        return({"path": None, "category": None, "probability" : None})


def verification(responce, **kwargs):
    #function that checks if prediction is < 0.8 and copies the file to /verify folder. It also includes the predicted breed in the name of the file 
    
    delimiter = " "

    if not os.path.exists("MLOps/data/verify"):
        os.makedirs("MLOps/data/verify")

    if responce["path"] is not None:
        if responce["probability"] <= 0.8:
           #copy file to verify folder
           shutil.copy("MLOps/data/new_images/"+responce["path"], "MLOps/data/verify/"+responce["category"]+delimiter+datetime.now().strftime("%Y-%m-%d-%H-%M-%S")+delimiter+responce["path"])
           return("added for verification") 
        else: return("no need to verify")        
    else: return("empty directory")

def outgoing(responce, **kwargs):
    #function that moves a file from /new_images to outgoing. It also includes the predicted breed in the name of the file 

    delimiter = " "

    if not os.path.exists("MLOps/data/outgoing"):
        os.makedirs("MLOps/data/outgoing")

    if responce["path"] is not None:
        #move file to outgoing folder
        shutil.move("MLOps/data/new_images/"+responce["path"], "MLOps/data/outgoing/"+responce["category"]+delimiter+datetime.now().strftime("%Y-%m-%d-%H-%M-%S")+delimiter+responce["path"])
        return("moved to outgoing") 
    else: 
        return("empty directory")


with DAG(dag_id='data_pipeline', schedule_interval='@None', start_date=datetime(2021,8,17), catchup=False) as dag:
    p0 = PythonOperator(task_id='processing', python_callable = _process)
    p1 = PythonOperator(task_id='verification', python_callable = verification, 
                                                op_kwargs={'responce': p0.output})
    p2 = PythonOperator(task_id='outgoing', python_callable = outgoing, 
                                                op_kwargs={'responce': p0.output})
    p0 >> p1 >> p2