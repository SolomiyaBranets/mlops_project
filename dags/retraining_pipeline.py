from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import shutil
import re
import os

def _extend_training_set():
    #function that moves all the files from the /verified folder, renames them and moves to /images folder - where the training data is
    
    def image_name(breed):
        #function that takes a breed, looks for the last number of the file with that breen in images folder and returns what should be the new name

        def atoi(text):
            return int(text) if text.isdigit() else text

        def natural_keys(text):
            '''
            alist.sort(key=natural_keys) sorts in human order
            http://nedbatchelder.com/blog/200712/human_sorting.html
            (See Toothy's implementation in the comments)
            '''
            return [ atoi(c) for c in re.split(r'(\d+)', text) ]
        
        res = [i for i in os.listdir('MLOps/data/images') if breed in i]
        if len(res) > 0:
            res.sort(key=natural_keys)
            latest_num = int(re.split(r'(\d+)', res[-1])[-2])
            image_name = breed + "_"+str(latest_num+1)+".jpg"
        else: 
            image_name = breed + "_"+str(1)+".jpg"
        return image_name
    
    #loop to go through all the files in the /verified folder and move them to the /images folder
    if not len(os.listdir('MLOps/data/verified/')) == 0:
        for i in os.listdir('MLOps/data/verified'):
            name = image_name(breed = i.split(" ")[0])
            #move file to images folder
            shutil.move("MLOps/data/verified/"+i, "MLOps/data/images/"+name)
        return("moved to images") 
    else: return("empty directory") 

with DAG(dag_id='retraining_pipeline', schedule_interval='@daily', start_date=datetime(2021,8,17), catchup=False) as dag:
    p0 = PythonOperator(task_id='extend_training_set', python_callable = _extend_training_set)
    p1 = BashOperator(task_id='commit_new_training_data', bash_command = "cd MLOps; dvc commit; dvc push; git commit data/.gitignore data/images.dvc -m 'new data commit by retraining_pipeline dag'; git push")
    p0 >> p1
