FROM apache/airflow:2.1.2
RUN sudo apt-get install git
RUN pip install dvc