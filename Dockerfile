FROM apache/airflow:2.6.1-python3.10
ADD requirements.txt . 
RUN pip install -r requirements.txt