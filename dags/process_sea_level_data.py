import datetime
import pendulum
import os

import requests
import pandas as pd
from airflow.decorators import dag, task


@dag(
    dag_id="process-employees",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 5, 18, tz="'Asia/Bangkok'"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
)
def processSeaLevel():

    @task
    def get_data():
        pass

    @task
    def merge_data():
        pass

    get_data() >> merge_data()

dag = processSeaLevel()