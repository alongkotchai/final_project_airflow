import datetime
import pendulum
import os
import requests
import pandas as pd
import time
from airflow.decorators import dag, task


@dag(
    dag_id="process-employees",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 5, 18, tz='Asia/Bangkok'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
)
def processSeaLevel():

    @task
    def get_data():
        data = {'date':[],'water_level':[]}
        url = 'https://tiservice.hii.or.th/opendata/data_catalog/water_level/'
        base_df = pd.read_csv('data/base_sea_level.csv')
        try:
            last_date = base_df['date'].values[-1:][0]
        except:
            last_date = '2015-01-01'
        last_date = last_date.split('-')
        last_year = int(last_date[0])
        last_month = int(last_date[1])

        now = pendulum.now('Asia/Bangkok')

        for y in range(last_year,now.year+1):
            for i in range(last_month,now.month):
                print(y,',',i)
                df = pd.read_csv(url+f'{y}/{y}{i:02d}/GLF001.csv')
                time.sleep(1)
                df.drop('time',axis='columns', inplace=True)
                df = df.groupby('date').max()
                data['date'].extend(df.index.tolist())
                data['water_level'].extend(df['water_lv'].tolist())

        scrape_df = pd.DataFrame.from_dict(data)
        scrape_df.to_csv('data/temp_sea_level.csv',index=False)

    @task
    def merge_data():
        try:
            base_df = pd.read_csv('data/base_sea_level.csv')
            temp_df = pd.read_csv('data/temp_sea_level.csv')
            base_df = pd.concat([base_df,temp_df])
            base_df.to_csv('data/base_sea_level.csv',index=False)
            return 0
        except: return 1

    get_data() >> merge_data()

dag = processSeaLevel()