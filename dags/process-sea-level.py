import datetime
import pendulum
import pandas as pd
import time
from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator

@dag(
    dag_id="process-sea-level",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 5, 16, tz='Asia/Bangkok'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=180),
)
def ProcessSeaLevel():

    @task
    def get_base_data():
        try:
            base_df = pd.read_csv('/opt/airflow/dags/data/base_sea_level.csv')
        except: 
            base_df = pd.DataFrame.from_dict({'date':[],'water_level':[]})
        return base_df

    @task(multiple_outputs=True)
    def get_last_date(base_df):
        try:
            last_date = base_df['date'].values[-1:][0]
        except: 
            last_date = '2014-12-31'
        last_date = [int(x) for x in last_date.split('-')]
        return {'last_date':last_date,'base_df':base_df}

    @task(multiple_outputs=True)
    def get_new_data(ar):
        last_date = ar['last_date']
        base_df = ar['base_df']
        data = {'date':[],'water_level':[]}
        url = 'https://tiservice.hii.or.th/opendata/data_catalog/water_level/'
        
        now = pendulum.now('Asia/Bangkok')

        for y in range(last_date[0],now.year+1):
            m = 13
            n = 1
            if y == last_date[0]: n = last_date[1]+1
            if y == now.year: m = now.month
            for i in range(n,m):
                df = pd.read_csv(url+f'{y}/{y}{i:02d}/GLF001.csv')
                time.sleep(2)
                df.drop('time',axis='columns', inplace=True)
                df = df.groupby('date').max()
                data['date'].extend(df.index.tolist())
                data['water_level'].extend(df['water_lv'].tolist())

        return {'new_df':pd.DataFrame.from_dict(data),'base_df':base_df}

    @task
    def merge_data(ar):
        new_df = ar['new_df']
        base_df = ar['base_df']
        try:
            base_df = pd.concat([base_df,new_df])
            base_df.to_csv('/opt/airflow/dags/data/base_sea_level.csv',index=False)
            return 0
        except: return 1

    trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_sea_forecast",
        trigger_dag_id="forecast-sea-level",
        wait_for_completion=True,
    )


    merge_data(get_new_data(get_last_date(get_base_data()))) >> trigger_dependent_dag

dag = ProcessSeaLevel()