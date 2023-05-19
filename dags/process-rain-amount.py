import datetime
import pendulum
import requests
import pandas as pd
import time
from datetime import date, timedelta
from bs4 import BeautifulSoup
from airflow.decorators import dag, task,task_group
from airflow.operators.dagrun_operator import TriggerDagRunOperator

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def request_data(date):
  url = 'https://weather.bangkok.go.th/rain/RainHistory/IndexAllStation'
  form_data = {
    'datePick': date,
    'StationTime':'23:55',
    'account':'8'}
  data = requests.post(url, data=form_data, timeout=360)
  soup1 = BeautifulSoup(data.text, "lxml")
  data = soup1.select('tr')
  sdict = {}
  for tr in data[1:]:
    td = tr.select('td')
    try:
      sdict[td[1].string] = float(td[11].string)
    except: sdict[td[1].string] = None
  return sdict


@dag(
    dag_id="process-rain-amount",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 5, 16, tz='Asia/Bangkok'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
)
def ProcessSeaLevel():

    @task
    def get_base_data():
        try:
            return pd.read_csv('/opt/airflow/dags/data/base_rain_amount.csv')
        except: 
            return None

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

        rain_data = request_data('14/05/2023')
        if 'อำเภอเมืองสมุทรปราการ' in rain_data:
            rain_data.pop('อำเภอเมืองสมุทรปราการ')
        data_dict = {'date':[],}
        for sta in rain_data:
            data_dict[sta] = []
        
        now = pendulum.now('Asia/Bangkok')
        
        start_date = date(last_date[0], last_date[1], last_date[2]) + \
        timedelta(days=1)
        end_date = date(now.year, now.month, now.day)

        for single_date in daterange(start_date, end_date):
            date_str = single_date.strftime("%d/%m/%Y")
            ty = 0
            while ty < 10:
                try:
                    data = request_data(date_str)
                    time.sleep(2)
                    ty = 101
                except Exception as e:
                    ty += 1

            data_dict['date'].append(single_date.strftime("%Y-%m-%d"))
            for st in rain_data:
                if st in data:
                    data_dict[st].append(data[st])
                else:
                    data_dict[st].append(None)

        return {'new_df':pd.DataFrame.from_dict(data_dict),'base_df':base_df}
        

    @task
    def merge_data(ar):
        new_df = ar['new_df']
        base_df = ar['base_df']
        try:
            base_df = pd.concat([base_df,new_df])
            base_df.to_csv('/opt/airflow/dags/data/base_rain_amount.csv',index=False)
            return 0
        except: return 1

    trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_rain_forecast",
        trigger_dag_id="forecast-rain-amount",
        wait_for_completion=True,
    )
    merge_data(get_new_data(get_last_date(get_base_data()))) >> trigger_dependent_dag

dag = ProcessSeaLevel()