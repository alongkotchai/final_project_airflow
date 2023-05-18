import datetime
import pendulum
import os
import requests
import pandas as pd
import time
from datetime import date, timedelta
from bs4 import BeautifulSoup
from airflow.decorators import dag, task

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def request_data(date):
  url = 'https://weather.bangkok.go.th/rain/RainHistory/IndexAllStation'
  form_data = {
    'datePick': date,
    'StationTime':'23:55',
    'account':'8'}
  data = requests.post(url, data=form_data, timeout=60)
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
    dag_id="process-employees",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 5, 18, tz='Asia/Bangkok'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
)
def processSeaLevel():

    @task
    def get_data():

        rain_data = request_data('14/05/2023')
        rain_data.pop('อำเภอเมืองสมุทรปราการ')
        data_dict = {'date':[],}
        for sta in rain_data:
            data_dict[sta] = []
        
        now = pendulum.now('Asia/Bangkok')
        base_df = pd.read_csv('data/base_rain_amount.csv')
        try:
            last_date = base_df['date'].values[-1:][0]
        except:
            last_date = '2015-01-01'
        last_date = last_date.split('-')

        start_date = date(int(last_date[0]), int(last_date[1]), int(last_date[2]))
        end_date = date(now.year, now.month, now.day)

        for single_date in daterange(start_date, end_date):
            date_str = single_date.strftime("%d/%m/%Y")
            ty = 0
            while ty < 100:
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

        scrape_df = pd.DataFrame.from_dict(data_dict)
        scrape_df.to_csv('data/temp_rain_amount.csv',index=False)
        

    @task
    def merge_data():
        try:
            base_df = pd.read_csv('data/base_rain_amount.csv')
            temp_df = pd.read_csv('data/temp_rain_amount.csv')
            base_df = pd.concat([base_df,temp_df])
            base_df.to_csv('data/base_rain_amount.csv',index=False)
            return 0
        except: return 1

    get_data() >> merge_data()

dag = processSeaLevel()