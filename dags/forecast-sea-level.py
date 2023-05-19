import datetime
import pendulum
import os
import pandas as pd
import numpy as np
import pickle
from prophet import Prophet
from pmdarima import auto_arima

from airflow.decorators import dag, task

@dag(
    dag_id="forecase-sea-level",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 5, 16, tz='Asia/Bangkok'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
)
def ForecastSeaLevel():

    @task
    def get_data():
        df = pd.read_csv('/opt/airflow/dags/data/base_sea_level.csv')
        return df

    @task
    def clean_data(data_df):
        data_df['water_level'].replace('-',np.nan,inplace=True)
        data_df['date'] = pd.to_datetime(data_df['date']).dt.date
        data_df['water_level'] = data_df['water_level'].astype('float')
        data_df['water_level'].fillna(data_df['water_level'].median(),inplace=True)
        data_df[data_df['water_level'].isna()]
        data_df.rename(columns={'date':'ds','water_level':'y'},inplace=True)
        t = pd.Timestamp('2023-04-12 00:00:00').date()
        data_df = data_df[data_df['ds'] <= t]
        data_df['ds'] = pd.to_datetime(data_df['ds'])
        data_df.to_csv('/opt/airflow/dags/data/clean_sea_level.csv' \
                       ,index=False, header=True , encoding = 'utf-8-sig')
        return data_df

    @task
    def forecast(clean_df):
        model_sea_level = Prophet()
        model_sea_level.fit(clean_df)
        future_dates_sea_level = model_sea_level.make_future_dataframe(periods=365)
        forecast_sea_level = model_sea_level.predict(future_dates_sea_level)
        record_sea_level = forecast_sea_level[['ds', 'yhat']].copy()
        with open(f'/opt/airflow/dags/data/model/sea_level.pkl','wb') as f:
            pickle.dump(model_sea_level, f)
        return record_sea_level
        
    @task
    def save_data(forecast_df):
        forecast_df.to_csv(f'/opt/airflow/dags/data/output/sea_level_output.csv'\
                                ,index=False,header=True,encoding = 'utf-8-sig')
        return 0

    data_df = get_data()
    clean_df = clean_data(data_df)
    forecast_df = forecast(clean_df)
    save_data(forecast_df)

dag = ForecastSeaLevel()