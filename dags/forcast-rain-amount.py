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
    dag_id="forecast-rain_amount",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 5, 16, tz='Asia/Bangkok'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
)
def ForecastRainAmount():

    @task
    def get_data():
        df = pd.read_csv('/opt/airflow/dags/data/base_rain_amount.csv')
        return df

    @task
    def clean_data(data_df):
        data_df['date'] = pd.to_datetime(data_df['date']).dt.date
        data_df.reset_index(inplace=True)
        data_df.drop(columns=['index'],inplace=True)
        copy = data_df.copy()
        all_district = copy.columns[1::]
        for district in all_district:
            copy[district] = copy[district].fillna(copy[district].mean().round(2))
        t = pd.Timestamp('2023-04-12 00:00:00').date()
        copy = copy[copy['date'] <= t]
        copy.rename(columns={'ป้อมปราบฯ':'ป้อมปราบศัตรูพ่าย','ราษฏร์บูรณะ':'ราษฎร์บูรณะ'},inplace=True)
        copy.to_csv('/opt/airflow/dags/data/clean_rain_amount.csv' \
                        ,index=False, header=True , encoding = 'utf-8-sig')
        return copy

    @task
    def forecast(clean_df):
        districts = clean_df.columns[1::]
        array2 = {}
        al = None
        for district in districts:
            tmp = clean_df[['date',district]]
            train = tmp.rename(columns={'date':'ds',district:'y'})
            
            model_rain_amount = Prophet()
            model_rain_amount.fit(train)
            
            future_dates = model_rain_amount.make_future_dataframe(periods=365)
            forecast_rain_amount = model_rain_amount.predict(future_dates)
            record_rain_amount = forecast_rain_amount[['ds', 'yhat']].copy()

            array2[district] = np.array(record_rain_amount['yhat'])

            record_rain_amount.to_csv(f'/opt/airflow/dags/data/output/rain_amount_output_{district}.csv'\
                            ,index=False,header=True,encoding = 'utf-8-sig')
            with open(f'/opt/airflow/dags/data/model/rain_amount_{district}.pkl','wb') as f:
                    pickle.dump(model_rain_amount, f)

            temp = record_rain_amount.rename(columns={'yhat':district})
            temp = temp[district]
            if type(al) != type(None):
                al = pd.concat([al,temp],axis=1)
            else:
                al = temp
        return al
        
    @task
    def save_data(forecast_df):
        forecast_df.to_csv(f'/opt/airflow/dags/data/output/rain_amount_output_all.csv'\
                                ,index=False,header=True,encoding = 'utf-8-sig')
        return 0

    data_df = get_data()
    clean_df = clean_data(data_df)
    forecast_df = forecast(clean_df)
    save_data(forecast_df)

dag = ForecastRainAmount()