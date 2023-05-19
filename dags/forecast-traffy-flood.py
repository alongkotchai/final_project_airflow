import datetime
import pendulum
import os
import pandas as pd
import numpy as np
import pickle
from prophet import Prophet
from pmdarima import auto_arima
from datetime import date, timedelta

from airflow.decorators import dag, task

@dag(
    dag_id="forecast-traffy-flood",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 5, 16, tz='Asia/Bangkok'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
)
def ForecastTraffyFlood():

    @task
    def get_data():
        df = pd.read_csv('/opt/airflow/dags/data/report.csv')
        return df
    
    @task
    def get_rain():
        df = pd.read_csv('/opt/airflow/dags/data/base_rain_amount.csv')
        return df
    
    @task
    def get_sea():
        df = pd.read_csv('/opt/airflow/dags/data/base_sea_level.csv')
        return df
    
    @task
    def get_f_rain():
        df = pd.read_csv('/opt/airflow/dags/data/output/rain_amount_output_all.csv')
        return df
    
    @task
    def get_f_sea():
        df = pd.read_csv('/opt/airflow/dags/data/output/sea_level_output.csv')
        return df
        
    @task
    def clean_data(data_df):
        data_df['timestamp'] = pd.to_datetime(data_df['timestamp']).dt.date
        data_df['report'] = 1
        group_df = data_df.groupby('district')
        district = list()
        for key,value in group_df:
            ser = value['timestamp'].value_counts()
            district.append({'date':ser.index,key:ser.values})
        st_date = date(2019, 9, 19)
        ed_date = date(2023, 4, 21)
        all_day = {'date':[]}
        for n in range(int((ed_date - st_date).days)):
            all_day['date'].append((st_date + timedelta(n)).strftime("%Y-%m-%d"))
            all_day = pd.DataFrame(all_day)
            all_day['date'] = pd.to_datetime(all_day['date']).dt.date
        for d in district:
            tmp = pd.DataFrame(d)
            all_day = all_day.merge(tmp,how='left',on='date')
        all_day.fillna(0,inplace=True)
        all_day.columns = all_day.columns.str.replace('_.*','',regex=True)
        return all_day

    @task
    def forecast(clean_report_df,clean_rain_df,f_rain_df,f_sea_df,clean_sea_df):
        array1 = np.array(f_sea_df['yhat'])
        array2 = { d:np.array(f_rain_df[d]) for d in clean_rain_df.columns[1::]}
        districts = clean_rain_df.columns[1::]
        clean_sea_df = clean_sea_df.rename(columns={'y':'sea_level'})
        for district in districts:
            tmp = clean_rain_df[['date',district]]
            train = tmp.rename(columns={'date':'ds',district:'y'})

            temp_report = clean_report_df[['date', f'{district}']].copy()
            temp_rain_amount= clean_rain_df[['date', f'{district}']].copy()

            temp_report['date'] = pd.to_datetime(clean_report_df['date'])
            temp_rain_amount['date'] = pd.to_datetime(clean_rain_df['date'])

            temp_report.rename(columns={'date':'ds',f'{district}':'y'},inplace=True)
            temp_rain_amount.rename(columns={'date':'ds',f'{district}':'rain_amount'},inplace=True)

            merged_df = pd.concat([temp_report.set_index('ds'), temp_rain_amount.set_index('ds') \
                                   , clean_sea_df.set_index('ds')], axis=1, join='inner').reset_index()

            model_report = Prophet()
            model_report.add_regressor('sea_level')
            model_report.add_regressor('rain_amount')
            model_report.fit(merged_df)

            future_dates_report = model_report.make_future_dataframe(periods=365)
            x = future_dates_report.shape[0]
            future_dates_report['sea_level'] = array1[-x:]
            future_dates_report['rain_amount'] = array2[district][-x:]
            forecast3 = model_report.predict(future_dates_report)

            record_report_amount = forecast3[['ds', 'yhat']].copy()
            record_report_amount.to_csv(f'/opt/airflow/dags/data/output/report_output_{district}.csv'\
                            ,index=False,header=True,encoding = 'utf-8-sig')
            with open(f'/opt/airflow/dags/data/model/report_{district}.pkl','wb') as f:
                    pickle.dump(model_report, f) 

    data_df = get_data()
    clean_df = clean_data(data_df)
    clean_rain_df = get_rain()
    clean_sea_df = get_sea()
    f_rain_df = get_f_rain()
    f_sea_df = get_f_sea()
    forecast(clean_df,clean_rain_df,f_rain_df,f_sea_df,clean_sea_df)

dag = ForecastTraffyFlood()