import datetime
import pendulum
import os
import pandas as pd
import numpy as np
import pickle
from prophet import Prophet
from pmdarima import auto_arima
from datetime import date, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.baseoperator import chain
import time
from airflow.decorators import dag, task

@dag(
    dag_id="forecast-traffy-flood",
    schedule_interval='0 1 * * *',
    start_date=pendulum.datetime(2023, 5, 16, tz='Asia/Bangkok'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
)
def ForecastTraffyFlood():

    @task
    def get_data(**kwargs):
        ti = kwargs["ti"]
        df = pd.read_csv('/opt/airflow/dags/data/report.csv')
        ti.xcom_push("base_data_df", df)
    
    @task
    def get_rain(**kwargs):
        ti = kwargs["ti"]
        df = pd.read_csv('/opt/airflow/dags/data/clean_rain_amount.csv')
        ti.xcom_push("clean_rain_df", df)
    
    @task
    def get_sea(**kwargs):
        ti = kwargs["ti"]
        df = pd.read_csv('/opt/airflow/dags/data/clean_sea_level.csv')
        ti.xcom_push("clean_sea_df", df)
    
    @task
    def get_f_rain(**kwargs):
        ti = kwargs["ti"]
        df = pd.read_csv('/opt/airflow/dags/data/output/rain_amount_output_all.csv')
        ti.xcom_push("f_rain_df", df)
    
    @task
    def get_f_sea(**kwargs):
        ti = kwargs["ti"]
        df = pd.read_csv('/opt/airflow/dags/data/output/sea_level_output.csv')
        ti.xcom_push("f_sea_df", df)
        
    @task
    def clean_data(**kwargs):
        ti = kwargs["ti"]
        df = ti.xcom_pull(task_ids="get_data", key="base_data_df")
        ti.xcom_push("clean_data_df", df)
        # data_df['date'] = pd.to_datetime(data_df['date']).dt.date
        # data_df['report'] = 1
        # group_df = data_df.groupby('district')
        # district = list()
        # for key,value in group_df:
        #     ser = value['date'].value_counts()
        #     district.append({'date':ser.index,key:ser.values})
        # st_date = date(2019, 9, 19)
        # ed_date = date(2023, 4, 21)
        # all_day = {'date':[]}
        # for n in range(int((ed_date - st_date).days)):
        #     all_day['date'].append((st_date + timedelta(n)).strftime("%Y-%m-%d"))
        #     all_day = pd.DataFrame(all_day)
        #     all_day['date'] = pd.to_datetime(all_day['date']).dt.date
        # for d in district:
        #     tmp = pd.DataFrame(d)
        #     all_day = all_day.merge(tmp,how='left',on='date')
        # all_day.fillna(0,inplace=True)
        # all_day.columns = all_day.columns.str.replace('_.*','',regex=True)
        #return data_df

    @task
    def forecast(**kwargs):
        ti = kwargs["ti"]
        clean_report_df = ti.xcom_pull(task_ids="clean_data", key="clean_data_df")
        clean_rain_df = ti.xcom_pull(task_ids="get_rain", key="clean_rain_df")
        clean_sea_df = ti.xcom_pull(task_ids="get_sea", key="clean_sea_df")
        f_rain_df = ti.xcom_pull(task_ids="get_f_rain", key="f_rain_df")
        f_sea_df = ti.xcom_pull(task_ids="get_f_sea", key="f_sea_df")
        array1 = np.array(f_sea_df['yhat'])
        array2 = { d:np.array(f_rain_df[d]) for d in clean_rain_df.columns[1::]}
        districts = clean_rain_df.columns[1::]
        clean_sea_df = clean_sea_df.rename(columns={'y':'sea_level'})
        for district in districts:
            tmp = clean_rain_df[['date',district]]
            train = tmp.rename(columns={'date':'ds',district:'y'})
            temp_report = clean_report_df[['date', f'{district}']].copy()
            temp_rain_amount= clean_rain_df[['date', f'{district}']].copy()
            # temp_report['date'] = pd.to_datetime(temp_report['date'])
            # temp_rain_amount['date'] = pd.to_datetime(temp_rain_amount['date'])
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

    wait_for_sea_level_forecast = ExternalTaskSensor(
        task_id="wait_sea_level_forecast",
        external_dag_id="process-sea-level",
        external_task_id="trigger_sea_forecast",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_delta= timedelta(hours=1),
        poke_interval = 10
    )

    wait_for_rain_amount_forecast = ExternalTaskSensor(
        task_id="wait_rain_amount_forecast",
        external_dag_id="process-rain-amount",
        external_task_id="trigger_rain_forecast",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_delta= timedelta(hours=1),
        poke_interval = 10
    )

    chain([ wait_for_rain_amount_forecast, wait_for_sea_level_forecast],get_data(),clean_data(), \
                                [get_rain() , get_sea(), get_f_rain(), get_f_sea()],forecast())

dag = ForecastTraffyFlood()