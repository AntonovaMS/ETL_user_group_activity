
import requests
import psycopg2
import json
import time
from datetime import datetime, timedelta 

from airflow import DAG 
from airflow.operators.python import PythonOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash_operator import BashOperator

headers = {'X-Nickname': 'antonovams',
           'X-Cohort': '2',
           'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}
postgres_conn_id = 'postgresql_de' 
dwh_hook = PostgresHook(postgres_conn_id)
conn = dwh_hook.get_conn()
cursor = conn.cursor()


def api_to_stg_restaurants():
    url_restaurants = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field=id&sort_direction=asc&limit=50offset=0'
    response_restaurants = requests.get(url_restaurants, headers=headers).json()
    dwh_hook = PostgresHook(postgres_conn_id)
    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    for doc in response_restaurants:
        id = doc["_id"]
        name = doc["name"]
        query_sql = f"insert into stg.dm_restaurants(id,name) values ( '{id}','{name}')"
        cursor.execute(query_sql)
        conn.commit()
        time.sleep(1)
    cursor.close()


def  api_to_stg_couriers():
    Flag = True
    i = 0
    while Flag == True:
    
        url_couriers = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=desc&sort_direction=desc&limit=50&offset={i}'
        response_couriers = requests.get(url_couriers, headers=headers).json()
        if   response_couriers==[]:
            Flag = False
            print('couriers_FlagFalse')
            break  
        else:    
            dwh_hook = PostgresHook(postgres_conn_id)
            conn = dwh_hook.get_conn()
            cursor = conn.cursor()
            for doc in response_couriers:
                id = doc["_id"]
                name = doc["name"]
                query_sql = f"insert into stg.dm_couriers(id,name) values ( '{id}','{name}')"
                cursor.execute(query_sql)
                conn.commit()
                time.sleep(1)
            cursor.close()
                           
        i+=50

business_dt = '{{ ds }}'+' 00:00:00'
business_dt2 = '{{ ds }}'+' 23:59:59'

def api_to_stg_deliveries(date, date2):
    
    Flag = True
    i = 0
    while Flag == True:
        url_deliveries = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?&from={date}&to={date2}&sort_field=id&sort_direction=asc&offset={i}'
        response_deliveries = requests.get(url_deliveries, headers=headers).json()
        if   response_deliveries==[]:
            Flag = False
            print('deliveries_FlagFalse')
            break  
        else:   
            dwh_hook = PostgresHook(postgres_conn_id)
            conn = dwh_hook.get_conn()
            cursor = conn.cursor()
           
            for doc in response_deliveries :
                order_id = doc["order_id"]
                order_ts = doc["order_ts"]
                delivery_id = doc["delivery_id"]
                courier_id = doc["courier_id"]
                address = doc["address"]
                delivery_ts = doc["delivery_ts"]
                rate = doc["rate"]
                sum = doc["sum"]
                tip_sum = doc["tip_sum"]
                query_sql = f"insert into stg.fct_deliveries(order_id ,order_ts ,delivery_id ,courier_id ,address ,delivery_ts ,rate ,sum ,tip_sum ) values ('{order_id}','{order_ts}','{delivery_id}','{courier_id}','{address}','{delivery_ts}','{rate}','{sum}','{tip_sum}')"
                cursor.execute(query_sql)
                conn.commit()
                time.sleep(1)
            cursor.close()

        i+=50







args = { 

    "owner": "student", 
    'email': ['student@example.com'], 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1 ,
    'schedule_interval': '0 0 * * *',
    'start_date' :datetime(2022,12,14)} 

with DAG( 

        'couriers_ledger', 
        default_args=args, 
        description='Dag for lesson5', 
        catchup=True

) as dag: 
    api_to_stg_restaurants = PythonOperator( 
        task_id='rest', 
        python_callable=api_to_stg_restaurants,
        dag=dag) 
 
    api_to_stg_couriers = PythonOperator( 
        task_id='cour', 
        python_callable=api_to_stg_couriers,
        dag=dag) 

    api_to_stg_deliveries = PythonOperator( 
        task_id='deliv', 
        python_callable=api_to_stg_deliveries,
        op_kwargs={'date': business_dt, 'date2': business_dt2},
        dag=dag) 

    update_dds_dm_restaurants_table = PostgresOperator( 
        task_id='update_dds_dm_restaurants', 
        postgres_conn_id=postgres_conn_id, 
        sql="update_dds_dm_restaurants.sql")
 
    update_dds_dm_couriers_table = PostgresOperator( 
        task_id='update_dds_dm_couriers', 
        postgres_conn_id=postgres_conn_id, 
        sql="update_dds_dm_couriers.sql")

    update_dds_dm_deliveries_table = PostgresOperator( 
        task_id='update_dds_dm_deliveries', 
        postgres_conn_id=postgres_conn_id, 
        sql="update_dds_dm_deliveries.sql")

    upsert_cdm_dm_courier_ledger_table = PostgresOperator( 
        task_id='update_cdm_dm_courier_ledger', 
        postgres_conn_id=postgres_conn_id, 
        sql="upsert_dm_courier_ledger.sql",
        parameters={"date": {business_dt}})
 


( 

[api_to_stg_restaurants, api_to_stg_couriers, api_to_stg_deliveries] 

>>[update_dds_dm_restaurants_table, update_dds_dm_couriers_table, update_dds_dm_deliveries_table] 

>> upsert_cdm_dm_courier_ledger_table

)