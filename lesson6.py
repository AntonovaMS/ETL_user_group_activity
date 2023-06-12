from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from datetime import datetime
import boto3
from vertica_python import connect
from airflow.contrib.operators.vertica_operator import VerticaOperator

vertica_conn = 'vertica_conn'

def sprint6_dag_get_data():
    files = ['group_log.csv']
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    for i in files:
        s3_client.download_file(
            Bucket='sprint6',
            Key=i,
            Filename='/data/'+i
        )





with DAG( 

        'lesson6', 
        schedule_interval=None, 
        start_date= datetime(2022,12,14)

) as dag:

    sprint6_dag_get_data = PythonOperator( 
        task_id='get_data_from_s3', 
        python_callable=sprint6_dag_get_data,
        dag=dag)


    load_to_stg_group_log = VerticaOperator(
        task_id= 'load_to_stg_group_log',
        sql="upl_to_stg_group_log.sql", vertica_conn_id= vertica_conn,
        task_concurrency=1, dag=dag)

    stg_to_l_user_group_activity = VerticaOperator(
        task_id= 'stg_to_l_user_group_activity',
        sql="from_stg_to_l_user_group_activity.sql", vertica_conn_id= vertica_conn,
        task_concurrency=1, dag=dag)

    stg_to_s_auth_history = VerticaOperator(
        task_id= 'stg_to_s_auth_history',
        sql="stg_to_s_auth_history.sql", vertica_conn_id= vertica_conn,
        task_concurrency=1, dag=dag)
#добавить очистку таблицы stg_group_log как отдельный шаг в даге после загрузок во все таблицы?
# или же достаточно добавить в файл "stg_to_s_auth_history.sql" в конец строку с очисткой? 
(
    sprint6_dag_get_data >>

    load_to_stg_group_log >>
    
    stg_to_l_user_group_activity >>

    stg_to_s_auth_history


)