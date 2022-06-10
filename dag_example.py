from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
import requests
import boto3
import psycopg2, psycopg2.extras

dag = DAG(
        'etl_update_user_data',
        default_args={
            'owner': 'student',
            'email': ['student@example.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Move user data from files on S3 to Postgres',
        schedule_interval='0 12 * * *',
        catchup=True,
        start_date=datetime(2021, 1, 1),
        end_date=datetime(2021, 2, 10),
        params={'business_dt': '{{ ds }}'}
        )

def create_files_request(conn_name, business_dt):
    conn = BaseHook.get_connection(conn_name)
    host = conn.host
    method = f"/{conn_name}"
    requests.post(conn.host, data={'business_dt': business_dt})

create_files_request = PythonOperator(
    task_id='create_files_request',
    python_callable=create_files_request,
    op_kwargs={'conn_name':'create_files_api', 'business_dt': '{{ ds }}'},
    dag=dag)

def get_files_from_s3(business_dt,s3_conn_name):
    # опишите тело функции
    session = boto3.session.Session()
    BUCKET_NAME = 's3-sprint3'
    key ='cohort_1/nkurdyubov/TWpBeU1pMHdOUzB5TkZReE16b3lPVG93TXdsaGRTMXlZVzFoZW1GdQ==/'
    s3 = session.client(
        service_name=s3_conn_name,
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id = 'YCAJEWXOyY8Bmyk2eJL-hlt2K',
        aws_secret_access_key = 'YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA'
    )
    for s3_filename in ['customer_research.csv','user_order_log.csv',
                    'user_activity_log.csv']:
        local_filaname = '/lessons/5. Реализация ETL в Airflow/4. Extract: как подключиться к хранилищу, чтобы получить файл/Задание 2/' + business_dt.replace('-','') + '_' + s3_filename # add date to filename
        key_i = key+s3_filename
        s3.download_file(
            Bucket=BUCKET_NAME,
            Key=key_i,
            Filename=local_filaname
        )

get_files = PythonOperator(task_id='get_files_task',
                                python_callable=get_files_from_s3,
                                op_kwargs={'business_dt': '{{business_dt}}',
                                's3_conn_name': 's3'},
                                dag=dag)

def load_file_to_pg(filename,pg_table,conn_args):
    ''' csv-files to pandas dataframe '''
    f = pd.read_csv(filename)
    
    ''' load data to postgres '''
    cols = ','.join(list(f.columns))
    insert_stmt = f"INSERT INTO {pg_table} ({cols}) VALUES %s"
    
    pg_conn = psycopg2.connect(**conn_args)
    cur = pg_conn.cursor()
    psycopg2.extras.execute_values(cur, insert_stmt, f.values)
    pg_conn.commit()
    cur.close()
    pg_conn.close()

pg_conn = BaseHook.get_connection('pg_connection')

load_customer_research = PythonOperator(task_id='load_customer_research',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': '{{ds}}'.replace('-','') + '_customer_research.csv',
                                                'pg_table': 'stage.customer_research',
                                                'conn_args': pg_conn},
                                    dag=dag)

load_user_order_log = PythonOperator(task_id='load_user_order_log',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': '{{ds}}'.replace('-','') + '_user_order_log.csv',
                                                'pg_table': 'stage.user_order_log',
                                                'conn_args': pg_conn},
                                    dag=dag)

load_user_activity_log = PythonOperator(task_id='load_user_activity_log',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': '{{ds}}'.replace('-','') + '_user_activity_log.csv',
                                                'pg_table': 'stage.user_activity_log',
                                                'conn_args': pg_conn},
                                    dag=dag)

load_price_log = PythonOperator(task_id='user_order_log',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': '{{ds}}'.replace('-','') + '_price_log.csv',
                                                'pg_table': 'stage.price_log',
                                                'conn_args': pg_conn},
                                    dag=dag)

def pg_execute_query(query,conn_args):
    conn = psycopg2.connect(**conn_args)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

dim_upd_sql_query = '''
-- UPDATE
update mart.d_item
set item_name = (select distinct s.name from  
    (select distinct item_id, last_value(item_name) over(partition by item_id order by date_time desc) as name 
            from stage.user_order_log) s where s.item_id = item_id)


-- INSERT -- не удаляйте эту строку
insert into public.d_item (item_id, item_name)
select 
distinct item_id, last_value(item_name) over(partition by item_id order by date_time desc) as name from stage.user_order_log
where item_id not in (select distinct item_id from mart.d_item)
order by item_id
'''

facts_upd_sql_query = '''
-- DELETE
delete from mart.f_daily_sales
where date_id in (select distinct date_time from stage.user_order_log)

-- INSERT -- не удаляйте эту строку
insert into mart.f_daily_sales
(date_id,
item_id,
customer_id,
quantity,
payment_amount)
select date_time as date_id, item_id, customer_id, quantity, payment_amount as amount
from stage.user_order_log
'''

pg_conn = BaseHook.get_connection('pg_connection')
update_dimensions = PythonOperator(task_id='update_dimensions',
                                    python_callable=pg_execute_query,
                                    op_kwargs={'query': dim_upd_sql_query,
                                                'conn_args': pg_conn},
                                    dag=dag)
                                    
update_facts = PythonOperator(task_id='update_facts',
                                    python_callable=pg_execute_query,
                                    op_kwargs={'query': facts_upd_sql_query,
                                                'conn_args': pg_conn},
                                    dag=dag)

create_files_request >> get_files >> [load_customer_research, load_user_order_log, load_user_activity_log, load_price_log] >> update_dimensions >> update_facts