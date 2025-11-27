from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph
import numpy as np

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#параметры соединения
connection = {'host': '...',
'database':'...',
'user':'...',
'password':'...'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-ishmuhametov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 11, 15),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *' # ежедневно в 23:00

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl():

    @task()
    def extract_fa():
        query_fa = """
            SELECT 
               toDate(time) as event_date, 
               user_id,
               gender, 
               age,
               os,
               countIf(action='view') as views,
               countIf(action='like') as likes
            FROM 
                simulator_20251020.feed_actions 
            where 
                toDate(time) = toDate(now()) - 1
            group by
                event_date,
                user_id,
                gender, 
                age,
                os
            order by 
                event_date,
                user_id,
                gender, 
                age,
                os"""
        fa = ph.read_clickhouse(query=query_fa, connection=connection)
        return fa

    @task()
    def extract_fm():
        query_fm ='''
            SELECT   t.event_date as event_date,
                     t.user_id as user_id,
                     gender, 
                     age,
                     os,
                     t1.messages_received, t2.messages_sent, t3.users_received, t4.users_sent
            FROM (
                  SELECT 
                     toDate(a.time) as event_date, 
                     user_id,
                     gender, 
                     age,
                     os
                  FROM 
                      simulator_20251020.message_actions a
                  where 
                      event_date = toDate(now()) - 1
                  group by
                      event_date,
                      user_id,    
                      gender, 
                      age,
                      os
                  ) t
            LEFT JOIN ( -- Число полученных сообщений - messages_received
                  SELECT 
                      toDate(time) as event_date,
                      receiver_id,
                      count(receiver_id) as messages_received
                  FROM 
                      simulator_20251020.message_actions
                  GROUP BY  
                      event_date,
                      receiver_id
                  ) t1 on t1.event_date=t.event_date and t1.receiver_id = t.user_id
            LEFT JOIN (-- Число отправленных сообщений - messages_sent
                  SELECT 
                      toDate(time) as event_date,
                      user_id,
                      count(user_id) as messages_sent
                  FROM 
                      simulator_20251020.message_actions 
                  GROUP BY  
                      event_date,
                      user_id
                  ) t2 on t2.event_date=t.event_date and t2.user_id = t.user_id    
            LEFT JOIN (-- От скольких пользователей получили сообщения - users_received
                  SELECT 
                      toDate(time) as event_date,
                      receiver_id,
                      count(distinct user_id) as users_received
                  FROM 
                      simulator_20251020.message_actions 
                  GROUP BY  
                      event_date,
                      receiver_id
                  ) t3 on t3.event_date=t.event_date and t3.receiver_id = t.user_id       
            LEFT JOIN (-- Скольким пользователям отправили сообщение - users_sent
                  SELECT 
                      toDate(time) as event_date,
                      user_id,
                      count(distinct receiver_id) as users_sent
                  FROM 
                      simulator_20251020.message_actions 
                  GROUP BY  
                      event_date,
                      user_id
                  ) t4 on t4.event_date=t.event_date and t4.user_id = t.user_id       
            order by 
                t.event_date,
                t.user_id
            '''
        fm = ph.read_clickhouse(query=query_fm, connection=connection)
        return fm    
    
 
    @task
    def transfrom(fa, fm):
        df = pd.merge(fa, fm, on=['event_date', 'user_id'], how='outer')
        df['gender'] = df.apply(lambda row: row['gender_x'] if not pd.isnull(row['gender_x']) else row['gender_y'], axis=1)
        df['age'] = df.apply(lambda row: row['age_x'] if not pd.isnull(row['age_x']) else row['age_y'], axis=1)
        df['os'] = df.apply(lambda row: row['os_x'] if not pd.isnull(row['os_x']) else row['os_y'], axis=1)
        df = df[['event_date', 'user_id', 'gender', 'age', 'os', 'views','likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df = df.fillna(0)
        m = df.select_dtypes(np.number)
        df[m.columns]= m.round().astype('int')
        return df

    @task
    def transfrom_gender(df):
        df_gender = df[['event_date', 'gender', 'views','likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df_gender = df_gender.groupby(['event_date', 'gender']).sum().reset_index()
        df_gender['dimension'] = 'gender'
        df_gender = df_gender.rename(columns={'gender': 'dimension_value'})
        df_gender = df_gender[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        return df_gender
    
    @task
    def transfrom_age(df):
        df_age = df[['event_date', 'age', 'views','likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df_age = df_age.groupby(['event_date', 'age']).sum().reset_index()
        df_age['dimension'] = 'age'
        df_age = df_age.rename(columns={'age': 'dimension_value'})
        df_age = df_age[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        return df_age
    
    @task
    def transfrom_os(df):
        df_os = df[['event_date', 'os', 'views','likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df_os = df_os.groupby(['event_date', 'os']).sum().reset_index()
        df_os['dimension'] = 'os'
        df_os = df_os.rename(columns={'os': 'dimension_value'})
        df_os = df_os[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        return df_os
    
    @task
    def load(df_gender, df_age, df_os):
        #запрос создания таблички 
        query_test = '''CREATE TABLE IF NOT EXISTS test.dag_iaf
                            (event_date Date,
                             dimension String,
                             dimension_value String,
                             views UInt32,
                             likes UInt32,
                             messages_received UInt32,
                             messages_sent UInt32,
                             users_received UInt32,
                             users_sent UInt32
                            )
                            ENGINE = MergeTree()
                            ORDER BY event_date
        '''
        df_final = pd.concat([df_gender, df_age, df_os], axis=0)
        context = get_current_context()
        ds = context['ds']
       
        ph.execute(query_test, connection=connection_test)
        # --- Загрузка в ClickHouse ---
        ph.to_clickhouse(
            df_final,
            table='dag_iaf',
            index=False,
            connection=connection_test)
        
        print(f'Metrics for {ds}')
        print(df_final)


    fa = extract_fa()
    fm = extract_fm()
    df = transfrom(fa, fm)
    df_gender = transfrom_gender(df)
    df_age = transfrom_age(df)
    df_os = transfrom_os(df)
    load(df_gender, df_age, df_os)

dag_etl = dag_etl()
