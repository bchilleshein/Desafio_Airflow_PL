#tarefas.py
from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import csv
import pandas as pd
import base64

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def task1():

  conn = sqlite3.connect("/home/bcarolinah/airflow_tooltorial/data/Northwind_small.sqlite")
  cursor = conn.cursor()
  cursor.execute("""SELECT * FROM "Order";
  """)
  with open("output_orders.csv", 'w', newline='') as csv_file: 
      csv_writer = csv.writer(csv_file)
      csv_writer.writerow([i[0] for i in cursor.description]) 
      csv_writer.writerows(cursor)
  conn.close() 

def task2():

  p1 = pd.read_csv("./output_orders.csv")
  conn = sqlite3.connect("/home/bcarolinah/airflow_tooltorial/data/Northwind_small.sqlite")
  p2 = pd.read_sql_query("select OrderId, Quantity from OrderDetail", conn)
  p3 = pd.merge(p1, p2, how='left', left_on='Id', right_on='OrderId')
  p4 = p3[p3['ShipCity'] == 'Rio de JANEIRO']
  p4['RioId'] = p4.index
  total = p3[p3['ShipCity']=='Rio de Janeiro']['Quantity'].sum()

  total_orders = open("count.txt", 'w')
  total_orders.write(str(total))
  conn.close() 

def task3():
    
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None

with DAG(
    'AtividadePrática',
    default_args=default_args,
    description='Atividade Prática - Módulo V',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """

    task1 = PythonOperator(
        task_id='transform_in_csv',
        python_callable=task1,
        provide_context=True
    )
    task2 = PythonOperator(
        task_id='orders_number',
        python_callable=task2,
        provide_context=True
    )

    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=task3,
        provide_context=True
    )

task1 >> task2 >> export_final_output