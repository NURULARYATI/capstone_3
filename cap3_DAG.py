'''
=================================================

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai penjualan ecommerce di Amerika.
=================================================
'''

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 



# Fungsi untuk mengambil data dari PostgreSQL.
def get_data_from_postgresql():
    '''
    Fungsi ini digunakan untuk mengambil data dari PostgreSQL dan menyimpannya ke file CSV.

    Parameters:
    url: string - lokasi PostgreSQL
    database: string - nama database dimana data disimpan
    table: string - nama table dimana data disimpan

    Returns:
    data: list of str - daftar data yang ada di database
    '''
    conn_string = "dbname='airflow' host='postgres' port='5432' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_nurul_data_raw.csv', index=False)




# Fungsi untuk melakukan proses Data Cleaning
def clean_data():
    '''
    Fungsi ini digunakan untuk membersihkan data dari duplikat dan nilai yang hilang.
    
    Baca data dari file CSV, ubah nama kolom menjadi huruf kecil dengan mengganti spasi dengan garis bawah,
    hapus baris duplikat, dan hapus baris dengan nilai yang hilang.
    Terakhir, simpan DataFrame yang telah dibersihkan kembali ke file CSV.

    Parameters:
    input_file_path: string - Lokasi file CSV yang berisi data mentah yang akan dibersihkan.
    output_file_path: string - Lokasi file CSV yang akan berisi data yang telah dibersihkan.

    Returns:
    daftar data yang sudah dibersihkan
    '''
    # Read the CSV file
    df = pd.read_csv('/opt/airflow/dags/P2M3_nurul_data_raw.csv')

    # Convert column names to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(' ', '_', regex=True)

    # Drop duplicate rows
    df = df.drop_duplicates()

    # Drop rows with missing values
    df = df.dropna()

    # Write the cleaned DataFrame back to a CSV file
    df.to_csv('/opt/airflow/dags/P2M3_nurul_data_clean.csv', index=False)


# Fungsi untuk load data ke dalam Elastic Search.
def import_csv_to_elasticsearch():
    '''
    Fungsi ini digunakan untuk mengimpor data dari file CSV ke Elasticsearch.

    Parameters:
    csv_file_path: string - lokasi file CSV yang berisi data yang akan diimpor
    elasticsearch_url: string - URL Elasticsearch, contoh: 'http://elasticsearch:9200'
    index_name: string - nama indeks di Elasticsearch
    doc_type: string - tipe dokumen di Elasticsearch

    Returns:
    dict - Hasil eksekusi indeks Elasticsearch.
    '''
    es = Elasticsearch('http://elasticsearch:9200')
    df = pd.read_csv('/opt/airflow/dags/P2M3_nurul_data_clean.csv')

    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="ecommerce", doc_type="doc", body=doc)
        print(res)



# DAG setup
default_args = {
    'owner': 'nurul',
    'start_date': dt.datetime(2023, 10, 27, 19, 36, 0) - dt.timedelta(hours=7),
    'retries':1,
    'retry_delay': dt.timedelta(minutes=3),
}

with DAG('M3DAG',
         default_args=default_args,
         schedule_interval='30 6 * * *',
        ) as dag:
    
    # Task to fetch data from PostgreSQL
    fetch_task = PythonOperator(
        task_id='get_data_from_postgresql',
        python_callable=get_data_from_postgresql
    )
    
    # Task to clean the data
    clean_task = PythonOperator(
        task_id='clean_dataframe',
        python_callable=clean_data
    )
    
    # Task to post to Kibana
    post_to_kibana_task = PythonOperator(
        task_id='post_to_kibana', 
        python_callable=import_csv_to_elasticsearch
    )
    
# # Set task dependencies
fetch_task >> clean_task >> post_to_kibana_task       


