import os
import io
from sqlalchemy import create_engine
import pandas as pd
import requests
from oauth2client.service_account import ServiceAccountCredentials
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def load_clean_hdfs_to_postgres():
    """
    Đọc dữ liệu clean từ HDFS rồi nạp vào PostgreSQL.
    Luồng:
    HDFS clean files -> /tmp/clean_crypto.csv -> pandas -> PostgreSQL
    """
    hdfs_cat_cmd = (
        "hdfs dfs -fs hdfs://namenode:9000 "
        "-cat /datalake/clean/crypto/* > /tmp/clean_crypto.csv"
    )
    subprocess.run(hdfs_cat_cmd, shell=True, check=True)

    columns = [
        "trade_date",
        "symbol",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "return_pct",
        "volatility",
    ]

    df = pd.read_csv("/tmp/clean_crypto.csv", header=None, names=columns)

    if df.empty:
        print("Không có dữ liệu clean trong HDFS để nạp vào PostgreSQL.")
        return

    df = df.fillna("")
    print(f"Số dòng clean đọc từ HDFS: {len(df)}")

    # Ép kiểu để nạp PostgreSQL ổn định hơn
    numeric_cols = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "return_pct",
        "volatility",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df["symbol"] = df["symbol"].astype(str)

    # Kết nối PostgreSQL
    
    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    )

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS clean_crypto_pg (
        trade_date DATE,
        symbol VARCHAR(50),
        open_price DOUBLE PRECISION,
        high_price DOUBLE PRECISION,
        low_price DOUBLE PRECISION,
        close_price DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        quote_asset_volume DOUBLE PRECISION,
        number_of_trades INTEGER,
        taker_buy_base_volume DOUBLE PRECISION,
        taker_buy_quote_volume DOUBLE PRECISION,
        return_pct DOUBLE PRECISION,
        volatility DOUBLE PRECISION
    );
    """

    truncate_sql = "TRUNCATE TABLE clean_crypto_pg;"

    with engine.begin() as conn:
        conn.exec_driver_sql(create_table_sql)
        conn.exec_driver_sql(truncate_sql)

    df.to_sql(
        "clean_crypto_pg",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )

    print(f"Đã nạp {len(df)} dòng vào PostgreSQL bảng clean_crypto_pg.")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crypto_end_to_end_latest',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['hadoop', 'hive', 'latest']
) as dag:

    upload_raw_to_hdfs = BashOperator(
        task_id='upload_raw_to_hdfs',
        bash_command='''
        hdfs dfs -fs hdfs://namenode:9000 -mkdir -p /datalake/raw/crypto/ && \
        hdfs dfs -fs hdfs://namenode:9000 -put -f /opt/airflow/data/*.csv /datalake/raw/crypto/
        '''
    )

    create_raw_table = SQLExecuteQueryOperator(
        task_id='create_raw_table',
        conn_id='hiveserver2_default',
        sql='''
        CREATE EXTERNAL TABLE IF NOT EXISTS raw_crypto (
            dt_time STRING, symbol STRING, open_price DOUBLE, high_price DOUBLE,
            low_price DOUBLE, close_price DOUBLE, volume DOUBLE, quote_asset_volume DOUBLE,
            number_of_trades INT, taker_buy_base_volume DOUBLE, taker_buy_quote_volume DOUBLE,
            return_pct DOUBLE, volatility DOUBLE
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
        LOCATION '/datalake/raw/crypto/'
        tblproperties ("skip.header.line.count"="1")
        '''
    )

    create_clean_table = SQLExecuteQueryOperator(
        task_id='create_clean_table',
        conn_id='hiveserver2_default',
        sql='''
        CREATE TABLE IF NOT EXISTS clean_crypto (
            trade_date DATE,
            symbol STRING,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            volume DOUBLE,
            quote_asset_volume DOUBLE,
            number_of_trades INT,
            taker_buy_base_volume DOUBLE,
            taker_buy_quote_volume DOUBLE,
            return_pct DOUBLE,
            volatility DOUBLE
        )
        STORED AS PARQUET
        '''
    )

    transform_raw_to_clean_table = SQLExecuteQueryOperator(
        task_id="transform_raw_to_clean_table",
        conn_id="hiveserver2_default",
        sql="""
        INSERT OVERWRITE TABLE clean_crypto
        SELECT
            TO_DATE(CAST(dt_time AS TIMESTAMP)) AS trade_date,
            UPPER(TRIM(symbol)) AS symbol,
            COALESCE(open_price, 0.0),
            COALESCE(high_price, 0.0),
            COALESCE(low_price, 0.0),
            COALESCE(close_price, 0.0),
            COALESCE(volume, 0.0),
            COALESCE(quote_asset_volume, 0.0),
            COALESCE(number_of_trades, 0),
            COALESCE(taker_buy_base_volume, 0.0),
            COALESCE(taker_buy_quote_volume, 0.0),
            COALESCE(return_pct, 0.0),
            COALESCE(volatility, 0.0)
        FROM raw_crypto
        WHERE dt_time IS NOT NULL
          AND symbol IS NOT NULL
          AND symbol != ''
          AND volume >= 0
          AND high_price >= low_price
        """,
    )

    export_clean_to_hdfs = SQLExecuteQueryOperator(
        task_id="export_clean_to_hdfs",
        conn_id="hiveserver2_default",
        sql="""
        INSERT OVERWRITE DIRECTORY '/datalake/clean/crypto/'
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        SELECT
            trade_date,
            symbol,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            quote_asset_volume,
            number_of_trades,
            taker_buy_base_volume,
            taker_buy_quote_volume,
            return_pct,
            volatility
        FROM clean_crypto
        """,
    )

    load_clean_to_postgres = PythonOperator(
        task_id="load_clean_to_postgres",
        python_callable=load_clean_hdfs_to_postgres,
    )

    upload_raw_to_hdfs >> create_raw_table >> create_clean_table >> transform_raw_to_clean_table>> export_clean_to_hdfs >> load_clean_to_postgres