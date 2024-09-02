from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.python import PythonOperator, BranchPythonOperator

import pandas as pd
import os
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook



def ingest_transform_data():
    # Access the 'fileloc' parameter from the DAG
    fileloc = dag.params['raw_src']  

    try:
        # Check if the file exists
        if not os.path.exists(fileloc):
            raise FileNotFoundError(f"File not found: {fileloc}")

        # Check if the file is readable
        if not os.access(fileloc, os.R_OK):
            raise PermissionError(f"File is not readable: {fileloc}")

        # Load the CSV file into a DataFrame
        df = pd.read_csv(fileloc)
            
    except FileNotFoundError as fnf_error:
        logging.error(fnf_error)
        return None

    except PermissionError as perm_error:
        logging.error(perm_error)
        return None
    
    except Exception as e:
        logging.error(f"Unexpected error while processing the file: {str(e)}")
        return None
        
    # Perform transformations
    # Filter out rows where price is 0 or negative.
    df = df[df['price'] > 0]
        
    # Convert last_review to a datetime object.
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Handle missing values (if any) last_review dates by filling them with the earliest date
    earliest_date = df['last_review'].min()
    df['last_review'] = df['last_review'].fillna(earliest_date)

    # Handle missing values in reviews_per_month by filling them with 0.
    df['reviews_per_month'] = df['reviews_per_month'].fillna(0)

    # Drop any rows(if any) with missing latitude or longitude values.
    df.dropna(subset=['latitude', 'longitude'], inplace=True)

    # Save the transformed data back to another temporary file or directly to the database
    transformed_file_path = dag.params['transformed_src']
        
    df.to_csv(transformed_file_path, index=False)

def load_into_postgres():
    # obtain variables for processing
    file_path = dag.params['transformed_src']
    conn_id = dag.params['db']['postgres_conn_id']
    table = dag.params['db']['table']

    # Check whether file exists on following path
    if not file_path:
        logging.error('Transformed file is not found')
        return None

    # Create connection to postgres using predifined connection string
    try: 
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = pg_hook.get_conn()
        # Create cursor for inserting data
        cursor = connection.cursor()

        # open transformed file and put data into table
        with open('/opt/airflow/transformed/transformed_AB_NYC_2019.csv') as f:
            cursor.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER DELIMITER ','", f)
            
        #commit operation
        connection.commit()

    # catch if error occured during insert operation
    except Exception as e:
        logging.error(f"Failed to load data into PostgreSQL: {str(e)}")
        return None
        
    finally:
        # close cursor and connection for not wasting space
        cursor.close()
        connection.close() 

def check_record_count():
    conn_id = dag.params['db']['postgres_conn_id']
    table = dag.params['db']['table']

    try:
        # Create connection to postgres using predifined connection string
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = pg_hook.get_conn()
        # Create cursor for enumerating data
        cursor = connection.cursor()

        # count the number of rows loaded into table
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        row_count_table = cursor.fetchone()[0]
        # log it
        logging.info(f'Row count in {table}: {row_count_table}')
            
        # open transformed data
        df = pd.read_csv('/opt/airflow/transformed/transformed_AB_NYC_2019.csv')
        # check its row count
        row_count_csv = len(df)
        logging.info(f'Row count in transformed csv: {row_count_csv}')
        # compare counts value for file and table 
        if row_count_csv != row_count_table:
            #if they don't match log it
            logging.info(f"Row count doesn't match in table {row_count_table} and transformed csv {row_count_csv} ")

    except Exception as e:
        logging.error(f"Row count quality check failed: {str(e)}")
        
    finally:
        # close cursor and connection for not wasting space
        cursor.close()
        connection.close()

def check_null_values():
    # obtain variables for processing
    conn_id = dag.params['db']['postgres_conn_id']
    table = dag.params['db']['table']

    try:
        # Create connection to postgres using predifined connection string
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        connection = pg_hook.get_conn()
        # Create cursor for enumerating data
        cursor = connection.cursor()    

        # Check there are no null values in columns  price, minimum_nights, and availability_365
        columns_to_check = ['price', 'minimum_nights', 'availability_365']
        for column in columns_to_check:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
            null_count = cursor.fetchone()[0]
            # if null values are presented -> log it
            if null_count > 0:
                logging.error(f"Data quality check failed: {null_count} NULL values found in column '{column}'")
                return None
            else:
                logging.info(f"No NULL values found in column '{column}'")

    except Exception as e:
        logging.error(f"Quality check null values failed: {str(e)}")

    finally:
        cursor.close()
        connection.close()

def branch_on_quality_checks(**kwargs):
    ti = kwargs['ti']
    record_count_check = ti.xcom_pull(task_ids='check_record_count')
    null_values_check = ti.xcom_pull(task_ids='check_null_values')

    if record_count_check and null_values_check:
        return 'proceed'
    else:
        return 'log_error_task'

def log_error_task(**kwargs):
    # Log the error to a local file
    with open('/path/to/log_file.txt', 'a') as f:
        f.write("Data quality check failed.\n")

default_args = {
    'owner': 'Edward',
    'retries': 2,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id='nyc_airbnb_etl',
    default_args=default_args,
    description='Pipeline for Data Ingestion, Transformation, and Loading',
    start_date=datetime(2024, 8, 28, 2),
    schedule='@daily',
    params={
        'raw_src': '/opt/airflow/raw/AB_NYC_2019.csv', # Parameter for raw file location,
        'transformed_src': '/opt/airflow/transformed/transformed_AB_NYC_2019.csv', # Parameter for transformed file loc
        'db': {  # Database connection details
            'host': 'localhost',
            'port': 5432,
            'name': 'airflow_etl',
            'user': 'postgres',
            'password': 'postgres',
            'postgres_conn_id': 'postgres_localhost',
            'table': 'airbnb_listings'
        }  
    }
) as dag:
    
    # Create a PythonOperator task to ingest data
    ingest_task = PythonOperator(
        task_id='ingest_transform_data',
        python_callable=ingest_transform_data,
        provide_context=True,  
    )

    # Create a PostgreSql task to load into postgres
    load_task = PythonOperator(
        task_id = 'load_data',
        python_callable=load_into_postgres,
        provide_context=True,
    )

    record_count_task = PythonOperator(
        task_id='record_count_check',
        python_callable=check_record_count,
    ) 
    null_check_task = PythonOperator(
        task_id='check_null_values',
        python_callable=check_null_values,
    )  
    branching = BranchPythonOperator(
        task_id='branch_on_quality_checks',
        python_callable=branch_on_quality_checks,
        provide_context=True,
    )


    ingest_task >> load_task >> record_count_task >> null_check_task
