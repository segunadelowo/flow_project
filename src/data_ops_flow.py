import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

#from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.task_runners import SequentialTaskRunner

from data_processing import test_call

@task(log_prints=True) #, tags=["extract"] cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
def extract_data(url: str):
    
    df_data = pd.DataFrame()
    
    return df_data


@task(log_prints=True)
def transform_data(df):
    
    return df


@task(log_prints=True, retries=3)
def load_data(df):
    test_call()


@flow(name="Data OPS",task_runner=SequentialTaskRunner())
def main_flow():

    csv_url = "file.com"
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    load_data(data)


if __name__ == '__main__':
    main_flow()