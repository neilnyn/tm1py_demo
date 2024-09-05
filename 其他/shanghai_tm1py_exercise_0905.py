import logging
import argparse
import csv
import configparser
from TM1py.Services import TM1Service
from TM1py.Objects import Dimension, Hierarchy, Element,Cube,ElementAttribute
from collections import defaultdict
from TM1py.Utils import CaseAndSpaceInsensitiveTuplesDict
import datetime
import json
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename=__name__ + '.log', filemode='w')

#connect to cloud TM1 instance,info
TM1_CLOUD_PARAMS = {
    "base_url": "https://cubewise.planning-analytics.ibmcloud.com/tm1/api/plansamp",
    "user": "cubewise01_tm1_automation",
    "namespace": "LDAP",
    "password": "XI2b5FryLHR679",
    "ssl": True,
    "verify": True,
    "async_requests_mode": True
}
# logging decorator with parameter which set the context of the log message
def log_context(context):
    def log_decorator(func):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__name__)
            logger.info("Start" + context)
            before = datetime.datetime.now()
            result = func(*args, **kwargs)
            after = datetime.datetime.now()
            logger.info("End" + context)
            logger.info(context + "Duration: " + str(after - before))
            return result
        return wrapper
    return log_decorator


# connect to TM1 instance
def get_tm1_service(instance_name):
    config = configparser.ConfigParser()
    config.read(r'config.ini')
    return TM1Service(**config[instance_name])

# read json file
def read_json_file_to_dataframe(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
        df = pd.json_normalize(data)
        return df,data

df_products, dict_data = read_json_file_to_dataframe(r'C:\Users\User\tm1py_demo\其他\products.json')
df_products['Level000'] = 'All Products'
df_products["Level001"] = df["Category.ProductType"]
df_products["Level002"] = df["Category.SubCategory"]
df_products["Level003"] = df["Category.MainCategory"]
df_products['Alias:a']=df_products['ProductName']

# build dimension from dataframe
def build_dimension_from_dataframe(tm1,df,dimension_name):

    tm1.hierarchies.update_or_create_hierarchy_from_dataframe(
        dimension_name=dimension_name,
        hierarchy_name=dimension_name,
        df=df,
        unwind=True)
with get_tm1_service('Hello_World') as tm1:
    dimension_name='Product'
    build_dimension_from_dataframe(tm1,df_products,dimension_name)

with get_tm1_service('Hello_World') as tm1:
    df_sales,dict_data_sales = read_json_file_to_dataframe(r'C:\Users\User\tm1py_demo\其他\sales.json')

    df_sales['Version']='Actual'
    df_sales=df_sales[['Month','Version','Region','Customer','ProductID','SalesChannel','Measure','Value']]

    tm1.cells.write_dataframe(cube_name="Sales", data=df_sales, use_blob=True)

