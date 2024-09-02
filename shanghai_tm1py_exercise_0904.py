import logging
import argparse
import csv
import configparser
from TM1py.Services import TM1Service
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename='dimension_update_demo.log', filemode='w')


# logging decorator with parameter which set the context of the log message
def log_context(context):
    def log_decorator(func):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__name__)
            logger.info("Start" + context)
            result = func(*args, **kwargs)
            logger.info("End" + context)
            return result
        return wrapper
    return log_decorator

# connect to TM1 instance
def get_tm1_service(instance_name):
    config = configparser.ConfigParser()
    config.read(r'config.ini')
    return TM1Service(**config[instance_name])
