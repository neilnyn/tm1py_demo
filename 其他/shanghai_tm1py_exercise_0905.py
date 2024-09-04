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

tm1=get_tm1_service('Hellow_World')