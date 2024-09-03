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

# CSV file path
csv_file_path = "product_hierarchy.csv"

# Dimension and hierarchy names
dimension_name = "Product"
hierarchy_name = "Product"
# connect to TM1 instance
def get_tm1_service(instance_name):
    config = configparser.ConfigParser()
    config.read(r'config.ini')
    return TM1Service(**config[instance_name])


# build dimensions from csv file by hierarchy method by with open csv method
def read_csv_file(file_path):
    hierarchy_data = []
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            hierarchy_data.append({
                'child': row['child'],
                'parent': row['parent']
            })
    return hierarchy_data


# update or create product hierarchy in TM1 instance
def update_or_create_dimension(tm1, dimension_name,hierarchy_data:list):
    # Check if dimension exists, if not create it
    if not tm1.dimensions.exists(dimension_name):
        dimension = tm1.dimensions.create(dimension_name)

    # Get the hierarchy
    hierarchy = tm1.dimensions.hierarchies.get(dimension_name, dimension_name)

    # hierarchy_data
    hierarchy_data = hierarchy_data

    # Process the hierarchy data
    # set of all elements to add
    elements_to_add = set()
    # dictionary of edges to update
    edges_to_update = {}
    # for each item in the hierarchy data is a dictionary with child and parent
    for item in hierarchy_data:
        child = item['child']
        parent = item['parent']
        # Add elements to the set
        elements_to_add.add(child)
        if parent:
            elements_to_add.add(parent)
        # Update edges
        if parent:
            edges_to_update[parent] = child

    # Add elements if they don't exist
    for element in elements_to_add:
        if not hierarchy.contains_element(element):
            hierarchy.add_element(element, 'Numeric')

    # Unwind the hierarchy before updating
    hierarchy.remove_all_edges()

    # Update edges
    for parent, child in edges_to_update.items():
        hierarchy.add_edge(parent, child, 1)

    # Update the hierarchy
    tm1.dimensions.update(dimension_name)

with get_tm1_service("Hello_World") as tm1:
    hierarchy_data = read_csv_file(csv_file_path)
    update_or_create_dimension(tm1, dimension_name, hierarchy_data)