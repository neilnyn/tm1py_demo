import logging
import argparse
import csv
import configparser
from TM1py.Services import TM1Service
from TM1py.Objects import Dimension, Hierarchy, Element,Cube
from collections import defaultdict
from TM1py.Utils import CaseAndSpaceInsensitiveTuplesDict
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename=__file__ + '.log', filemode='w')


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

# generate year hierarchy data
def year_hierarchy_data():
    hierarchy_data = []
    for year in range(2010, 2051):
        hierarchy_data.append({'parent':'All Years', 'child':str(year)})
    return hierarchy_data
# gernerate month hierarchy data
def month_hierarchy_data():
    hierarchy_data = []
    for month in range(1, 13):
        hierarchy_data.append({'parent':'All Months', 'child':'M%02d' % month})
    return hierarchy_data

# generate day hierarchy data
def day_hierarchy_data():
    hierarchy_data = []
    for day in range(1, 32):
        hierarchy_data.append({'parent':'All Days', 'child':'D%02d' % day})
    return hierarchy_data
# generate measure dimension data
def measure_dimension_data():
    return [{'parent':'','child':'Quantity'},{'parent':'','child':'Amount'}]
# update or create product hierarchy in TM1 instance
@log_context("Update or create dimension using hierarchy API")
def update_or_create_dimension_use_hierarchy_api(tm1, dimension_name,hierarchy_data:list):
    # Check if dimension exists, if not create it
    if not tm1.dimensions.exists(dimension_name):
        new_dimension = Dimension(dimension_name)
        new_hierarchy = Hierarchy(dimension_name, dimension_name)
        new_dimension.add_hierarchy(new_hierarchy)
        tm1.dimensions.create(new_dimension)

    # Get the hierarchy
    hierarchy = tm1.dimensions.hierarchies.get(dimension_name, dimension_name)

    # hierarchy_data
    hierarchy_data = hierarchy_data

    # Process the hierarchy data
    # set of all elements to add
    elements_to_add = set()
    # dictionary of edges to update
    edges_to_update = defaultdict(list)
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
            edges_to_update[parent].append(child)

    # Add elements if they don't exist
    for element in elements_to_add:
        if not hierarchy.contains_element(element):
            hierarchy.add_element(element, 'Numeric')

    # Unwind the hierarchy before updating
    hierarchy.remove_all_edges()

    # Update edges
    for parent, children in edges_to_update.items():
        for child in children:
            hierarchy.add_edge(parent, child, 1)

    # Update the hierarchy
    tm1.dimensions.hierarchies.update(hierarchy)




# update or create product hierarchy in TM1 instance
@log_context("Update or create dimension using element API")
def update_or_create_dimension_use_element_api(tm1, dimension_name, hierarchy_data: list):
    # Check if dimension exists, if not create it
    if not tm1.dimensions.exists(dimension_name):
        new_dimension = Dimension(dimension_name)
        new_hierarchy = Hierarchy(dimension_name, dimension_name)
        new_dimension.add_hierarchy(new_hierarchy)
        tm1.dimensions.create(new_dimension)

    # Get the hierarchy
    all_elements = tm1.elements.get_element_names(dimension_name, dimension_name)

    # Process the hierarchy data
    elements_to_add = set()
    edges_to_update = defaultdict(list)

    for item in hierarchy_data:
        child = item['child']
        parent = item['parent']

        elements_to_add.add(child)
        if parent:
            elements_to_add.add(parent)
            edges_to_update[parent].append(child)

    # Add or update elements
    for element in elements_to_add:
        if element not in all_elements:
            # If element doesn't exist, create it
            tm1.elements.create(dimension_name, dimension_name, element, 'Numeric')

    # Remove all existing edges
    current_edges = tm1.elements.get_edges(dimension_name, dimension_name)
    for edge in current_edges:
        tm1.elements.remove_edge(dimension_name, dimension_name, edge[0], edge[1])

    # Add new edges
    new_edges = CaseAndSpaceInsensitiveTuplesDict()
    for parent, children in edges_to_update.items():
        for child in children:
            new_edges[(parent, child)] = 1
    tm1.elements.add_edges(dimension_name, dimension_name,new_edges)


# build cube if not exist
def buid_cube(tm1, cube_name, dimensions:list):
    if not tm1.cubes.exists(cube_name):
        new_cube = Cube(name=cube_name, dimensions=dimensions)
        tm1.cubes.create(new_cube)

# Main execution
if __name__ == "__main__":
    with get_tm1_service("Hello_World") as tm1:
        #update product hierarchy using element API and hierarchy API
        # CSV file path
        csv_file_path = "product-hierarchy-csv.csv"
        # Dimension names
        dimension_name = "Product"
        hierarchy_data = read_csv_file(csv_file_path)
        update_or_create_dimension_use_element_api(tm1, dimension_name, hierarchy_data)
        update_or_create_dimension_use_hierarchy_api(tm1, dimension_name, hierarchy_data)
        #update Year
        hier_data = year_hierarchy_data()
        update_or_create_dimension_use_hierarchy_api(tm1, "Year", hier_data)
        #update Month
        hier_data = month_hierarchy_data()
        update_or_create_dimension_use_hierarchy_api(tm1, "Month", hier_data)
        #update Day
        hier_data = day_hierarchy_data()
        update_or_create_dimension_use_hierarchy_api(tm1, "Day", hier_data)
        #update Measure
        measure_data = measure_dimension_data()
        update_or_create_dimension_use_hierarchy_api(tm1, "Measure", measure_data)
        # Build cube
        dimensions = ["Product", "Year", "Month", "Day","Measure"]
        cube_name = "Product_Sales"
        buid_cube(tm1, cube_name, dimensions)

