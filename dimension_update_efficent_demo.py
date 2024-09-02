import logging
import argparse
import sys
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


# get tm1 connection
def get_tm1_service(instance_name):
    config = configparser.ConfigParser()
    config.read(r'config.ini')
    return TM1Service(**config[instance_name])
# ETL csv file to TM1 Dimension,csv structure:child,child_desc,parent
def read_csv_file(file_path):
    child_index = 0
    child_desc_index = 1
    parent_index = 2
    parent_map = defaultdict(list) # {p: [c1, c2, c3]}
    element_attributes = {}  # {element: description}
    with open(file_path, 'r',encoding='utf-8') as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            parent = row[parent_index]
            child = row[child_index]
            child_desc = row[child_desc_index]
            parent_map[parent].append(child)
            element_attributes[child] = child_desc
            # Add parent to ensure all elements are included
            if parent not in element_attributes:
                element_attributes[parent] = ""
    return parent_map, element_attributes
# filter out parent_map, return particular parent and its all descendants
def filter_hierarchy(parent_map,element_attributes,root_node):
    def get_descendants(node):
        descendants = set()
        to_process = [node]
        while to_process:
            current = to_process.pop()
            for child in parent_map.get(current, []):
                if child not in descendants:
                    descendants.add(child)
                    to_process.append(child)
        return descendants
    descendants = get_descendants(root_node)
    descendants.add(root_node)

    filtered_pararent_map = defaultdict(list)
    filtered_attributes_map = {}
    for parent, children in parent_map.items():
        if parent in descendants:
            filtered_pararent_map[parent] = children
            filtered_attributes_map[parent] = element_attributes.get(parent, "")
            for child in children:
                filtered_attributes_map[child] = element_attributes.get(child, "")
    # transfer filtered_parent_map to tuple list
    filtered_parent_map_edges = [(parent, child)for parent, children in filtered_pararent_map.items() for child in children]

    return filtered_pararent_map, filtered_parent_map_edges,filtered_attributes_map
# unwind before update
def unwind_consolidated_element(tm1,dimension_name,element:str,edges,hierarchy_target:object):
    element_obj = tm1.elements.get(dimension_name,dimension_name,element)
    # only unwind if element is consolidated
    if element_obj.element_type.name == 'CONSOLIDATED':
        # get all descendants of element
        descendants_edges = hierarchy_target.get_descendants_edges(element,recursive=True)
        descendants_edges_list = [key for key in descendants_edges]
        descendants_edges_set = set(descendants_edges_list)
        # unwind element
        csv_edges_set = set(edges)
        edges_need_unwind = descendants_edges_set - csv_edges_set
        # only unwind if there are edges need to unwind
        for edge in edges_need_unwind:
            hierarchy_target.remove_edge(edge[0],edge[1])

    return descendants_edges
# update dimension
def update_hierarchy_from_edges_with_attr(tm1,dimension_name,parent_map,edges,element_attributes,consolidated_element:str):
    # get target hierarchy
    hierarchy_target = tm1.dimensions.hierarchies.get(dimension_name,dimension_name)
    # unwind
    descendants_edges = unwind_consolidated_element(tm1,dimension_name,consolidated_element,hierarchy_target)
    descendants_edges_list = [key for key in descendants_edges]
    descendants_edges_set = set(descendants_edges_list)
    # edges from csv file to set
    csv_edges_set = set(edges)
    # edges need to update
    edges_need_update = csv_edges_set - descendants_edges_set
    # update edges
    # descendants_edges to set of all elements
    def prepare_edges_set(edges):
        all_elements = set()
        for parent, children in parent_map.items():
            all_elements.add(parent)
            for child in children:
                all_elements.add(child)
        return all_elements
    descendants_set = prepare_edges_set(descendants_edges)
    # add elements to hierarchy if not exist
    for parent, children in parent_map.items():
        if parent =='':
            continue
        if not parent in descendants_set:
            if not tm1.elements.exists(dimension_name,dimension_name,parent):
                hierarchy_target.add_element(parent,'Numeric')
        for child in children:
            if not child in descendants_set:
                if not tm1.elements.exists(dimension_name, dimension_name, child):
                    hierarchy_target.add_element(child,'Numeric')
    for edge in edges_need_update:
        hierarchy_target.add_edge(edge[0],edge[1],1)
    # add attributes
    attrs_list = [{'desc':'String'},{'parent':'String'}]
    for attr in attrs_list:
        attr_desc = list(attr.items())[0][0]
        attr_type = list(attr.items())[0][1]
        if not tm1.elements.exists('}ElementAttributes_'+ dimension_name,'}ElementAttributes_'+ dimension_name,attr_desc):
            hierarchy_target.add_element_attribute(attr_desc,attr_type)
    #update hierarchy
    tm1.dimensions.hierarchies.update(hierarchy_target)

    # update attributes value
    cellset = {}
    for attr_name in ['desc']:
        for element, attr_value in element_attributes.items():
            cellset[(element, attr_name)] = attr_value
    for attr_name in ['parent']:
        for parent, children in parent_map.items():
            if parent =='':
                continue
            for child in children:
                cellset[(child, attr_name)] = parent
    tm1.cells.write_through_cellset(cube_name='}ElementAttributes_'+dimension_name, cellset_as_dict=cellset)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path','-f',required=True)
    parser.add_argument('--root_node','-r',required=True)
    parser.add_argument('--dimension_name','-n',required=True)
    args = parser.parse_args()
    # read csv file
    parent_map, element_attributes = read_csv_file(args.file_path)
    # filter hierarchy
    filtered_parent_map, edges,filtered_element_attributes = filter_hierarchy(parent_map,element_attributes,args.root_node)
    # update dimension
    with get_tm1_service('tm1srv01') as tm1:
        try:
            update_hierarchy_from_edges_with_attr(tm1,args.dimension_name,filtered_parent_map,edges,filtered_element_attributes,args.root_node)

        except Exception as e:
            raise e

