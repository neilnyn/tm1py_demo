from TM1py.Services import TM1Service
from collections import defaultdict
import configparser
import argparse
import csv


# get tm1 connection
def get_tm1_service(instance_name):
    config = configparser.ConfigParser()
    config.read(r'config.ini')
    return TM1Service(**config[instance_name])


def get_dimension_hierarchy(tm1, dimension_name):
    hierarchy = defaultdict(set)
    elements = tm1.dimensions.hierarchies.elements.get_elements(dimension_name, dimension_name)
    for element in elements:
        element_name = element.name
        parents = tm1.dimensions.hierarchies.elements.get_parents(dimension_name, dimension_name, element_name)
        for parent in parents:
            hierarchy[parent.name].add(element_name)
    return hierarchy


def comapre_hierarchies(hierarchy1, hierarchy2):
    all_parents_elements = set(hierarchy1.keys()) | set(hierarchy2.keys())
    difference = []
    for parent in all_parents_elements:
        children1 = hierarchy1.get(parent, set())
        children2 = hierarchy2.get(parent, set())
        if children1 != children2:
            difference.append({'parent_element': parent, 'dim1_children': children1, 'dim2_children': children2})
    return difference


def check():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_name', '-f', required=True)
    parser.add_argument('--dimension1', '-d1', required=True)
    parser.add_argument('--dimension2', '-d2', required=True)
    args = parser.parse_args()
    with get_tm1_service('tm1srv01') as tm1:
        hierarchy1 = get_dimension_hierarchy(tm1, args.dimension1)
        hierarchy2 = get_dimension_hierarchy(tm1, args.dimension2)
        difference = comapre_hierarchies(hierarchy1, hierarchy2)
        with open(args.file_name, 'w', newline='') as f:
            writer = csv.writer(f)
            if difference:
                writer.writerow(['found differences between hierarchies'])
                for diff in difference:
                    writer.writerow([f"parent element: {diff['parent_element']}"])
                    writer.writerow([f"{args.dimension1}'s children: {diff['dim1_children']}"])
                    writer.writerow([f"{args.dimension2}'s children: {diff['dim2_children']}"])
            else:
                writer.writerow(['no differences found between hierarchies'])


if __name__ == '__main__':
    check()
