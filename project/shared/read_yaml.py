#Functions that reads yaml file and returns array of yaml file path
import yaml
import sys

def read_yaml(path):
    try:
        with open(path,"r") as file:
            yaml_data = yaml.safe_load(file)
            return yaml_data
    except Exception as e:
        print(f"Error while reading yaml file: {e}")
        sys.exit(1)
