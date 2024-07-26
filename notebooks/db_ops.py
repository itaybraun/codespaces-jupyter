################
## TODO: 
## Create an instance of the class. Set an active connection so the app won't need to open and close it all the time. 



#################################################
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import yaml
from datetime import date, datetime
import json

class DB_Ops:
    def __init__(self):
        self._active_con_str = None  # Initialize the private variable for the connection string

        pass
    @property
    def active_con_str(self):
        return self._active_con_str

    @active_con_str.setter
    def active_con_str(self, connection_string):
        self._active_con_str = connection_string
        
    @staticmethod
    def add(a: int, b: int) -> int:
        return a + b

    @staticmethod
    def execute_sql_query(con_str: str, sql_command: str) -> pd.DataFrame:
        try:
            engine = create_engine(con_str)
            with engine.connect() as connection:
                df = pd.read_sql_query(sql_command, connection)
            return df
        except SQLAlchemyError as e:
            print(f"An error occurred: {str(e)}")
            return pd.DataFrame()
    
    @staticmethod
    def get_sql_for_operation(operation_name, **param_values):
        # Load the configuration
        with open('db_ops_config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        # Find the requested operation
        operation = next((op for op in config['operations'] if op['name'] == operation_name), None)
        
        if not operation:
            raise ValueError(f"Operation '{operation_name}' not found in configuration.")
        
        if operation['type'] != 'data_retrieval':
            raise ValueError(f"Operation '{operation_name}' is not a data retrieval operation.")
        
        sql = operation['sql_command']
        
        # Handle parameter substitution
        for param in operation.get('parameters', []):
            param_name = param['name']
            if param_name not in param_values:
                raise ValueError(f"Required parameter '{param_name}' not provided for operation '{operation_name}'.")
            
            value = param_values[param_name]
            placeholder = f":{param_name}"
            
            # Handle different data types
            if isinstance(value, str):
                sql = sql.replace(placeholder, f"'{value}'")
            elif isinstance(value, (int, float)):
                sql = sql.replace(placeholder, str(value))
            elif isinstance(value, (date, datetime)):
                sql = sql.replace(placeholder, f"'{value.isoformat()}'")
            elif isinstance(value, (dict, list)):
                json_value = json.dumps(value)
                sql = sql.replace(placeholder, f"'{json_value}'")
            else:
                raise ValueError(f"Unsupported parameter type for '{param_name}': {type(value)}")
        
        return sql


    def get_sql_for_operation_2(operation_name, **param_values):
        # Load the configuration
        with open('db_ops_config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        # Find the requested operation
        operation = next((op for op in config['operations'] if op['name'] == operation_name), None)
        
        if not operation:
            raise ValueError(f"Operation '{operation_name}' not found in configuration.")
        
        if operation['type'] != 'data_retrieval':
            raise ValueError(f"Operation '{operation_name}' is not a data retrieval operation.")
        
        sql = operation['sql_command']
        
        # Handle parameter substitution
        for param in operation.get('parameters', []):
            param_name = param['name']
            if param_name not in param_values:
                raise ValueError(f"Required parameter '{param_name}' not provided for operation '{operation_name}'.")
            
            value = param_values[param_name]
            placeholder = f":{param_name}"
            
            # Handle different data types
            if isinstance(value, str):
                sql = sql.replace(placeholder, f"'{value}'")
            elif isinstance(value, (int, float)):
                sql = sql.replace(placeholder, str(value))
            elif isinstance(value, (date, datetime)):
                sql = sql.replace(placeholder, f"'{value.isoformat()}'")
            elif isinstance(value, (dict, list)):
                json_value = json.dumps(value)
                sql = sql.replace(placeholder, f"'{json_value}'")
            else:
                raise ValueError(f"Unsupported parameter type for '{param_name}': {type(value)}")
        
        return sql
