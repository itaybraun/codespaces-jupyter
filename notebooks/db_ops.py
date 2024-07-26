import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

class DB_Ops:
    def __init__(self):
        pass

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