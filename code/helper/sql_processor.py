from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeEngine
from sqlalchemy import inspect, text
import pandas as pd

class CloudSQLDatabase:
    def __init__(self, user, password, host, port, database, big_flag=False):
        self.database_uri = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        self.engine = create_engine(self.database_uri)
        self.Base = declarative_base()
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.tables = {}
        self.big_flag = big_flag

    def create_table(self, table_name, columns):
        if self.table_exists(table_name):
            print(f"Table '{table_name}' already exists.")
            return

        class_attrs = {
            '__tablename__': table_name,
            'id': Column(Integer, primary_key=True, autoincrement=True),
        }

        for column_name, column_type in columns.items():
            if isinstance(column_type, TypeEngine):
                class_attrs[column_name] = Column(column_type)
            else:
                class_attrs[column_name] = Column(self._get_sqlalchemy_type(column_type))

        table_class = type(table_name, (self.Base,), class_attrs)
        self.tables[table_name] = table_class
        self.Base.metadata.create_all(self.engine)
        print(f"Table '{table_name}' created successfully")

    def _get_sqlalchemy_type(self, dtype):
        if self.big_flag:
            dtype_map = {
                'int64': BigInteger,
                'float64': Float,
                'object': String,
                'bool': Boolean,
                'datetime64': DateTime
            }
        else:
            dtype_map = {
                'int64': Integer,
                'float64': Float,
                'object': String,
                'bool': Boolean,
                'datetime64': DateTime
            }
        return dtype_map.get(str(dtype), String)

    def insert_data(self, table_name, data):
        if not self.table_exists(table_name):
            print(f"Table '{table_name}' does not exist.")
            return

        try:
            data.to_sql(table_name, self.engine, if_exists='append', index=False)
            print(f"Data inserted successfully into '{table_name}'")
        except Exception as e:
            print(f"Error while inserting data: {e}")
            self.session.rollback()

    def fetch_data(self, query):
        # if not self.table_exists(table_name):
        #     print(f"Table '{table_name}' does not exist.")
        #     return None

        try:
            query = text(query)
            df = pd.read_sql(query, self.engine)

            return df
        except Exception as e:
            print(f"Error while fetching data: {e}")
            return None

    def close_connection(self):
        self.session.close()
        print("PostgreSQL connection is closed")

    def table_exists(self, table_name):
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
