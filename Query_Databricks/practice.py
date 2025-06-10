import os
from dotenv import load_dotenv
import sys
from datetime import datetime


def read_sql_query(filename: str) -> str:
    """Read SQL query from a file"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(current_dir, 'SQL_Commands', filename)
    
    with open(sql_path, 'r') as sql_file:
        return sql_file.read()

# Usage in your code:
# query = read_sql_query('get_all_equipment.sql')
# print(query)

def get_env():
    print(os.getenv('DATABRICKS_TOKEN'))


get_env()
