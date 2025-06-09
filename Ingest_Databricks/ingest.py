from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd 
from delta.tables import DeltaTable

def get_parquet_path(self):
    # Get path to hi.txt
    current_file = Path(__file__)
    file_path = current_file.parent.parent / 'Query_Databricks' / 'equipment_list.parquet'
    
    return file_path

def parquet_to_dataframe(self):
    # isn't this just redundant, if i'm going to parquet file, what's the point of unwrapping it bfr it gets to
    # databricks 
    df = pd.read_parquet(get_parquet_path())
    return df

