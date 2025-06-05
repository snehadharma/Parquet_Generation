# Parquet_Generation

Creating a custom API wrapping around queries to different platforms. Our goal is to be able to make requests to this custom API and transform the data we receive into parquet files. 

## Method 1 

The first method of querying data we employ is simply grabbing data from a JSON file. We transform this data to follow a pydantic BaseModel for more structured and uniform accesses. We store this data in a cache to reduce storage strains on the FastAPI server. 

## Method 2 

The second method of querying data we employ is using databricks. There are two ways we do this, using the Databricks Queries API and using the Databricks SQL connector. We then wrap a FastAPI around these calls so we cna make requests to this custom API. 
