# Parquet Generation

Creating a custom API wrapping around queries to different platforms. Our goal is to be able to make requests to this custom API and transform the data we receive into parquet files. We then take these parquet files and ingest them into Databricks.

## To Upload to Databricks: 

* Change target and source file paths as necessary
* Run the `python Ingest_Databricks/upload_to_blob.py` command

---

## To Query Data From Databricks: 

### Method 1 

The first method of querying data we employ is simply grabbing data from a JSON file. We transform this data to follow a pydantic BaseModel for more structured and uniform accesses. We store this data in a cache to reduce storage strains on the FastAPI server. 

Run this method by: 
* Navigating to the Query_JSON folder
* Running the `python server.py` command
* Opening a different terminal and running the `python request.py` command

### Method 2 

The second method of querying data we employ is using databricks. There are two ways we do this, using the Databricks Queries API and using the Databricks SQL connector. We then wrap a FastAPI around these calls so we can make requests to this custom API. 

Run this method by: 
* Navigating to the Query_Databricks folder 
* Either choosing the Queries API or SQL connector method
* Running the `python server.py` command
* Opening a different terminal and running the `python request.py` command

---

Now we have generated parquet files using two different methods. 