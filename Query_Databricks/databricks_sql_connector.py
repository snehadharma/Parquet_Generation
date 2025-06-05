from databricks import sql
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Get credentials from .env file
server_hostname = os.getenv('DATABRICKS_HOST')  # Your Databricks workspace URL
http_path = os.getenv('DATABRICKS_HTTP_PATH')  # SQL warehouse HTTP path
access_token = os.getenv('DATABRICKS_TOKEN')  # Your personal access token

# Print execution info
print(f"Execution Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"User: {os.getenv('USER', 'snehadharma')}")

try:
    # Establish connection
    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    )
    
    print("Successfully connected to Databricks!")
    
    # Create a cursor
    cursor = connection.cursor()
    
    # Execute query
    query = "SELECT * FROM kmt_sneha_dev.stores.equipment_sample_data"
    cursor.execute(query)
    
    # Fetch and print results
    result = cursor.fetchall()
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Print column headers
    print("\nColumns:")
    print(", ".join(columns))
    
    # Print results
    print("\nResults:")
    for row in result:
        print(row)

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close cursor and connection
    try:
        cursor.close()
    except:
        pass
    
    try:
        connection.close()
    except:
        pass
    print("\nConnection closed.")