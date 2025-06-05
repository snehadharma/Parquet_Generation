import pandas as pd
import logging
from Common import appenv
from databricks import sql as dbsql
from Common import dataframeHelper as dfHelper

def GetSqlForEnvironment(sql):
   """
   Expecting that every catalog will have an environment as the suffix. kmt_dev.
   """
   db = appenv.config.GetSqlWarehouseSettings()
   #return sql.replace("_dev.", f"{db['environment']}.")
   return sql.replace("<<_dev>>.", f"_{db['eltv2_environment']}.")

def ExecuteSql(sqlFilePath, params = None):
   """
   Execute a sql command. Like create catalog
   """
   db = appenv.config.GetSqlWarehouseSettings()
   methodParams = {
        "sqlFilePath": sqlFilePath,
        "parameters": params
   }
   logging.debug("Executing sql using {parameters}", parameters=methodParams)
   try:
      connection = dbsql.connect(
                        server_hostname = db["server_hostname"],
                        http_path = db["http_path"],
                        access_token = db["access_token"])
      sql = GetSqlForEnvironment(appenv.GetQuery(sqlFilePath))
      cursor = connection.cursor()
      ret = cursor.execute(sql, parameters=params).fetchall()
      return ret
   except Exception as ex:
        logging.exception(exc_info=ex, msg="Error occurred when getting data using {parameters}", parameters=methodParams)
        raise ex
   finally:
      """ 
      An error happens here. Looks like dataframe is not really populated and needs the cursor to remain open.
      To replicate. Uncomment and run all tests.
      TODO: Need to figure out how best to handle this scenario. Seems like Oracle works differently.
      """
      cursor.close()
      connection.close()

def GetCursor(sql):
   db = appenv.config.GetSqlWarehouseSettings()
   sqlForEnv = GetSqlForEnvironment(sql)
   connection = dbsql.connect(
                     server_hostname = db["server_hostname"],
                     http_path = db["http_path"],
                     access_token = db["access_token"])
   cursor = connection.cursor()
   return cursor.execute(sqlForEnv)

def GetDataFrame(sqlFilePath, params = None):
   db = appenv.config.GetSqlWarehouseSettings()
   methodParams = {
        "sqlFilePath": sqlFilePath,
        "parameters": params
   }
   logging.debug("Getting data using {parameters}", parameters=methodParams)
   try:
      connection = dbsql.connect(
                        server_hostname = db["server_hostname"],
                        http_path = db["http_path"],
                        access_token = db["access_token"])
      sql = GetSqlForEnvironment(appenv.GetQuery(sqlFilePath))
      cursor = connection.cursor()
      df = pd.DataFrame(cursor.execute(sql).fetchall())
      dfHelper.SetColumnNames(df, cursor)
   except Exception as ex:
        logging.exception(exc_info=ex, msg="Error occurred when getting data using {parameters}", parameters=methodParams)
        raise ex
   finally:
      """ 
      An error happens here. Looks like dataframe is not really populated and needs the cursor to remain open.
      To replicate. Uncomment and run all tests.
      TODO: Need to figure out how best to handle this scenario. Seems like Oracle works differently.
      """
      cursor.close()
      connection.close()
   return df