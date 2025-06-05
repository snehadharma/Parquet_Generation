import socket
import seqlog
import logging
import os
import json
from azure.identity import ClientSecretCredential

class Config:
    def __init__(self, globalConfigPath):
        self.LoadGlobalSettings(globalConfigPath)
        self.tempPath = "./temp"
        if(os.path.exists(self.tempPath) == False):
            os.mkdir(self.tempPath)
            
    def GetDBCatalogEnv(self):
        catalog_suffix = self.GetSqlWarehouseSettings()["catalogSuffix"]
        env = self.GetSqlWarehouseSettings()["eltv2_environment"]
        # if(env.lower().strip() != ""):
        #     env = f"{env[1:]}"
        dzroot = self.GetSqlWarehouseSettings().get("eltv2_dropzone_root")
        if(dzroot == None):
            raise Exception("Could not find 'eltv2_dropzone_root' in the configuration file. Typically it should be python/databricksload/...")
        return env, catalog_suffix, dzroot

    def GetLagDays(self):
        """
        In SPT environment database is only refreshed once a week. For testing purposes
        we use a number to adjust the last updated date
        """
        return  self.GetSqlWarehouseSettings().get("lagDays") or 0

    def LoadGlobalSettings(self, globalConfigPath):
        """
        Load the global settings that is maintained at a single place to ease maintenance. 
        Examples: connection strings. 
        """
        envName = 'DONTUSEBusSolConnectionStringsFile'
        globalConfig = globalConfigPath
        logger = logging
        if(envName in os.environ.keys()):
            globalConfig = os.environ[envName]
        logger.info(f"Loading configurations from {globalConfig}")
        if os.path.exists(globalConfig) == False:
            raise Exception(f"""
            Could not find expected configuration file. Looked for file {globalConfig}.
            Path to the file can be customized by setting environment variable {globalConfig}
            """)
            
        f = open(globalConfig)
        self.cns = json.load(f)

        seqlog.configure_from_file(self.cns["seqConfigFile"])

    def GetAzureDropZone(self):
        creds = self.cns['oracleExtract']
        account_url = creds["accountUrl"]

        token_credential = ClientSecretCredential(
            tenant_id=creds["credentials"]["tenantId"],
            client_id=creds["credentials"]["clientId"],
            client_secret=creds["credentials"]["clientSecret"]
        )

        return token_credential, account_url

    def GetSqlWarehouseSettings(self):
        return self.cns['dataBricksSqlWareHouseSettings']

    def GetSecret(self, cnname):
        return self.cns['connectionStrings'][cnname]
    
    def get_adf_pipeline_trigger_credentials(self):
        return self.cns['adf_pipeline_trigger']

def GetQuery(relPath:str):
    p = relPath
    #if(self.isRunningInJupyter == True):
    #    p = relPath.replace("./LaborUtilizationStreaming", "./")
    with open(p) as sqlFile:
        sql = sqlFile.read()
    return sql

def Startup(app, globalConfigPath, elt_import_args=None):
    global config
    config = Config(globalConfigPath=globalConfigPath)
    """
    Setup the startup settings for the application.
    """
    envName = "DOTNET_ENVIRONMENT is not set."
    if("DOTNET_ENVIRONMENT" in os.environ.keys()):
        envName = os.environ["DOTNET_ENVIRONMENT"]

    username = os.getlogin()
    props = {
        "MachineName": socket.gethostname(),
        "ProcessId": os.getpid(),
        "ApplicationName": app,
        "envName" : envName
    }
    if(os.path.exists("./Errors") == False):
      os.mkdir("./Errors")

    #seqlog.set_global_log_properties(props)
    seqlog.set_global_log_properties(
        envName = props["envName"],
        ApplicationName = props["ApplicationName"], 
        MachineName = props["MachineName"], 
        ProcessId = props["ProcessId"],
        DataBricksImportParameters = elt_import_args,
        ExecutedByUser = username
        )
    logging.getLogger('azure').setLevel(logging.WARNING)

