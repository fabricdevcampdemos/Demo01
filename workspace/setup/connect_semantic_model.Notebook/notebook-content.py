# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7a72393c-650e-457a-bb38-af66775a092b",
# META       "default_lakehouse_name": "setup_logs",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "7a72393c-650e-457a-bb38-af66775a092b",
# META           "workspace_id": "00000000-0000-0000-0000-000000000000"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


import logging
import requests
import os
from datetime import datetime
import time
import sys
import msal


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


time.sleep(60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def setup_logging():
    """ Setup logging """
    log_dir = "/lakehouse/default/Files/logs/"
    log_filename = f"notebook_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = os.path.join(log_dir, log_filename)
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        force=True,
        handlers=[
          logging.FileHandler(log_path),      # Outputs to file
          logging.StreamHandler(sys.stdout)   # Outputs to console
        ]
    )

setup_logging()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_current_context():
    userId = notebookutils.runtime.context.get("userId")
    logging.info(f'userId: {userId}')

    currentWorkspaceName = notebookutils.runtime.context.get("currentWorkspaceName")
    currentWorkspaceId = notebookutils.runtime.context.get("currentWorkspaceId")

    logging.info(f'currentWorkspaceName: {currentWorkspaceName}')
    logging.info(f'currentWorkspaceId: {currentWorkspaceId}')

log_current_context()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class TokenManager: 
    CURRENT_USER_ID = notebookutils.runtime.context.get("userId") 
    SPN_USER_ID = 'c7b17a8e-3113-4f27-8b8c-9dc2439bd0a3' 
    
    # use single MSAL app instance across calls for in-memory SPN tokencaching
    _MSAL_APP = None 

    @classmethod 
    def get_fabric_access_token(cls): 
        if cls.CURRENT_USER_ID == cls.SPN_USER_ID: 
            return cls._get_fabric_access_token_for_service_principal() 
        else: 
            return notebookutils.credentials.getToken("pbi") 

    @classmethod 
    def _get_fabric_access_token_for_service_principal(cls): 
        spn_scopes = ['https://api.fabric.microsoft.com/.default'] 
        
        # 1. Initialize the MSAL app instance only once to preserve the cache across executions
        if cls._MSAL_APP is None:
            logging.info('Initializing MSAL ConfidentialClientApplication...')
            vault_url = "https://fabricdevopssecrets.vault.azure.net/" 
            tenant_id = notebookutils.credentials.getSecret(vault_url, 'tenant-id') 
            client_id = notebookutils.credentials.getSecret(vault_url, 'client-id') 
            client_secret = notebookutils.credentials.getSecret(vault_url, 'client-secret') 
            
            cls._MSAL_APP = msal.ConfidentialClientApplication( 
                client_id, 
                authority = f'https://login.microsoftonline.com/{tenant_id}', 
                client_credential = client_secret 
            ) 
        
        authentication_result = cls._MSAL_APP.acquire_token_for_client(
          scopes=spn_scopes
        )

        if "access_token" not in authentication_result:
            logging.error(
              f"Token acquisition failed: {authentication_result.get('error')} - "
              f"{authentication_result.get('error_description')}"
            )
            raise RuntimeError(
                f"Token acquisition failed: {authentication_result.get('error')} - "
                f"{authentication_result.get('error_description')}"
            )
        
        return authentication_result["access_token"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class FabricRestApi:
    """Wrapper class for Fabric REST API"""

    FABRIC_REST_API_BASE_URL = 'https://api.fabric.microsoft.com/v1/'

    CURRENT_WORKSPACE_NAME = notebookutils.runtime.context.get("currentWorkspaceName")
    CURRENT_WORKSPACE_ID = notebookutils.runtime.context.get("currentWorkspaceId")
    CURRENT_NOTEBOOK_ID = notebookutils.runtime.context.get("currentNotebookId")
    DEFAULT_LAKEHOUSE_NAME = notebookutils.runtime.context.get("defaultLakehouseName")
    DEFAULT_LAKEHOUSE_ID = notebookutils.runtime.context.get("defaultLakehouseId")

    WORKSPACE_ADMIN_GROUP = "e4ff16d1-b3b1-4c33-b091-ff9a4b4172d6"
 
    @classmethod
    def _get_fabric_access_token(cls):
        return TokenManager.get_fabric_access_token()

    @classmethod
    def _execute_get_request(cls, endpoint):
        """Execute GET Request on Fabric REST API Endpoint"""
        rest_url = cls.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                           'Authorization': f'Bearer {access_token}'}
        response = requests.get(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code == 200:
            return response.json()
        if response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            logging.info(f'Waiting {wait_time} seconds due to 429 TOO MANY REQUESTS error')
            time.sleep(wait_time)
            return cls._execute_get_request(endpoint)
        else:
            logging.error(f'Error executing GET request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request(cls, endpoint, post_body=''):
        """Execute POST request with support for Long-running Operations (LRO)"""
        rest_url = cls.FABRIC_REST_API_BASE_URL + endpoint
        access_token = cls._get_fabric_access_token()
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}

        response = requests.post(url=rest_url, json=post_body, headers=request_headers, timeout=60)

        if response.status_code in { 200, 201, 204 }:
            try:
                return response.json()
            except JSONDecodeError:
                return None

        if response.status_code == 202:
            operation_state_url = response.headers.get('Location')
            wait_time = 10 # int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            response = requests.get(url=operation_state_url, headers=request_headers, timeout=60)
            operation_state = response.json()
            while operation_state['status'] != 'Succeeded' and \
                    operation_state['status'] != 'Failed':
                time.sleep(wait_time)
                response = requests.get(url=operation_state_url,
                                        headers=request_headers,
                                        timeout=60)
                operation_state = response.json()

            if operation_state['status'] == 'Succeeded':
                if 'Location' in response.headers:
                    operation_result_url = response.headers.get('Location')
                    response = requests.get(url=operation_result_url,
                                            headers=request_headers,
                                            timeout=60)
                    if response.status_code == 200:
                        return response.json()
                    else:
                        AppLogger.log_error(f"Error - {response.status_code}")
                        return None
                else:
                    return None
            else:
                logging.error(f"Error - {operation_state}")

        elif response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            time.sleep(wait_time)
            return cls._execute_post_request(endpoint, post_body)
     
        elif response.status_code == 404: # handle NOT FOUND error
            logging.info('Received 404 NOT FOUND error - waiting 10 seconds and retrying')
            wait_time = 10
            time.sleep(wait_time)
            return cls._execute_post_request(endpoint, post_body)
        
        else:
            logging.error(
                f'Error executing POST request: {response.status_code} - {response.text}')
            raise RuntimeError(f'Error executing POST request: {response.status_code} - {response.text}')

    @classmethod
    def list_workspaces(cls):
        """ List Workpaces """
        return cls._execute_get_request('workspaces')

    @classmethod
    def get_workspace_info(cls):
        """Get Workspace information by ID"""
        return cls._execute_get_request(f'workspaces/{cls.CURRENT_WORKSPACE_ID}')

    @classmethod
    def add_workspace_user(cls, user_id, role_assignment):
        """Add workspace role for user"""
        endpoint =    f'workspaces/{cls.CURRENT_WORKSPACE_ID}/roleAssignments'
        post_body = {
            'role': role_assignment,
            'principal': {
            'id': user_id,
            'type': 'User'
            }
        }
        return cls._execute_post_request(endpoint, post_body)

    @classmethod
    def add_workspace_group(cls, group_id, role_assignment):
        """Add workspace role for group"""
        endpoint =    f'workspaces/{cls.CURRENT_WORKSPACE_ID}/roleAssignments'
        post_body = {
            'role': role_assignment,
            'principal': {
            'id': group_id,
            'type': 'Group'
            }
        }
        return cls._execute_post_request(endpoint, post_body)

    @classmethod
    def add_workspace_spn(cls, spn_id, role_assignment):
        """Add workspace role for user"""
        endpoint =    f'workspaces/{cls.CURRENT_WORKSPACE_ID}/roleAssignments'
        post_body = {
            'role': role_assignment,
            'principal': {
            'id': spn_id,
            'type': 'ServicePrincipal'
            }
        }
        return cls._execute_post_request(endpoint, post_body)

    @classmethod
    def list_workspace_items(cls, item_type = None):
        """Get items in workspace"""
        endpoint = f'workspaces/{cls.CURRENT_WORKSPACE_ID}/items'

        if item_type is not None:
            endpoint = endpoint + f'?type={item_type}'

        return cls._execute_get_request(endpoint)['value']

    @classmethod
    def get_item_by_name(cls, display_name, item_type):
        """Get Item by Name"""
        items = cls.list_workspace_items(item_type)
        for item in items:
            if item['displayName'] == display_name:
                return item
        return None

    @classmethod
    def provision_workspace_identity(cls, workspace_role = 'Admin'):
        """Provision Workspace Identity"""
        if cls.workspace_has_provisioned_identity():
            logging.info('Workspace identity has already been provisioned')
            return

        logging.info('Provisioning workspace identity')
        rest_url = f"workspaces/{cls.CURRENT_WORKSPACE_ID}/provisionIdentity"
        workspace_identity = cls._execute_post_request(rest_url)
        service_principal_id = workspace_identity['servicePrincipalId']
        logging.info(f'Workspace identity provisioned with service principal id [{service_principal_id}]')
        cls.add_workspace_spn(service_principal_id, workspace_role)
        logging.info(f'Workspace identity added to workspace role of [{workspace_role}]')

    @classmethod
    def workspace_has_provisioned_identity(cls):
        """Check if Workspace has a provisioned identity"""
        workspace_info = cls.get_workspace_info()
        return 'workspaceIdentity' in workspace_info

    @classmethod
    def deprovision_workspace_identity(cls):
        """Deprovision Workspace Identity"""
        if not cls.workspace_has_provisioned_identity():
            logging.info('Workspace identity is not currently provisioned')
            return
        logging.info('Deprovisioning workspace identity')
        rest_url = f"workspaces/{cls.CURRENT_WORKSPACE_ID}/deprovisionIdentity"
        cls._execute_post_request(rest_url)

    @classmethod
    def list_connections(cls):
        """List all connections accessible to caller"""
        return cls._execute_get_request("connections")['value']

    @classmethod
    def get_connection_by_name(cls, display_name):
        """Get Connection By Name"""
        connections = cls.list_connections()
        for connection in connections:
            if connection['displayName'] == display_name:
                return connection
        return None

    @classmethod
    def create_connection(cls, create_connection_request):
        """ Create new connection"""
        logging.info(f"Creating connection with display name [{create_connection_request['displayName']}]")

        existing_connections = cls.list_connections()
        for connection in existing_connections:
            if 'displayName' in connection and \
                connection['displayName'] == create_connection_request['displayName']:
                logging.info(f"Using existing Connection with id [{connection['id']}]")
                return connection

        connection = cls._execute_post_request('connections', create_connection_request)

        logging.info(f"Connection created with id [{connection['id']}]")

        cls.add_connection_role_assignment_for_group(
          connection['id'],
          cls.WORKSPACE_ADMIN_GROUP,
          'Owner')

        return connection

    @classmethod
    def add_connection_role_assignment_for_user(cls, connection_id, admin_user_id, connection_role):
        """Add connection role assignment for user"""
        rest_url = f'connections//{connection_id}//roleAssignments'
        post_body = {
            'principal': {
                'id': admin_user_id,
                'type': "User"
            },
            'role': connection_role
        }
        return cls._execute_post_request(rest_url, post_body)

    @classmethod
    def add_connection_role_assignment_for_group(cls, connection_id, group_id, connection_role):
        """Add connection role assignment for group"""
        rest_url = f'connections//{connection_id}//roleAssignments'
        post_body = {
            'principal': {
                'id': group_id,
                'type': "Group"
            },
            'role': connection_role
        }
        return cls._execute_post_request(rest_url, post_body)

    @classmethod
    def add_connection_role_assignment_for_service_principal(cls, connection_id, spn_object_id, connection_role):
        """Add connection role assignment for service principal"""
        rest_url = f'connections//{connection_id}//roleAssignments'
        post_body = {
            'principal': {
                'id': spn_object_id,
                'type': "ServicePrincipal"
            },
            'role': connection_role
        }
        return cls._execute_post_request(rest_url, post_body)

    @classmethod
    def create_azure_storage_connection_with_sas_token(cls, server, path):
        """Create Azure Storage connections"""
        
        display_name = f"ADLS-SAS-[{server}{path}]"
     
        vault_url = "https://fabricdevopssecrets.vault.azure.net/"
        secret_name = "azure-storage-sas-token"
        sas_token = notebookutils.credentials.getSecret(vault_url, secret_name)
    
        create_connection_request = {
            'displayName': display_name,
            'connectivityType': 'ShareableCloud',
            'privacyLevel': 'Organizational',
            'connectionDetails': {
                'type': 'AzureDataLakeStorage',
                'creationMethod': 'AzureDataLakeStorage',
                'parameters': [ 
                    { 'value': server, 'dataType': 'Text', 'name': 'server' },
                    { 'value': path, 'dataType': 'Text', 'name': 'path' }
                ]
            },
            'credentialDetails': {
                'credentials': {
                    'token': sas_token,
                    'credentialType': 'SharedAccessSignature'
                },
                'singleSignOnType': 'None',
                'connectionEncryption': 'NotEncrypted',
                'skipTestConnection': 'false'
            }
        }

        return cls.create_connection(create_connection_request)
  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


class PowerBiRestApi:
    """Power BI REST API Wrapper Class used for operations not yet supported by Fabric REST API"""

    CURRENT_WORKSPACE_NAME = notebookutils.runtime.context.get("currentWorkspaceName")
    CURRENT_WORKSPACE_ID = notebookutils.runtime.context.get("currentWorkspaceId")
    CURRENT_NOTEBOOK_ID = notebookutils.runtime.context.get("currentNotebookId")
    DEFAULT_LAKEHOUSE_NAME = notebookutils.runtime.context.get("defaultLakehouseName")
    DEFAULT_LAKEHOUSE_ID = notebookutils.runtime.context.get("defaultLakehouseId")

    POWER_BI_RSET_API_ROOT_URL = 'https://api.powerbi.com/v1.0/myorg/'
    
    @classmethod
    def _get_access_token(cls):
        return TokenManager.get_fabric_access_token()

    @classmethod
    def _execute_get_request_to_powerbi(cls, endpoint):
        """Execute GET Request on Power BI REST API Endpoint"""
        rest_url = cls.POWER_BI_RSET_API_ROOT_URL + endpoint
        access_token = cls._get_access_token()
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        response = requests.get(url=rest_url, headers=request_headers, timeout=60)
        if response.status_code in { 200, 202 }:
            return response.json()
        elif response.status_code == 429: # handle TOO MANY REQUESTS error
            wait_time = int(response.headers.get('Retry-After'))
            logging.info(f'Waiting {wait_time} seconds due to 429 TOO MANY REQUESTS error')
            time.sleep(wait_time)
            return cls._execute_get_request_to_powerbi(endpoint)
        else:
            logging.error(
                f'Error executing GET request: {response.status_code} - {response.text}')
            return None

    @classmethod
    def _execute_post_request_to_powerbi(cls, endpoint, post_body=''):
        rest_url = cls.POWER_BI_RSET_API_ROOT_URL + endpoint
        access_token = cls._get_access_token()
        request_headers = {'Content-Type':'application/json',
                             'Authorization': f'Bearer {access_token}'}
        response = requests.post(url=rest_url, headers=request_headers, json=post_body, timeout=60)
        return response

    @classmethod
    def list_datasources_for_semantic_model(cls, semantic_model_id):
        """Get Datasource for semantic model using Power BI REST API"""     
        rest_url    = f'groups//{cls.CURRENT_WORKSPACE_ID}//datasets//{semantic_model_id}//datasources'   
        logging.info(f'Getting datasources for semantic model {semantic_model_id}')
        return cls._execute_get_request_to_powerbi(rest_url)['value']

    @classmethod
    def get_adls_dataource_from_semantic_model(cls, semantic_model_id):
        """Get ADLS datasource from semantic model"""
        logging.info(f'Calling list_datasources_for_semantic_model for semantic model {semantic_model_id}')
        data_sources = cls.list_datasources_for_semantic_model(semantic_model_id)
        for datasource in data_sources:                        
            if datasource['datasourceType'] == 'AzureDataLakeStorage':
                server    = f"https://{datasource['connectionDetails']['server']}"
                path      = datasource['connectionDetails']['path']
                return {
                  'server': server,
                  'path': path
                }

    @classmethod
    def bind_semantic_model_to_connection(cls, semantic_model_id, connection_id):
        """Bind semantic model to connection"""
        rest_url    = f'groups//{cls.CURRENT_WORKSPACE_ID}//datasets//{semantic_model_id}//Default.BindToGateway'
        post_body    = {
            'gatewayObjectId': '27cad5cb-c945-4e8e-a7e2-7b9acd87af29',
            'datasourceObjectIds': [ connection_id ]
        }
        cls._execute_post_request_to_powerbi(rest_url, post_body)

    @classmethod
    def refresh_semantic_model(cls, semantic_model_id):
        """Refresh semantic model"""
        logging.info('Refreshing semantic model...')
        rest_url    = f'groups//{cls.CURRENT_WORKSPACE_ID}//datasets//{semantic_model_id}//refreshes'
        post_body = { 'notifyOption': 'NoNotification', 'type': 'Automatic' }
        response = cls._execute_post_request_to_powerbi(rest_url, post_body)
        refresh_id = response.headers.get('x-ms-request-id')
        rest_url_refresh_details = \
            f'groups/{cls.CURRENT_WORKSPACE_ID}/datasets/{semantic_model_id}/refreshes/{refresh_id}'
        refresh_details = cls._execute_get_request_to_powerbi(rest_url_refresh_details)

        while 'status' not in refresh_details or refresh_details == 'Unknown':
            time.sleep(6)
            refresh_details = cls._execute_get_request_to_powerbi(rest_url_refresh_details)

        logging.info("Refresh operation complete")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logging.info('Getting metadata for semantic model')
model = FabricRestApi.get_item_by_name('Product Sales Imported Model', 'SemanticModel')
model_id = model['id']

logging.info('Getting Azure datasource from semantic definition')
datasource = PowerBiRestApi.get_adls_dataource_from_semantic_model(model_id)
server = datasource['server']
path = datasource['path']

logging.info('Creating ADLS connection')
connection = FabricRestApi.create_azure_storage_connection_with_sas_token(server, path)
connection_id = connection['id']

logging.info('Binding semantic model to ADLS connection')
PowerBiRestApi.bind_semantic_model_to_connection(model_id, connection_id)

logging.info('Refreshing imported data for semantic model')
PowerBiRestApi.refresh_semantic_model(model_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logging.shutdown()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
