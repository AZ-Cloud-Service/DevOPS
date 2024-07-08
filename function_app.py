import azure.functions as func
import logging
import random
import json
import string
from datetime import datetime
import mysql.connector
import time
import os
from azure.identity import DefaultAzureCredential
from azure.mgmt.rdbms.mysql_flexibleservers import MySQLManagementClient
from azure.mgmt.rdbms.mysql_flexibleservers.models import Server,CreateMode
from azure.mgmt.monitor import MonitorManagementClient

subscription_id = os.getenv('SUBSCRIPTION_ID')
resource_group_name = os.getenv('RESOURCE_GROUP_NAME')
source_server_name = os.getenv('SOURCE_SERVER_NAME')
replicaPrefix = os.getenv('REPLICA_PREFIX')
location = os.getenv('LOCATION')
lowerbound = float(os.getenv('LOWERBOUND'))

db_lb_host = os.getenv('DB_LB_HOST')
db_read_hostgroup_id = os.getenv('DB_READ_HOSTGROUP_ID')
db_dns = os.getenv('DB_DNS')

proxysql_db_admin_port = os.getenv('PROXYSQL_DB_ADMIN_PORT')
proxysql_db_name = os.getenv('PROXYSQL_DB_NAME')
proxysql_db_user = os.getenv('PROXYSQL_DB_USER')
proxysql_db_password = os.getenv('PROXYSQL_DB_PASSWORD')
proxysql_backend_delete_delay = os.getenv('PROXYSQL_BACKEND_DELETE_DELAY')



app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="prodMysqlReplica02")
def prodMysqlReplica02(req: func.HttpRequest) -> func.HttpResponse:

    response_data = {
        "subscription_id": subscription_id,
        "resource_group_name": resource_group_name,
        "source_server_name": source_server_name,
        "replica_prefix": replicaPrefix,
        "location": location,
        "lowerbound": lowerbound,
        "db_lb_host": db_lb_host,
        "db_read_hostgroup_id": db_read_hostgroup_id,
        "db_dns": db_dns,
        "proxysql_db_admin_port": proxysql_db_admin_port,
        "proxysql_db_name": proxysql_db_name,
        "proxysql_db_user": proxysql_db_user,
        "proxysql_db_password": proxysql_db_password,
        "proxysql_backend_delete_delay": proxysql_backend_delete_delay
    }
    logging.info(response_data)
    logging.info(getAlertCondition(req))
   
    if(manageReplica(getAlertCondition(req))):
        return func.HttpResponse(
             "replica scaled successfully",
             status_code=200
        )
    else:
        return func.HttpResponse(
             "replica scailing failed",
             status_code=503
        )



def generate_service_name(constant, random_length=3):
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=random_length)).lower()
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y%m%d%H%M%S") + current_time.strftime("%f")[:3]
    service_name = f"{constant}-{random_string}{formatted_time}"
    return service_name

def createReplica():
    try:
        credential = DefaultAzureCredential()
        mysql_client = MySQLManagementClient(credential, subscription_id)
        sourceServer=mysql_client.servers.get(resource_group_name, source_server_name)
        if sourceServer:
            replicaInstance = Server(
                location=location,
                create_mode=CreateMode.REPLICA,
                source_server_resource_id=sourceServer.id
            )
            replicasName=generate_service_name(replicaPrefix,5)
            print(replicasName)
            async_create = mysql_client.servers.begin_create(resource_group_name,replicasName, replicaInstance)
            replica = async_create.result()
            return replicasName
    except Exception as e:
        logging.error(f"An error occurred while creating the replica: {e}")
        return None

def list_replicas():
    try:
        credential = DefaultAzureCredential()
        mysql_client = MySQLManagementClient(credential, subscription_id)
        replicas = mysql_client.servers.list_by_resource_group(resource_group_name)

        replica_list = []
        for replica in replicas:
            source_server_resource_id = replica.source_server_resource_id
            if(source_server_resource_id):
                replica_source_server_name = source_server_resource_id.split('/')[-1]
                if replica.replication_role == "Replica" and source_server_name in replica_source_server_name:
                    replica_list.append(replica.name)

        return replica_list

    except Exception as e:
        logging.error(f"An error occurred while listing replicas: {e}")
        return None
def delete_replica(replica_name):
    try:
        logging.info(replica_name)
        credential = DefaultAzureCredential()
        mysql_client = MySQLManagementClient(credential, subscription_id)
        async_delete = mysql_client.servers.begin_delete(resource_group_name, replica_name)
        async_delete.result()  # Wait for the deletion to complete

        logging.info(f"Replica {replica_name} deleted successfully.")
        return 1
    except Exception as e:
        logging.error(f"An error occurred while deleting the replica: {e}")
        return 0
    

def getAlertCondition(req):
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid JSON payload",
            status_code=400
        )
    try:
        condition = req_body['data']['alertContext']['condition']
        threshold = None

        if 'allOf' in condition:
            for metric_condition in condition['allOf']:
                if 'threshold' in metric_condition:
                    threshold = float(metric_condition['threshold'])
                    break
        
        return threshold

    except (KeyError, ValueError) as e:
        print(f"Error extracting threshold value: {e}")
        return None


def filter_replicas(replica_list, prefix):
    filteredList= [replica for replica in replica_list if replica.startswith(prefix)]
    if not filteredList:
        return None
    else:
        return filteredList



def scale_up(count):
    if(len(list_replicas())<=10):
        conn=connect_to_db(host=db_lb_host,
                       user=proxysql_db_user,
                       database=proxysql_db_name,
                       password=proxysql_db_password,
                       port=proxysql_db_admin_port)
        for i in range(count):
            replicaName=createReplica()
            if(replicaName):
                add_mysql_server(connection=conn,hostgroup_id=db_read_hostgroup_id,hostname=replicaName)
                logging.info(f"created replica:- {replicaName} ")
                logging.info(f"scaled up with scale factor:- {count}")

        return 1
    else:
        return 0


    
def scale_down(count):
    replicaList=filter_replicas(replica_list=list_replicas(),prefix=replicaPrefix)
    if(replicaList):
         conn=connect_to_db(host=db_lb_host,
                       user=proxysql_db_user,
                       database=proxysql_db_name,
                       password=proxysql_db_password,
                       port=proxysql_db_admin_port)
         for i in range(count):
             gracefully_stop_backend(connection=conn,hostname=replicaList[i])
             logging.info(f"waiting for {int(proxysql_backend_delete_delay)/60} minutes ")
             time.sleep(int(proxysql_backend_delete_delay))
             logging.info(f"deleting replica :- {replicaList[i]} ")
             delete_replica(replicaList[i])
             logging.info(f"deleting replica endpoint from proxySQL Cluster")
             delete_backend(connection=conn,hostgroup_id=db_read_hostgroup_id,hostname=replicaList[i])
             logging.info(f"scaled down with scale factor:- {count}")
         return 1
    else:
        return 0

    print(f"Scaled down by {count}. Total replicas: {self.replicas}")

def manageReplica(current_load):
    if (current_load >lowerbound):
        scale_factor = max(1, int(current_load /lowerbound))
        logging.info("Scale up started")
        return scale_up(scale_factor)
    elif current_load <= lowerbound:
        scale_factor = max(1, int(lowerbound / current_load))
        logging.info("Scale down started")
        return scale_down(scale_factor)
    else:
        logging.info("Load is within acceptable limits. No scaling required.")
        return 1

def connect_to_db(host, user, password, database,port):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port

        )
        if connection.is_connected():
            logging.info(f"connected to ProxySQL backend- {database} db")
            return connection
    except Exception as e:
        print(f"Error: {e}")
        return None

def add_mysql_server(connection, hostgroup_id, hostname):
    try:
        fqdn=f"{hostname}.{db_dns}"
        cursor = connection.cursor()
        query = "INSERT INTO mysql_servers (hostgroup_id, hostname) VALUES (%s, %s)"
        cursor.execute(query, (hostgroup_id, fqdn))
        connection.commit()

        cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
        cursor.execute("SAVE MYSQL SERVERS TO DISK;")
        connection.commit()
        print(f"Server {hostname} added successfully")
    except Exception as e:
       logging.exception(f"ProxySQl backend:-add failed :-{e} ")

def gracefully_stop_backend(connection, hostname):
    try:
        fqdn=f"{hostname}.{db_dns}"
        cursor = connection.cursor()
        query = "UPDATE mysql_servers SET status='OFFLINE_SOFT' WHERE hostname=%s"
        cursor.execute(query, (fqdn,))
        connection.commit()
        
        cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
        cursor.execute("SAVE MYSQL SERVERS TO DISK;")
        connection.commit()
        print(f"Server {hostname} stopped gracefully")
    except Exception as e:
        logging.exception(f"ProxySQl backend:- gracefully stop failed :-{e} ")

def force_stop_backend(connection, hostgroup_id, hostname):
    try:
        fqdn=f"{hostname}.{db_dns}"
        cursor = connection.cursor()
        query = "UPDATE mysql_servers SET status='OFFLINE_HARD' WHERE hostname=%s AND hostgroup_id=%s"
        cursor.execute(query, (fqdn, hostgroup_id))
        connection.commit()

        cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
        cursor.execute("SAVE MYSQL SERVERS TO DISK;")
        connection.commit()
        print(f"Server {hostname} force stopped")
    except Exception as e:
        logging.exception(f"ProxySQl backend:- force stop failed :-{e} ")

def delete_backend(connection,hostgroup_id,hostname):
    try:
        fqdn=f"{hostname}.{db_dns}"
        cursor = connection.cursor()
        query = "DELETE FROM mysql_servers WHERE hostgroup_id=%s AND hostname=%s"
        cursor.execute(query, (hostgroup_id, fqdn))
        connection.commit()

        cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
        cursor.execute("SAVE MYSQL SERVERS TO DISK;")
        connection.commit()
        print(f"Server {hostname} deleted")
    except Exception as e:
       logging.exception(f"proxysql-backend delete failed :-{e} ")
