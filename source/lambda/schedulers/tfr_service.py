# sys.path.insert(0, "C:\\Users\\spatel\\Documents\\GitHub\\aws-instance-scheduler\\source\\lambda")

from http import client
import time
import os

import sys
from tracemalloc import start
from xmlrpc import server

from yaml import NodeEvent
from configuration.server_schedule import ServerSchedule

START_BATCH_SIZE = 5
import jmespath
import schedulers
# AWS_ACCESS_KEY_ID =  os.environ['AWS_ACCESS_KEY_ID']
# AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
# AWS_SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']


# use EE-ENG-R1C servers :
# s-29c57eaef00c4b4cb 

import boto3

from boto_retry import get_client_with_retries
ERR_STARTING_SERVERS = "Error starting servers {}, ({})"
ERR_STOPPING_SERVERS = "Error stopping servers {}, ({})"
WARNING_SERVER_NOT_STARTING = "TFR server {} is not started"

WARNING_SERVER_NOT_STOPPING = "TFR server {} is not stopped"
INF_ADD_KEYS = "Adding {} ket(s) {} to servers(s) {}"
WARN_STARTED_SERVERS_TAGGING = "Error deleting or creating tags for started TFR servers {} ({})"
WARN_STOPPED_SERVERS_TAGGING = "Error deleting or creating tags for stopped TFR servers {} ({})"

#client = boto3.client('transfer', _region='us-east-1')

#serverId = ['s-29c57eaef00c4b4cb']
#_region = 'us-east-1'
class tfrService:
    print("***** inside the class *****")
    # TODO check if server lifecycle has states = stopping or starting or is it pending?
    TFR_STATE_ONLINE = "online"
    TFR_STATE_OFFLINE = "offline"
    TFR_STATE_STARTING = "starting"
    TFR_STATE_STOPPING = "stopping"

    TFR_STARTING_STATES = {TFR_STATE_STARTING,TFR_STATE_ONLINE}
    TFR_STOPPING_STATES = {TFR_STATE_STOPPING,TFR_STATE_OFFLINE}
    TFR_SCHEDULABLE_STATES = {TFR_STATE_ONLINE, TFR_STATE_OFFLINE}

    dynamodb = boto3.resource('dynamodb')
    #maintenance_table = dynamodb.Table(os.environ['MAINTENANCE_WINDOW_TABLE'])

    def __init__(self):
        print("***** inside __init__ *****")
        self.service_name = "transfer"
        self._ssm_maintenance_windows = None
        self._session = None
        self._logger = None
    
    def _init_scheduler(self,args):
        print("***** _init_scheduler *****")
        self._session = args.get(schedulers.PARAM_SESSION)
        self._context = args.get(schedulers.PARAM_CONTEXT)
        self._region = args.get(schedulers.PARAM_REGION)
        self._logger = args.get(schedulers.PARAM_LOGGER)
        self._account = args.get(schedulers.PARAM_ACCOUNT)
        self._tagname = args.get(schedulers.PARAM_TAG_NAME)
    
    def server_batches(cls, servers, size):
        print("***** inside server_batches  *****")
        server_buffer = []
        for server in servers:
            server_buffer.append(server)
            if len(server_buffer) == size:
                yield server_buffer
                server_buffer = []
        if len(server_buffer) > 0:
            yield server_buffer
    # TODO from ec2_servic.py
    def get_schedulable_servers():
        pass
    
    def get_tfr_server_status(self, client, server_ids):
        print("***** inside get_tfr_server_status *****")
        servers = client.describe_server_with_retries(ServerId=server_ids)
        # print(servers)
        jmes = "Servers[*].{ServerId:ServerId, State:State}[]"
        return jmespath.search(jmes, servers)
    
    def start_server(self, kwargs):
        print("***** inside start_server *****")
        def is_in_starting_state(state):
            return (state) in tfrService.TFR_STARTING_STATES
        
        self._init_scheduler(kwargs)

        servers_to_start = kwargs[schedulers.PARAM_STARTED_SERVERS]
        print("servers_to_start", servers_to_start)
        start_tags = kwargs[schedulers.PARAM_CONFIG].started_tags
        if start_tags is None:
            start_tags = []
        start_tags_key_names = [t["Key"] for t in start_tags]
        stop_tags_keys = [{"Key": t["Key"]} for t in kwargs[schedulers.PARAM_CONFIG].stopped_tags if t["Key"] not in start_tags_key_names]
        methods = ["start_server", "describe_server", "create_tags", "delete_tags"]
        client = get_client_with_retries("transfer", methods= methods, context= self._context, session= self._session, region= self._region)

        for server_batch in list(self.server_batches(servers_to_start, START_BATCH_SIZE)):
            server_ids = [i.id for i in server_batch]
            try:
                start_response = client.start_server_with_retries(ServerId=server_ids)
                servers_starting = [i["ServerId"] for i in start_response.get("StartingServers",[]) if is_in_starting_state(i.get("CurrentState",{}))]

                get_status_count = 0
                if len(servers_starting) < len(server_ids):
                    time.sleep(5)

                    servers_starting = [i["ServerId"] for i in self.get_tfr_server_status(client, server_ids) if is_in_starting_state(i.get("State",{}))]

                    if len(servers_starting) == len(server_ids):
                        break

                    get_status_count += 1
                    if get_status_count > 3:
                        for i in server_ids:
                            if i not in servers_starting:
                                self._logger.warning(WARNING_SERVER_NOT_STARTING,i)
                        break
                
                if len(servers_starting) > 0:
                    try:
                        if stop_tags_keys is not None and len(stop_tags_keys) > 0:
                            self._logger.info("offline",",".join(["\"{}\"".format(k["Key"]) for k in stop_tags_keys]),",".join(servers_starting))
                            client.delete_tags_with_retries(Resources=servers_starting,Tags=stop_tags_keys)
                        if len(start_tags) > 0:
                            self._logger.info(INF_ADD_KEYS,"start", str(start_tags),",".join(servers_starting,))
                            client.create_tags_with_retries(Resources=servers_starting,Tags=start_tags)
                    except Exception as ex:
                        self._logger.warning(WARN_STARTED_SERVERS_TAGGING,",".join(servers_starting, str(ex)))
                
                for i in servers_starting:
                    # TODO STATE_STARTING defined in server_scheduel.py 
                    yield i, ServerSchedule.STATE_STARTING
            except Exception as ex:
                self._logger.error(ERR_STARTING_SERVERS,",".join(server_ids), str(ex))


    def stop_server(self, kwargs):
        
        def is_in_stopping_state(state):
            return (state) in tfrService.TFR_STOPPING_STATES
        
        self._init_scheduler(kwargs)

        stopped_servers = kwargs[schedulers.PARAM_STOPPED_SERVERS]
        stop_tags = kwargs[schedulers.PARAM_CONFIG].stopped_tags
        if stop_tags is None:
            stop_tags = []
        stop_tags_key_names = [t["Key"] for t in stop_tags]

        start_tags_keys = [{"Key": t["Key"]} for t in kwargs[schedulers.PARAM_CONFIG].started_tags if t["Key"] not in stop_tags_key_names]

        methods = ["stop_server", "create_tags", "delete_tags", "describe_server"]
        client = get_client_with_retries("transfer", methods= methods, context= self._context, session= self._session, region = self._region)

        for server_batch in list(self.server_batches(stopped_servers)):

            server_ids = [i.id for i in server_batch]

            servers_stopping = []

            try:
                if len(servers_stopping) < len(server_ids):
                    time.sleep(5)

                    servers_stopping = [i["ServerId"] for i in self.get_tfr_server_status(client, server_ids)if is_in_stopping_state(i.get("State", {}))]

                    if len(servers_stopping) == len(server_ids):
                        break

                    get_status_count += 1
                    if get_status_count > 3:
                        for i in server_ids:
                            if i not in servers_stopping:
                                self._logger.warning(WARNING_SERVER_NOT_STOPPING, i)
                        break

                if len(servers_stopping) > 0:
                    try:
                        if start_tags_keys is not None and len(start_tags_keys):
                            self._logger.info("start", ",".join(["\"{}\"".format(k["Key"]) for k in start_tags_keys]), ",".join(servers_stopping))
                            client.delete_tags_with_retries(Resources = servers_stopping, Tags = start_tags_keys)
                        if len(stop_tags) > 0:
                            self._logger.info("stop", str(stop_tags), ",".join(servers_stopping))
                            client.create_tags_with_retries(Resources = servers_stopping, Tags = stop_tags)
                    except Exception as ex:
                        self._logger.warning(WARN_STOPPED_SERVERS_TAGGING, ",".join(servers_stopping), str(ex))

                for i in servers_stopping:
                    yield i, ServerSchedule.STATE_STARTING
                
            except Exception as ex:
                self._logger.error(ERR_STOPPING_SERVERS, ",".join(server_ids), str(ex))

# # Instance of class tfrService
# Object = tfrService()

# # Calling start_server function
# Object.start_server(serverId)
