
import os
import boto3
import time
from datetime import timedelta, datetime, timezone
import dateutil
import jmespath
import pytz
from botocore.exceptions import ClientError

import configuration
import schedulers
import time
from boto_retry import get_client_with_retries
from configuration import SchedulerConfigBuilder
from configuration.instance_schedule import InstanceSchedule
from configuration.running_period import RunningPeriod
from boto3.dynamodb.conditions import Key

START_TFR_BATCH_SIZE = 5

import boto3

from boto_retry import get_client_with_retries
ERR_STARTING_SERVERS = "Error starting servers {}, ({})"
ERR_STOPPING_SERVERS = "Error stopping servers {}, ({})"
ERR_MAINT_WINDOW_NOT_FOUND_OR_DISABLED = "SSM maintenance window {} used in schedule {} not found or disabled"

INF_FETCHING_SERVERS = " Fetching tfr servers for account {} in region {}"
INF_FETCHED_SERVERS = "Number of fetched tfr servers is {}, number of servers in a schedulable state is {}"

WARNING_SERVER_NOT_STARTING = "TFR server {} is not started"

WARNING_SERVER_NOT_STOPPING = "TFR server {} is not stopped"
INF_ADD_KEYS = "Adding {} ket(s) {} to servers(s) {}"
WARN_STARTED_SERVERS_TAGGING = "Error deleting or creating tags for started TFR servers {} ({})"
WARN_STOPPED_SERVERS_TAGGING = "Error deleting or creating tags for stopped TFR servers {} ({})"

DEBUG_SKIPPED_SERVER = "Skpping tfr server {} because it is not in a schedulable state ({})"
DEBUG_SELECTED_SERVERS = "Selected tfr server {} in state ({})"

MY_DEBUG = "1/3/2023 10:35 client = get_client_with_retries() {}"

class TfrService:
    TFR_STATE_ONLINE = "online"
    TFR_STATE_OFFLINE = "offline"
    TFR_STATE_STARTING = "starting"
    TFR_STATE_STOPPING = "stopping"

    TFR_STARTING_STATES = {TFR_STATE_STARTING,TFR_STATE_ONLINE}
    TFR_STOPPING_STATES = {TFR_STATE_STOPPING,TFR_STATE_OFFLINE}
    TFR_SCHEDULABLE_STATES = {TFR_STATE_ONLINE, TFR_STATE_OFFLINE}

    dynamodb = boto3.resource('dynamodb')
    maintenance_table = dynamodb.Table(os.environ['MAINTENANCE_WINDOW_TABLE'])

    def __init__(self):

        self.service_name = "transfer"
        self._ssm_maintenance_windows = None
        self._session = None
        self._logger = None
    
    def _init_scheduler(self,args):

        self._session = args.get(schedulers.PARAM_SESSION)
        self._context = args.get(schedulers.PARAM_CONTEXT)
        self._region = args.get(schedulers.PARAM_REGION)
        self._logger = args.get(schedulers.PARAM_LOGGER)
        self._account = args.get(schedulers.PARAM_ACCOUNT)
        self._tagname = args.get(schedulers.PARAM_TAG_NAME)
    
    def server_batches(cls, servers, size):

        server_buffer = []
        for server in servers:
            server_buffer.append(server)
            if len(server_buffer) == size:
                yield server_buffer
                server_buffer = []
        if len(server_buffer) > 0:
            yield server_buffer

#TODO 1
    def get_ssm_windows_service():
        pass

#TODO 2
    def get_ssm_windows_db():
        pass

#TODO 3
    def process_ssm_windows():
        pass

#TODO 4
    def check_window_running():
        pass

#TODO 5
    def put_window_dynamodb():
        pass

#TODO 6
    def remove_unused_windows():
        pass

#TODO 7
    def get_ssm_windows():
        pass

#TODO 8
    def ssm_maintenance_windows():
        pass

    def get_schedulable_instances(self, kwargs):

        self._session = kwargs[schedulers.PARAM_SESSION]
        context = kwargs[schedulers.PARAM_CONTEXT]
        region = kwargs[schedulers.PARAM_REGION]
        account = kwargs[schedulers.PARAM_ACCOUNT]
        self._logger = kwargs[schedulers.PARAM_LOGGER]
        tagname = kwargs[schedulers.PARAM_CONFIG].tag_name
        config = kwargs[schedulers.PARAM_CONFIG]
        
        self._logger.info('inside ----------------- def get_schedulable_instances()')
    
        self._logger.info("Enable SSM Maintenance window is set to {}", config.enable_SSM_maintenance_windows)
        if config.enable_SSM_maintenance_windows:
            self._logger.debug("load the ssm maintenance windows for account {}, and region {}", account, region)        
            self._ssm_maintenance_windows = self._ssm_maintenance_windows(self._)        
            self._logger.debug("finish loading the ssm maintenance windows")
        
        client = get_client_with_retries("transfer",["describe_server"], context = context, session = self._session, region = region)
        self._logger.debug(MY_DEBUG, client)
        # def __repr__(self):
        #     return self._logger.debug(MY_DEBUG, client.__str__())
       

        def is_in_schedulable_state(tfr_serv):
            state = tfr_serv["state"]
            return state in TfrService.TFR_SCHEDULABLE_STATES

        jmes = "Servers[*].{ServerId:ServerId, State:State, Tags:Tags}[]" + \
                "|[?Tags]|[?contains(Tags[*].Key, '{}')]".format(tagname)
        
        args = {}
        number_of_servers = 0
        servers = []
        done = False 

        self._logger.info(INF_FETCHING_SERVERS, account, region)

        while not done:

            server_resp = client.describe_server_with_retries(**args)
            for server_list in jmespath.search(jmes, server_resp):
                serv = self._select_server_data(server=server_list, tagname=tagname, config=config)
                number_of_servers += 1
                if is_in_schedulable_state(serv):
                    servers.append(serv)
                    self._logger.debug(DEBUG_SELECTED_SERVERS, serv[schedulers.SERV_ID], serv[schedulers.SERV_STATE_NAME])
                else:
                    self._logger.debug(DEBUG_SKIPPED_SERVER, serv[schedulers.SERV_ID], serv[schedulers.SERV_STATE_NAME])
            if "NextToken" in server_resp:
                args["NextToken"] = server_resp["NextToken"]
            else:
                done = True
        self._logger.info(INF_FETCHED_SERVERS, number_of_servers, len(servers))
        return servers
        
#TODO 9
    def _schedule_from_maintenance_window():
        pass

    def _select_server_data(self, server, tagname, config):
        
        self._init_scheduler(kwargs)
        self._logger.info('inside ----------------- def _select_server_data()')
        
        def get_tags(serv):
            return {tag["Key"]: tag["Value"] for tag in serv["Tags"]} if "Tags" in serv else {}

        tags = get_tags(server)
        name = tags.get("Name", "")
        server_id = server["ServerId"]
        state = server["State"]
        is_online = self.TFR_STATE_ONLINE == state
        is_offline = self.TFR_STATE_OFFLINE == state
        schedule_name = tags.get(tagname)

        maintenance_window_schedule = None
        schedule = config.schedules.get(schedule_name, None)
        if schedule is not None:
            if schedule.use_maintenance_window and schedule.ssm_maintenance_window not in [None, ""]:
                maintenance_window_schedule = self._ssm_maintenance_windows.get(schedule.ssm_maintenance_window, None)
                if maintenance_window_schedule is None:
                    self._logger.error(ERR_MAINT_WINDOW_NOT_FOUND_OR_DISABLED, schedule.ssm_maintenance_window,
                                       schedule.name)
                    self._ssm_maintenance_windows[schedule.ssm_maintenance_window] = "NOT-FOUND"
                if maintenance_window_schedule == "NOT-FOUND":
                    maintenance_window_schedule = None
        server_data = {
            schedulers.SERV_ID: server_id,
            schedulers.SERV_SCHEDULE: schedule_name,
            schedulers.SERV_NAME: name,
            schedulers.SERV_STATE: state,
            schedulers.SERV_STATE_NAME: server["State"]["Name"],
            schedulers.SERV_IS_ONLINE: is_online,
            schedulers.SERV_IS_OFFLINE: is_offline,
            schedulers.SERV_CURRENT_STATE: InstanceSchedule.STATE_ONLINE if is_online else InstanceSchedule.STATE_STOPPED,
            schedulers.INST_INSTANCE_TYPE: server["InstanceType"],
            schedulers.SERV_TAGS: tags,
            schedulers.SERV_MAINTENANCE_WINDOW: maintenance_window_schedule
        }
        return server_data

    def get_tfr_server_status(self, client, server_ids):
        self._init_scheduler(kwargs)
        self._logger.info('inside ----------------- def get_tfr_server_status()')
        servers = client.describe_server_with_retries(ServerId=server_ids)
        self._logger.debug('________________ servers', servers)
        jmes = "Servers[*].{ServerId:ServerId, State:State}[]"
        #self._logger.debug('________________ jmes', jmes)
        return jmespath.search(jmes, servers)
    
    def start_server(self, kwargs):
        
        def is_in_starting_state(state):
            return (state) in TfrService.TFR_STARTING_STATES
        
        self._init_scheduler(kwargs)
        self._logger.info('inside ----------------- def start_server()')
        servers_to_start = kwargs[schedulers.PARAM_STARTED_SERVERS]
        self._logger.debug('________________ servers_to_start', servers_to_start)
        start_tags = kwargs[schedulers.PARAM_CONFIG].started_tags
        if start_tags is None:
            start_tags = []
        start_tags_key_names = [t["Key"] for t in start_tags]
        stop_tags_keys = [{"Key": t["Key"]} for t in kwargs[schedulers.PARAM_CONFIG].stopped_tags if t["Key"] not in start_tags_key_names]
        methods = ["start_server", "describe_server", "create_tags", "delete_tags"]
        client = get_client_with_retries("transfer", methods=methods, context= self._context, session= self._session, region= self._region)

        for server_batch in list(self.server_batches(servers_to_start, START_TFR_BATCH_SIZE)):
            server_ids = [i.id for i in server_batch]
            self._logger.debug('________________ server_ids', server_ids)
            try:
                start_response = client.start_server_with_retries(ServerId=server_ids)
                servers_starting = [i["ServerId"] for i in start_response.get("StartingServers",[]) if is_in_starting_state(i.get("CurrentState",{}))]

                get_status_count = 0
                if len(servers_starting) < len(server_ids):
                    time.sleep(5)

                    servers_starting = [i["ServerId"] for i in self.get_tfr_server_status(client, server_ids) if is_in_starting_state(i.get("State",{}))]
                    self._logger.debug('________________ servers_starting', servers_starting)
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
                    yield i, InstanceSchedule.STATE_RUNNING
            except Exception as ex:
                self._logger.error(ERR_STARTING_SERVERS,",".join(server_ids), str(ex))


    def stop_server(self, kwargs):
        
        def is_in_stopping_state(state):
            return (state) in TfrService.TFR_STOPPING_STATES
        
        self._init_scheduler(kwargs)
        self._logger.info('inside ----------------- def stop_server()')
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
                    yield i, InstanceSchedule.STATE_STOPPED
                
            except Exception as ex:
                self._logger.error(ERR_STOPPING_SERVERS, ",".join(server_ids), str(ex))
