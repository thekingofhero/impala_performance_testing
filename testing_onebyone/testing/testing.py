from config import local_config_testing,postgres_db_connector
from utilities.postgres import mydb
import os
from hdware_set.cpufreq_set import CPUFreqSet
from hdware_set.check_sys_stat import check_sys_state

class testing:
    def __init__(self):
        self.impalad_nodes = local_config_testing()['impalad_nodes']
        self.cpufreq_range = local_config_testing()['CPUFreq_range']
        self.query_list = local_config_testing()['query_list']
        self.query_dir = local_config_testing()['query_dir']
        self.every_query_times = local_config_testing()['every_query_times']
        self.impala_server = local_config_testing()['IMPALA_SERVER']
        self.database_name = local_config_testing()['DATABASE_NAME']
        self.sh_path = os.path.join(local_config_testing()['cur_path'],'testing_onebyone','runsql.sh')
        self.network = local_config_testing()['net_work']
        self.des_dic = {}
        self.CPUFreqSet_OBJ = CPUFreqSet()
        self.check_sys_state_obj = check_sys_state()
    
    def init_db(self):
        self.mydb1 = mydb(postgres_db_connector)
        

    def clear_cache(self):
        for host in self.impalad_nodes:
            cmd = "ssh %s 'free && sync && echo 3 >/proc/sys/vm/drop_caches && free'"%(host)
            os.system(cmd)
