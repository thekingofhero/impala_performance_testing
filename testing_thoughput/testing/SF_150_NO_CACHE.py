from config import local_config
from hdware_set.cpufreq_set import CPUFreqSet
from hdware_set.check_sys_stat import check_sys_state
from utilities.time_trans import get_time_by_second 
import datetime
import os
from impala.dbapi import connect
import time

class SF_150_NO_CACHE:
    def __init__(self,logging,log_dir,user_name):
        self.impalad_nodes = local_config()['impalad_nodes']
        self.cpufreq_range = local_config()['CPUFreq_range']
        self.query_list = local_config()['query_list']
        self.query_dir = local_config()['query_dir']
        self.impala_server = local_config()['IMPALA_SERVER']
        self.database_name = local_config()['DATABASE_NAME']
        self.sh_path = os.path.join(local_config()['install_path'],'queries','runsql.sh')
        self.profile_path = log_dir 
        self.network = local_config()['net_work']
        self.des_dic = {}
        self.CPUFreqSet_OBJ = CPUFreqSet()
        self.check_sys_state_obj = check_sys_state()
        self.logging = logging
        self.user_name = user_name
        self.users_InTotal = os.path.split(os.path.split(self.profile_path)[0])[1]
        

    def run_testing(self):
        conn = connect(host='172.168.0.24', port=21050,database = 'tpcds_text_150')
        cursor = conn.cursor()
        count = 0
        cur_task = 0
        query_time = []
        with open(os.path.join(self.profile_path,'RESULT'),'w') as fp_w:
            for query in self.query_list:
                if query not in self.des_dic.keys():
                     self.des_dic[query] = {}
                for cpufreq in self.cpufreq_range:
                    if cpufreq not in self.des_dic[query].keys():
                        self.CPUFreqSet_OBJ.set(cpufreq)
                        self.des_dic[query][cpufreq] = []
                    start_time = time.time()
                    #for i in range(self.every_query_times):
                    
                    with open(os.path.join(local_config()['query_dir'],query),'r') as fp:
                         sql= fp.read().strip('profile;\n')
                         sql = sql.strip('; ')
                    while True:
                        query_start_time = time.time()
                        cursor.execute('%s'%(sql))
                        end_time = time.time()
                        query_time.append(end_time - query_start_time)
                        while True:
                            row=cursor.fetchone()
                            if row:
                                pass
                            else:
                                break
                        
                        cur_profile = cursor.get_profile()
                        
                        count = count + 1
                        
                        with open(os.path.join(self.profile_path,str(count)+'.log'),'w') as fp_profile:
                            fp_profile.write("%s"%(cur_profile))
                        
                        if end_time - start_time > local_config()['duration_time'] :
                            break
                        
                    self.des_dic[query][cpufreq].sort()
            print "%s,%s,%s"%(self.users_InTotal,self.user_name,count)
            #print >>self.logging,"%s,%s,%s"%(self.profile_path,self.user_name,count)
            fp_w.write("%s,%s,%s,%s"%(self.users_InTotal,self.user_name,count, query_time))
        cursor.close()
        conn.close()
        return self.des_dic
        
   
if __name__ == '__main__':
    with open('hehe','w') as fp:
        SF_150_NO_CACHE_obj = SF_150_NO_CACHE(fp,'./aa','1')
        SF_150_NO_CACHE_obj.run_testing()
