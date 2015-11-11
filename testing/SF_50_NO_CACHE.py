from config import local_config
from hdware_set.cpufreq_set import CPUFreqSet
from hdware_set.check_sys_stat import check_sys_state
from utilities.time_trans import get_time_by_second 
import datetime
import os

class SF_50_NO_CACHE:
    def __init__(self,logging,datetime_now):
        self.datanode_list = local_config()['datanode_list']
        self.cpufreq_range = local_config()['CPUFreq_range']
        self.query_list = local_config()['query_list']
        self.query_dir = local_config()['query_dir']
        self.every_query_times = local_config()['every_query_times']
        self.impala_server = local_config()['IMPALA_SERVER']
        self.database_name = local_config()['DATABASE_NAME']
        self.sh_path = os.path.join(local_config()['install_path'],'queries','runsql.sh')
        self.profile_path = os.path.join(local_config()['install_path'],'queries/logs/%s'%(datetime_now))
        self.network = local_config()['net_work']
        self.des_dic = {}
        self.CPUFreqSet_OBJ = CPUFreqSet()
        self.check_sys_state_obj = check_sys_state()
        self.logging = logging
        

    def run_testing(self):
        sum_task = len(self.query_list) * len(self.cpufreq_range) * self.every_query_times
        cur_task = 0
        with open(os.path.join(self.profile_path,'PROCESSING'),'w') as fp:
             fp.write('%s%%'%(str(float(cur_task)/float(sum_task)*100)))
        for query in self.query_list:
            if query not in self.des_dic.keys():
                self.des_dic[query] = {}
            for cpufreq in self.cpufreq_range:
                if cpufreq not in self.des_dic[query].keys():
                    self.CPUFreqSet_OBJ.set(cpufreq)
                    self.des_dic[query][cpufreq] = []
                for i in range(self.every_query_times):
                    if self.check_sys_state_obj.check_sys_io():
                        self.logging.info('"cpu_freq:%s---%s %s %s %s %s" has been started...'%(str(cpufreq)\
                                                        ,self.sh_path\
                                                        ,self.impala_server\
                                                        ,self.query_dir\
                                                        ,self.database_name\
                                                        ,query))
                        cmd = '%s %s %s %s %s %s %s'%(self.sh_path\
                                                        ,self.impala_server\
                                                        ,self.query_dir\
                                                        ,self.database_name\
                                                        ,query\
                                                        ,local_config()['sys_log']\
                                                        ,self.profile_path)

                        fp = os.popen(cmd,'r')
                        lines_list = fp.readlines()
                        try:
                            time_stamp = lines_list[-3].split(':')[-1].strip('\n')
                            time_line = get_time_by_second(lines_list[-2].split(' ')[-1])
                            remote_start_time = get_time_by_second((lines_list[-1].split(' ')[-2]).strip('\n'))
                            query_run_time = time_line - remote_start_time
                            self.des_dic[query][cpufreq].append([query_run_time,time_line,remote_start_time,time_stamp])
                        except Exception :
                            print ''.join(str_i for str_i in lines_list)
                        self.logging.info('"cpu_freq:%s---%s %s %s %s %s" has been finished in %ss!time_stamp is %s'%(str(cpufreq)\
                                                        ,self.sh_path\
                                                        ,self.impala_server\
                                                        ,self.query_dir\
                                                        ,self.database_name\
                                                        ,query\
                                                        ,str([query_run_time,time_line,remote_start_time])\
                                                        ,time_stamp))
                        fp.close()
                        cur_task += 1 
                        with open(os.path.join(self.profile_path,'PROCESSING'),'w') as fp:
                            fp.write('%s%%'%(str(float(cur_task)/float(sum_task)*100)))
                self.des_dic[query][cpufreq].sort()
        return self.des_dic

    def get_fastest(self,):
        fastest_logdir = os.path.join(self.profile_path,'fastest_logs')
        os.system('mkdir %s'%(fastest_logdir))

        fastest_list = []
        for query in self.des_dic.keys():
            for cpufreq in self.des_dic[query].keys():
                # add fastest one's  timestamp
                fastest_list.append(query + '_' +self.des_dic[query][cpufreq][0][-1])
        for timestamp in fastest_list:
            logname = os.path.join(self.profile_path, str(timestamp))
            cmd = 'cp -r %s %s'%(logname,fastest_logdir)
            os.system(cmd)
    def write_to_excel(self,work_book):
        sheet_name = 'SF_50_%s_NO_CACHE'%(self.network)
        work_book.create_sheet(sheet_name)
        work_book.set_cell(sheet_name,1,1,self.database_name)
        start_row = 3
        row = start_row
        for query in self.des_dic.keys():
            col = 2
            work_book.set_cell(sheet_name,col,row,query)
            col += 1
            for cpufreq in self.des_dic[query].keys():
                if row == start_row:
                    work_book.set_cell(sheet_name,col,row - 2,cpufreq)
                for i in range(self.every_query_times):
                    if row == start_row:
                        work_book.set_cell(sheet_name,col,row - 1,'TIME_LINE')
                        work_book.set_cell(sheet_name,col+1,row - 1,'Remote_Start_Time')
                        work_book.set_cell(sheet_name,col+2,row - 1,'Query_Run_Time')
                        work_book.set_cell(sheet_name,col+3,row - 1,'Time_Stamp')
                    query_run_time,time_line,remote_start_time,time_stamp = self.des_dic[query][cpufreq][i]
                    work_book.set_cell(sheet_name,col,row,time_line)
                    col += 1
                    work_book.set_cell(sheet_name,col,row,remote_start_time)
                    col += 1
                    work_book.set_cell(sheet_name,col,row,query_run_time)
                    col += 1
                    work_book.set_cell(sheet_name,col,row,time_stamp)
                    col += 1
            row += 1

        


                        
                                

            
                

    

if __name__ == '__main__':
    SF_50_10G_NO_CACHE_obj = SF_50_NO_CACHE()
    SF_50_10G_NO_CACHE_obj.run_testing()
