from config import local_config
from excel_relate.excel_relate import excel_writer
import testing
import datetime
import os
import re
import distinct_user
from multiprocessing import *
import time

def clear_cache():
    for host in self.impalad_nodes:
        cmd = "ssh %s 'free && sync && echo 3 >/proc/sys/vm/drop_caches && free'"%(host)
        os.system(cmd)

def collect_result(log_dir):
    result = ''
    for root,dir,files in os.walk(log_dir):
        for file in files:
            if file == 'RESULT':
                with open(os.path.join(root,file),'r') as fp:
                    result = ''.join([result,fp.read(),'\n'])
    with open(os.path.join(log_dir,'result_total.csv'),'w') as fpw:
        fpw.write(result)

def main():
    #program log path
    datetime_now = str(datetime.datetime.now().strftime("DATE%Y-%m-%d_TIME%H-%M"))
    #datetime_now = str("DATE2015-12-21_TIME13-24")
    start_timestamp = time.mktime(time.strptime(datetime_now,"DATE%Y-%m-%d_TIME%H-%M"))
    log_dir = './queries/logs/%s'%(datetime_now)
    
    if not os.path.isdir(log_dir):
        os.system('mkdir -p %s'%(log_dir))

    start = time.time()
    with open("%s/renderman.log"%(log_dir),'w') as logging:
        for user_number in local_config()['user_number']:
            start_timestamp = time.mktime(time.strptime(datetime.datetime.now().strftime("DATE%Y-%m-%d_TIME%H-%M"),"DATE%Y-%m-%d_TIME%H-%M"))
            #init & start consumer
            base_log_dir = os.path.join(log_dir,''.join(['user_number=',str(user_number)]))
            user_list = []
            clear_cache()

            for i in xrange(user_number):
                u = Process(target=distinct_user.user, args = [i,base_log_dir,logging])
                user_list.append(u)
            for user in user_list:
                user.start()
            while True:
                running = 0
                for user in user_list:
                   if user.is_alive():
                       running = running + 1
                if running == 0 :
                    break
            #collect logs
            ##remote_log
            print("Preparing remote logs")
            for desired_log in local_config()['desired_logs']['remote']:
                cmd_list = map(lambda x : 'ssh %s "python /home/wangdewei/extractLog.py %s %s"'%(x,str(start_timestamp),desired_log),local_config()['impalad_nodes'])
                res = [os.system(cmd) for cmd in cmd_list]
            #  scp
            scp_list = map(lambda x: 'scp %s:/home/wangdewei/extractLog/* %s'%(x,base_log_dir),local_config()['impalad_nodes'])
            res = [os.system(scp) for scp in scp_list]
            ##local
            print("Preparing local logs")
            for desired_log in local_config()['desired_logs']['local']:
                cmd = 'python extractLog.py %s %s %s'%(desired_log,str(start_timestamp),base_log_dir)
                print(cmd)
         #       os.system(cmd)

    collect_result(log_dir)
    end = time.time()
    duration = end - start
    print 'run in %f seconds'%(duration)
	
if __name__ == '__main__':
     main()

