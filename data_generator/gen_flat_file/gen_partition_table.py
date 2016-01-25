import os
import sys
from config import local_config_gen
import threading 
def gen_partition_tbl(tbl,start_thread_id,total_threads_num):
    tpcds_tool_root = local_config_gen()['tpcds_tool_root']
    thread_count = local_config_gen()['dsdgen_threads_per_node']
    thread_list = []
    for thread_i in range(start_thread_id,start_thread_id + thread_count):
        cmd = "%s \
                         -TABLE %s \
                         -SCALE %s \
                         -CHILD %s \
                         -PARALLEL %s \
                         -DISTRIBUTIONS %s \
                         -TERMINATE N \
                         -FILTER Y \
                         -QUIET Y | hdfs dfs -put - %s.dat\
               "%(os.path.join(tpcds_tool_root,'tools','dsdgen'),
                                tbl,
                                local_config_gen()['tpcds_scale_factor'],
                                thread_i,
                                total_threads_num,
                                os.path.join(tpcds_tool_root,'tools','tpcds.idx'),
                                os.path.join(local_config_gen()['flatfile_path_HDFS'],tbl,'_'.join([tbl,str(thread_i),str(total_threads_num)])))
        t = threading.Thread(target = os.system,args=[cmd])
        thread_list.append(t)
        t.start()
    for t in thread_list:
        if t.is_alive():
            t.join()
    print('Generated')

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 4:
        print("python gen_partition_table.py [tbl] [start_thread_id] [total_threads_num]")
    gen_partition_tbl(args[1],int(args[2]),int(args[3]))
