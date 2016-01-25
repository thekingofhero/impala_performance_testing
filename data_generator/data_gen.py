import os
from config import local_config_gen
from multiprocessing import Process
from data_generator.gen_flat_file.gen_dim_table import gen_dims
from data_generator.impala.tables import Tables
import time
def push():
    #hosts = local_config_gen()['master']
    #hosts.extend(local_config_gen()['impalad_nodes'])
    hosts = local_config_gen()['impalad_nodes']
    cur_path = local_config_gen()['cur_path']
    work_dir = local_config_gen()['work_dir']
    for host in hosts:
        #rmdir if exists
        with os.popen("ssh %s 'test -d %s;echo $?'"%(host,work_dir)) as fp:
            tmp = fp.read().strip('\n')
            if tmp == '0' :
                os.system("ssh %s 'rm -rf %s'"%(host,work_dir))
        #mkdir
        os.system("ssh %s mkdir -p %s"%(host,work_dir))
        os.system('scp -r %s/* %s:%s'%(cur_path,host,work_dir))
    return hosts
    
def hdfs_mdir():
    cmd = "hdfs dfs -mkdir -p %s"
    root_dir = local_config_gen()['flatfile_path_HDFS']
    overwrite = local_config_gen()['overwrite']
    with os.popen('hdfs dfs -test -d %s;echo $?'%(root_dir)) as fp:
        tmp = fp.read().strip('\n')
        if tmp == '0':
            if not overwrite :
                #directory is exists ,no need to generate data
                print('%s is exists.There is no need to generate data'%(root_dir))
                return False
            else:
                os.system('hdfs dfs -rm -r -f %s'%(root_dir))
    dirs = list(map(lambda x :os.path.join(root_dir,x) ,local_config_gen()['tpcds_tbls'].keys()))
    for hdfs_dir in dirs:
        print("Making directory:%s"%(hdfs_dir))
        os.system(cmd%(hdfs_dir))
    return True


def gen_facts(hosts):
    tbls = local_config_gen()['partition_tbls']
    tpcds_tool_root = local_config_gen()['tpcds_tool_root']
    print('Partitioned table is generating flat file!')
    for tbl in tbls:
        print('%s is generating......'%(tbl))
        proc_list = []
        thread_count = local_config_gen()['dsdgen_threads_per_node']
        total_threads_num = len(hosts) * thread_count
        for i,host in enumerate(hosts):
            start_thread_id = i*thread_count + 1
            #PYTHONPATH needs to be set
            cmd = "ssh %s 'cd %s && export PYTHONPATH=$PYTHONPATH:. &&python %s %s %s %s'"%(host,
                                                            local_config_gen()['work_dir'],
                                                                            os.path.join('data_generator','gen_flat_file','gen_partition_table.py'),
                                                                            tbl,
                                                                            str(start_thread_id),
                                                                            str(total_threads_num))
            proc = Process(target = os.system,args=[cmd])
            proc_list.append(proc)
            proc.start()
        for proc in proc_list:
            if proc.is_alive():
                proc.join()
    
def data_gen():
    s = time.time()
    hosts = push()
    need = hdfs_mdir() 
    if need:
        gen_dims()
        e1 = time.time()
        print('Dims generated in %fs'%(e1 - s))
        gen_facts(hosts)
        e2 = time.time()
        print('Partition tables datafile generated in %fs'%(e2 - e1))
    else:
        e1=e2=0
    #load into impala
    tables = Tables()
    tables.create_db()
    tables.create_external_table()
    tables.load_table()
    e3 = time.time()
    print('Dims loaded in %fs'%(e3 - e2))
    tables.create_partition_tbl()
    tables.load_partition_tbl_store_sales()
    e4 = time.time()
    print('Partition tables loaded in %fs'%(e4 - e3))
    tables.compute_table_stats()
    e5 = time.time()
    print('Compute tables in %fs'%(e5 - e4))
    print('Total time in %ss'%(e5 - s))

if __name__ == '__main__':
    data_gen()
