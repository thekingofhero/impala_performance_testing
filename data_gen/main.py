import os
from config import local_config
from multiprocessing import Process
from gen_flat_file.gen_dim_table import gen_dims
from impala.tables import Tables
def push():
    #hosts = local_config()['master']
    #hosts.extend(local_config()['impalad_nodes'])
    hosts = local_config()['impalad_nodes']
    cur_path = os.path.dirname(os.path.realpath(__file__))
    work_dir = local_config()['work_dir']
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
    root_dir = local_config()['flatfile_path_HDFS']
    overwrite = local_config()['overwrite']
    with os.popen('hdfs dfs -test -d %s;echo $?'%(root_dir)) as fp:
        tmp = fp.read().strip('\n')
        if tmp == '0':
            if not overwrite :
                #directory is exists ,no need to generate data
                print('%s is exists.There is no need to generate data'%(root_dir))
                return False
            else:
                os.system('hdfs dfs -rm -r -f %s'%(root_dir))
    dirs = list(map(lambda x :os.path.join(root_dir,x) ,local_config()['tpcds_tbls'].keys()))
    for hdfs_dir in dirs:
        print("Making directory:%s"%(hdfs_dir))
        os.system(cmd%(hdfs_dir))
    return True


def gen_facts(hosts):
    tbls = local_config()['partition_tbls']
    tpcds_tool_root = local_config()['tpcds_tool_root']
    print('Partitioned table is generating flat file!')
    for tbl in tbls:
        print('%s is generating......'%(tbl))
        proc_list = []
        thread_count = local_config()['dsdgen_threads_per_node']
        total_threads_num = len(hosts) * thread_count
        for i,host in enumerate(hosts):
            start_thread_id = i*thread_count + 1
            #PYTHONPATH needs to be set
            cmd = "ssh %s 'cd %s && export PYTHONPATH=$PYTHONPATH:. &&python %s %s %s %s'"%(host,
                                                            local_config()['work_dir'],
                                                                            os.path.join('gen_flat_file','gen_partition_table.py'),
                                                                            tbl,
                                                                            str(start_thread_id),
                                                                            str(total_threads_num))
            proc = Process(target = os.system,args=[cmd])
            proc_list.append(proc)
            proc.start()
        for proc in proc_list:
            if proc.is_alive():
                proc.join()
    
def main():
    hosts = push()
    need = hdfs_mdir() 
    if need:
        gen_dims()
        gen_facts(hosts)

    #load into impala
    #tables = Tables()
    #tables.create_db()
    #tables.create_external_table()
    #tables.load_table()
    #tables.create_partition_tbl()
    #tables.load_partition_tbl_store_sales()
if __name__ == '__main__':
    main()
