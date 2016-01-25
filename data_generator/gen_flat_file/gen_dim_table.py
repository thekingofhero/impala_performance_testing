import os
from config import local_config_gen
from multiprocessing import Process
def gen_dims():
    root_dir = local_config_gen()['flatfile_path_HDFS']
    proc_list = []
    print('Generating table flat file!')
    for tbl in local_config_gen()['tpcds_tbls'].keys():
        if tbl in local_config_gen()['partition_tbls']:
            continue
        if tbl in local_config_gen()['bind_tbl']:
            continue
        print('%s is generating......'%(tbl))
        cmd = "%s \
                -TABLE %s \
                -SCALE %s \
                -DISTRIBUTIONS %s \
                -TERMINATE N \
                -FILTER Y \
                -QUIET Y |hdfs dfs -put - %s.dat \
            "%(os.path.join(local_config_gen()['tpcds_tool_root'],'tools','dsdgen'),
                tbl,
                local_config_gen()['tpcds_scale_factor'],
                os.path.join(local_config_gen()['tpcds_tool_root'],'tools','tpcds.idx'),
                os.path.join(root_dir,tbl,tbl))
        proc = Process(target=os.system,args=[cmd])
        proc_list.append(proc)
        proc.start()
    for proc in proc_list:
        if proc.is_alive():
            proc.join()
    print('Generated')
