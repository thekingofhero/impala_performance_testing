import os
def local_config():
    #HDFS DATANODE
    impalad_nodes = [
                 'tracing024',
                 'tracing025',
                 'tracing026',
                 'tracing027',
                 ]
    #IMPALA CONFIG
    IMPALA_SERVER = impalad_nodes[0]
    #db_type:text/parquet/parquet_snappy
    db_type = 'text'
    #db_size:50,3000
    db_size = 150
    DATABASE_NAME = 'tpcds_%s_%s'%(db_type,db_size)
    

    #if use nmon ,set sys_log='all';else 'profile'
    sys_log = 'profile'
    #TESTING CASE
    test_case_list = [
        'SF_%s_NO_CACHE'%(db_size),
        #'SF_%s_CACHE'%(db_size),
    ]
    #HARD WARE
    #cpu
    #CPUFreq_range = [1.8,2.1,2.4,2.7]
    CPUFreq_range = [2.7]
    cpu_freq_dict = {
        '1.2':1200000,
        '1.6':1600000,
        '1.8':1800000,
        '2.1':2100000,
        '2.4':2400000,
        '2.5':2500000,
        '2.7':2700000,
    }
    #network
    net_work = '10G'

    #QUERY
    #query_type:text/parquet/parquet_snappy
    query_type = db_type
    query_dir = os.path.join('./queries/',query_type)
    query_list = [
             'q19.sql',
           # 'q27.sql',
           #  'q34.sql',
           #   'q3.sql' ,
           # 'q42.sql',
           #  'q43.sql',
           # 'q46.sql',
           # 'q52.sql',
           #  'q53.sql',
           #  'q55.sql',
           #  'q63.sql',
           # 'q68.sql',
           #'q79.sql',
           #  'q7.sql' ,
           #  'q89.sql',
           # 'q98.sql',
           #   'q59.sql',
           # 'q65.sql',
           # 'ss_max.sql',
           ]
    query_dic = {
    'q19.sql':['date_dim_%s'%(db_type),
                'store_sales_%s'%(db_type),
                'item_%s'%(db_type),
                'customer_%s'%(db_type),
                'customer_address_%s'%(db_type)]
    }
    #user_number = range(2,4,2)
    #user_number = [2]
    #user_number = [5,7,9,11,13,15]
    user_number = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,40,50]
    duration_time = 10*60 #by second*
    desired_logs = {
                'remote':['yarn-nodemanager','impalad'],
                'local':['/var/log/impala-llama/llama-cmf-impala-LLAMA-tracing017.log.out']
            }
    #INSTALL PATH
    install_path = os.path.dirname(os.path.realpath(__file__))
    
    return locals()
