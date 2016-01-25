import os
global_config={
        'tpcds_scale_factor':1,
        'master':['tracing021'],
        'impalad_nodes':['tracing021'],
        'db_format' :['parquet','none'],
        'cur_path' : os.path.dirname(os.path.realpath(__file__))
        }
postgres_db_connector = {
                          'db':'IMPALA_PERFORMANCE_TESTING',
                          'port':5432,
                          'host':'10.239.44.28',
                          'user':'postgres',
                          'pwd':'123456'
                          }

#configure for generating data
def local_config_gen():
    global global_config
    tpcds_tool_root = "data_generator/tpcds-kit"
    #SF 3000 means ~1TB
    tpcds_scale_factor = global_config['tpcds_scale_factor']
    #flat file's path on hdfs
    flatfile_path_HDFS = "/user/root/tpcds-data-%s"%(tpcds_scale_factor)
    master = global_config['master']
    impalad_nodes = global_config['impalad_nodes']
    work_dir = '/home/wangdewei/tpc_ds'
    dsdgen_threads_per_node = 24
    #[tbl_format,compresscode]
    #currently ,tbl_format support :text/parquet
    #           compresscode support :none/snappy
    db_format = global_config['db_format']
    db_name = 'tpcds_%s_%s_%d'%(db_format[0],db_format[1],tpcds_scale_factor)
    #key:flat file folder name
    #value:[external_tbl]
    tpcds_tbls = {
        'date_dim':['et_date_dim'],
        'time_dim':['et_time_dim'],
        'customer':['et_customer'],
        'customer_address':['et_customer_address'],
        'customer_demographics':['et_customer_demographics'],
        'household_demographics':['et_household_demographics'],
        'item':['et_item'],
        'promotion':['et_promotion'],
        'store':['et_store'],
        'store_sales':['et_store_sales'],
        'inventory':['et_inventory'],
    }
    #tbl doesn't use
    bind_tbl = ['inventory']
    partition_tbls = ['store_sales']
    #overwrite
    overwrite = False
    tbl_format_dic = {
                        'parquet':'parquetfile',
                        'text':'textfile'
                     }
    tbl_compress_codec_dic = {
                            'none':'none',
                            'snappy':'snappy'
                            }
    cur_path = global_config['cur_path']
    return locals()

#configure for testing
def local_config_testing():
    global global_config
    #HDFS DATANODE
    impalad_nodes = global_config['impalad_nodes'] 
    db_format = global_config['db_format']
    tpcds_scale_factor =  global_config['tpcds_scale_factor']
    DATABASE_NAME = 'tpcds_%s_%s_%d'%(db_format[0],db_format[1],tpcds_scale_factor)
    #IMPALA CONFIG
    IMPALA_SERVER = impalad_nodes[0]

    #if use nmon ,set sys_log='all';else 'profile'
    sys_log = 'all'
    #TESTING CASE
    test_case_list = [
        'SF_NO_CACHE',
        #'SF_CACHE',
    ]
    #HARD WARE
    #cpu
    #CPUFreq_range = [1.8,2.1,2.4,2.7]
    CPUFreq_range = [2.1]
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
    query_dir = './queries/'
    query_list = [
             'q19.sql',
    #        'q27.sql',
    #         'q34.sql',
    #          'q3.sql' ,
    #         'q42.sql',
    #         'q43.sql',
    #        'q46.sql',
    #         'q52.sql',
    #         'q53.sql',
    #         'q55.sql',
    #         'q63.sql',
    #        'q68.sql',
    #        'q79.sql',
    #         'q7.sql' ,
    #         'q89.sql',
    #        'q98.sql',
    #          'q59.sql',
    #        'q65.sql',
    #        'ss_max.sql',
            ]
    every_query_times = 1
    #INSTALL PATH
    cur_path = global_config['cur_path']
    
    return locals()
