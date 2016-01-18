def local_config():
    tpcds_tool_root = "tpcds-kit"
    #SF 3000 means ~1TB
    tpcds_scale_factor = 300
    #flat file's path on hdfs
    flatfile_path_HDFS = "/user/root/tpcds-data-%s"%(tpcds_scale_factor)
    master = ['tracing017']
    impalad_nodes = ['tracing024',\
             'tracing025',\
             'tracing026',\
             'tracing027']
    work_dir = '/home/wangdewei/tpc_ds'
    dsdgen_threads_per_node = 36
    #[tbl_format,compresscode]
    #currently ,tbl_format support :text/parquet
    #           compresscode support :none/snappy
    db_format = ['parquet','none']
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
    partition_tbls = ['store_sales']
    #overwrite
    overwrite = True
    tbl_format_dic = {
                        'parquet':'parquetfile',
                        'text':'textfile'
                     }
    tbl_compress_codec_dic = {
                            'none':'none',
                            'snappy':'snappy'
                            }
    return locals()
