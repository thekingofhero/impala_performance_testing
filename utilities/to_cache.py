from config import local_config
import os
dbname = local_config()['DATABASE_NAME']
db_type = local_config()['db_type']
tbl_list = [
            'customer_address_%s'%(db_type),
            'customer_demographics_%s'%(db_type),
            'customer_%s'%(db_type),
            'date_dim_%s'%(db_type),
            'household_demographics_%s'%(db_type),
            'item_%s'%(db_type),
            'promotion_%s'%(db_type),
            'store_%s'%(db_type),
            'store_sales_%s'%(db_type),
            'time_dim_%s'%(db_type)
]

def to_cache():
    for tbl_name in tbl_list:
        cmd = """
                impala-shell -i tracing024:21000 -d %s -q "alter table %s set cached     in 'testpool'"
            """%(dbname,tbl_name)
        os.system(cmd)

def un_cache():
    for tbl_name in tbl_list:
        cmd = """
                impala-shell -i tracing024:21000 -d %s -q "alter table %s set uncached"
            """%(dbname,tbl_name)
        os.system(cmd)
 
if __name__ == '__main__':
    to_cache()
    #un_cache()
