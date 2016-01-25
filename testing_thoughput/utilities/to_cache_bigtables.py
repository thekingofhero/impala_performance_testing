from config import local_config
import os
dbname = local_config()['DATABASE_NAME']
db_type = local_config()['db_type']
cache_dic = {'q43.sql':[2450816,2451179 ],
'q27.sql':[2450815,2451179 ],
'q65.sql':[2451911,2452275 ],
'q52.sql':[2451149,2451179 ],
'q19.sql':[2451484,2451513 ],
'q98.sql':[2451911,2451941 ],
'q63.sql':[2451911,2452275 ],
'q89.sql':[2451545,2451910 ],
'q79.sql':[2451180,2451269 ],
'q68.sql':[2451180,2451269 ],
'q42.sql':[2451149,2451179 ],
'q73.sql':[2451180,2451269 ],
'q53.sql':[2451911,2452275 ],
'q55.sql':[2452245,2452275 ],
'q7.sql' :[2450815,2451179 ],
'q3.sql' :[2451149,2451179],
'q34.sql':[2450816,2451910 ]}

def to_cache(query_id):
    range_ = cache_dic[query_id]
    for range_i in range(range_[0],range_[1]):
        cmd = """
            impala-shell -i tracing024:21000 -d %s -q "alter table store_sales_%s partition(ss_sold_date_sk = %d) set cached     in 'testpool'"
            """%(dbname,db_type,int(range_i))
        os.system(cmd)

def un_cache():
    cmd = """
        impala-shell -i tracing024:21000 -d %s -q "alter table store_sales_%s set uncached;"
        """%(dbname,db_type)
    os.system(cmd)
    os.system('impala-shell -i tracing024:21000 -d %s -q "compute stats store_sales_%s"'%(dbname,db_type))
 
if __name__ == '__main__':
    #to_cache()
    un_cache()
