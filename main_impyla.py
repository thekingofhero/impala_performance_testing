from config import local_config
from excel_relate.excel_relate import excel_writer
import testing
import datetime
import logging
import os
import time

def main():
   from impala.dbapi import connect
   time_s = time.time()
   for query in local_config()['query_list']:
       with open(os.path.join(local_config()['query_dir'],query),'r') as fp:
           conn = connect(host='172.168.0.24', port=21050,database = '%s'%(local_config()['DATABASE_NAME']))
           cursor = conn.cursor()
           os.system("""
              echo "clear OS cache on tracing017"
              free && sync && echo 3 >/proc/sys/vm/drop_caches && free

              echo "clear OS cache on tracing024"
              ssh tracing024 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

              echo "clear OS cache on tracing025"
              ssh tracing025 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

              echo "clear OS cache on tracing026"
              ssh tracing026 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

              echo "clear OS cache on tracing027"
              ssh tracing027 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

              """)
           time1 = time.time()
           sql= fp.read().strip('profile;\n')
           sql = sql.strip('; ')
           #print sql 
           try:
            cursor.execute('%s'%(sql))
           except:
               cursor.close()
               conn.close()
               print query,"wrong"
               continue
           time2 = time.time()
           while True:
               row=cursor.fetchone()
               if row:
                  # row
                  print(row)
               else:
                   break
           time3 = time.time()
           print "res:",query,time2 - time1,time3 - time1
           cursor.close()
           conn.close()
   #print time_s,time1,time2,time3
if __name__ == '__main__':
    main()
