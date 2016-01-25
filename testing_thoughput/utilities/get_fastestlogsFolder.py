import os
root_dir = "/home/wangdewei/workspace/impala_pt/queries/logs/DATE2015-11-11_TIME16-33"
query_dir_list = {
        "2.7":[
      'q19.sql_1447230881',   
'q27.sql_1447231257',   
'q3.sql_1447232577',    
'q34.sql_1447232163',   
'q42.sql_1447232770',   
'q43.sql_1447232936',   
'q46.sql_1447233406',   
'q52.sql_1447233960',   
'q53.sql_1447234174',   
'q55.sql_1447234415',   
'q59.sql_1447237772',   
'q63.sql_1447234538',   
'q65.sql_1447239847',   
'q68.sql_1447234827',   
'q7.sql_1447235507',    
'q79.sql_1447235339',   
'q89.sql_1447236220',   
'q98.sql_1447236343',   
'ss_max.sql_1447242540',




            ],
        "2.4":[
  

            ],
        "2.1":[
 



            ],
        "1.8":[
 

            ],
        }
for key in query_dir_list.keys():
    fastest_logs_dir = os.path.join(root_dir,"fastest_logs",key)
    os.system("mkdir -p %s"%(fastest_logs_dir))
    for query_dir in query_dir_list[key]:
        query_log = os.path.join(root_dir,query_dir)
        os.system("cp -r %s %s"%(query_log,fastest_logs_dir))
