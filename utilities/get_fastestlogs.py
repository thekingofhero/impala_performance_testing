import os
root_dir = "/home/wangdewei/workspace/impala_pt/queries/logs/DATE2015-10-22_TIME13-15/"
query_dir_list = [
        'q19.sql_1445308433',
        'q27.sql_1445311561',
        'q3.sql_1445319651',
        'q34.sql_1445315930',
        'q42.sql_1445323586',
        'q43.sql_1445328069',
        'q46.sql_1445331531',
        'q52.sql_1445335092',
        'q53.sql_1445339585',
        'q55.sql_1445343305',
        'q59.sql_1445499385',
        'q63.sql_1445346282',
        'q65.sql_1445504506',
        'q68.sql_1445349307',
        'q7.sql_1445491788',
        'q79.sql_1445352177',
        'q89.sql_1445494451',
        'q98.sql_1445496647',
        'ss_max.sql_1445514654',
        ]
fastest_logs_dir = os.path.join(root_dir,"fastest_logs")
os.system("mkdir -p %s"%(fastest_logs_dir))
for query_dir in query_dir_list:
    query_log = os.path.join(root_dir,query_dir,"profiles",query_dir.split("_")[0] + ".log")
    os.system("cp %s %s"%(query_log,fastest_logs_dir))
