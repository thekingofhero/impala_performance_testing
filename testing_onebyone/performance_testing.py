from config import local_config_testing,local_config_gen,postgres_db_connector,global_config
from excel_relate.excel_relate import excel_writer
import testing
import datetime
import logging
import os
from utilities.postgres import mydb

def create_admin_tbl(datetime_now):
    mydb1 = mydb(postgres_db_connector)
    #create admin tbl
    sql_create = """
            CREATE TABLE IF NOT EXISTS admin_control
            (
                id serial,
                tbl_name text,
                scale_factor integer,
                db_format text,
                database_name text,
                test_case text,
                global_config text,
                local_config_gen text,
                local_config_testing text,
                comment text, --CDH/impala version .etc
                constraint admin_contorl_pkey primary key (id)
            )
    """
    mydb1.runsql(sql_create)
    mydb1.commit()
    #insert admin tbl
    sql_insert = """
            insert into admin_control(tbl_name,scale_factor,db_format,database_name,test_case,global_config,local_config_gen,local_config_testing)
            values('%s',%d,'%s','%s','%s','%s','%s','%s')
    """%(datetime_now.replace('-','_'),
            local_config_testing()['tpcds_scale_factor'],
            str(local_config_testing()['db_format']).replace("'","''"),
            local_config_testing()['DATABASE_NAME'],
            '|'.join(local_config_testing()['test_case']),
          str(global_config).replace("'","''"),
          str(local_config_gen()).replace("'","''"),
          str(local_config_testing()).replace("'","''"))
    mydb1.runsql(sql_insert)
    mydb1.commit()
    mydb1.close()


def testing_onebyone():
    datetime_now = str(datetime.datetime.now().strftime("DATE%Y-%m-%d_TIME%H-%M"))
    create_admin_tbl(datetime_now)
    log_dir = os.path.join(local_config_testing()['cur_path'],'testing_result',datetime_now)
    if os.path.isdir(log_dir) == False:
        os.system('mkdir -p %s'%(log_dir))
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='%s/renderman.log'%(log_dir),
                    filemode='w')

    ex_wb = excel_writer(log_dir,datetime_now + '.xlsx')
    for test_case in local_config_testing()['test_case_list']:
        if test_case not in testing.TestingClassDict.keys():
            print '%s has not been completed...'%(test_case)
        else:
            print '%s has been completed...TIME:%s'%(test_case,datetime_now)
            test_obj = testing.TestingClassDict[test_case](logging,datetime_now)
            test_obj.run_testing()
            test_obj.create_tbl()
            test_obj.get_fastest()
            test_obj.write_to_excel(ex_wb)
    ex_wb.save()
    
if __name__ == '__main__':
    testing_onebyone()
