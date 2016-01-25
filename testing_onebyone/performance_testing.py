from config import local_config_testing
from excel_relate.excel_relate import excel_writer
import testing
import datetime
import logging
import os


def testing_onebyone():
    datetime_now = str(datetime.datetime.now().strftime("DATE%Y-%m-%d_TIME%H-%M"))
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
