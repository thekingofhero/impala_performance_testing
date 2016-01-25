from config import local_config
from excel_relate.excel_relate import excel_writer
import testing
import datetime
import logging
import os

def user(user_name,baselog_dir,logging):
    #ex_wb = excel_writer(datetime_now + '.xlsx')
    log_dir = os.path.join(baselog_dir,str(user_name))
    if not os.path.isdir(log_dir):
        os.system("mkdir -p %s"%(log_dir))

    for test_case in local_config()['test_case_list']:
        if test_case not in testing.TestingClassDict.keys():
            print testing.TestingClassDict
            print '%s has not been completed...'%(test_case)
        else:
            test_obj = testing.TestingClassDict[test_case](logging,log_dir,user_name)
            test_obj.run_testing()
            #test_obj.get_fastest()
            #test_obj.write_to_excel(ex_wb)
    #ex_wb.save()
    
if __name__ == '__main__':
    distinct_user()
