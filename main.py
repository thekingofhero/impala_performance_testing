from data_generator.data_gen import data_gen
from utilities.postgres import mydb
from testing_onebyone.performance_testing import testing_onebyone
from config import postgres_db_connector

def create_admin_tbl():
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
    mydb1.close()

def get_switch():
    with open('./switch.conf','r') as fp:
        for line in fp:
            if line.startswith("switch_data_gen"):
                  sw_data_gen = str(True) == line.split("=")[1].strip()

            if line.startswith("switch_pt_onebyone"):
                  sw_pt_onebyone = str(True) == line.split("=")[1].strip()

    return locals()

def main():
    #1.create admin control tbl on RDBMS
    create_admin_tbl()
    #2.which to run
    switch_dic = get_switch()
    if switch_dic['sw_data_gen']:
        data_gen()
    if switch_dic['sw_pt_onebyone']:
        testing_onebyone()

if __name__ == '__main__':
    main()
