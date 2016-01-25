import os
import re
from excel_relate.excel_relate import excel_writer
from config import local_config_testing
file = "/home/pset/renderman.log"
def analyze():
    des_dic = {}
    with open(file,"r") as fp:
        for line in fp:
            #pattern = ".*cpu_freq:(\d+.\d+).*\[(\d+.\d+), (\d+.\d+), (\d+.\d+)\]s!time_stamp is (\d+)"
            pattern = ".*cpu_freq:(\d+.\d+).*(q\d+.sql).*\[(\d+.\d+), (\d+.\d+), (\d+.\d+)\]s!time_stamp is (\d+)"
            line_match = re.match(pattern,line)
            if line_match:
                cpu_freq,query_name,run_time,time_line,remote_start_time,time_stamp = line_match.groups()
                if query_name not in des_dic.keys():
                    des_dic[query_name] = {}
                if cpu_freq not in des_dic[query_name].keys():
                    des_dic[query_name][cpu_freq] = []
                des_dic[query_name][cpu_freq].append([float(run_time),
                                                        float(time_line),
                                                        float(remote_start_time),
                                                        time_stamp])
    return des_dic

def write_to_excel(des_dic,work_book):
    sheet_name = 'temp'
    work_book.create_sheet(sheet_name)
    work_book.set_cell(sheet_name,1,1,"rendermanlog")
    start_row = 3
    row = start_row
    for query in des_dic.keys():
        col = 2
        work_book.set_cell(sheet_name,col,row,query)
        col += 1
        for cpufreq in des_dic[query].keys():
            des_dic[query][cpufreq].sort()
            if row == start_row:
                work_book.set_cell(sheet_name,col,row - 2,cpufreq)
            for i in range(local_config_testing()["every_query_times"]):
                if row == start_row:
                    work_book.set_cell(sheet_name,col,row - 1,'TIME_LINE')
                    work_book.set_cell(sheet_name,col+1,row - 1,'Remote_Start_Time')
                    work_book.set_cell(sheet_name,col+2,row - 1,'Query_Run_Time')
                    work_book.set_cell(sheet_name,col+3,row - 1,'Time_Stamp')
                if i >= len(des_dic[query][cpufreq]):
                    print("query:%s with cpu_freq(%s) has not been done,please retest it!"%(query,cpufreq))
                    break
                query_run_time,time_line,remote_start_time,time_stamp = des_dic[query][cpufreq][i]
                work_book.set_cell(sheet_name,col,row,time_line)
                col += 1
                work_book.set_cell(sheet_name,col,row,remote_start_time)
                col += 1
                work_book.set_cell(sheet_name,col,row,query_run_time)
                col += 1
                work_book.set_cell(sheet_name,col,row,time_stamp)
                col += 1
        row += 1
if __name__ == "__main__":
    ex_wb = excel_writer('temp_rendermanlog.xlsx')
    des_dic = analyze()
    write_to_excel(des_dic,ex_wb)
    ex_wb.save()
