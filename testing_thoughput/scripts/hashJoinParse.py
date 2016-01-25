import os
import re
import sys
import string
import csv

from scripts.utility import *
from statistics_info.scan_stats import collect_scanner_stats_info as scan_cs


user_list = [1, ]

timestamp = "DATE2016-01-13_TIME14-15"
log_path_prex = os.path.join("/home/wangdewei/workspace/impala_ptOnYarn/queries/logs", timestamp)

def totalProfileNum(path):
	#read from path+RESULT 
	result_file_path = os.path.join(path,"RESULT")
	total_number = 0

	with open(result_file_path,'rb') as result_file:
		result_container = csv.reader(result_file)
		for row in result_container:
			total_number = int(row[2])
	        #TODO: parse the time for every query
	return total_number

def HashJoinParse(profile_path, profile_name):


	return scan_cs(os.path.join(profile_path,profile_name))
	




def main():
	for user_num in user_list:
		for user in range(user_num):
			profile_path = os.path.join(log_path_prex,"user_number="+str(user_num),str(user))
			total_profile_num = totalProfileNum(profile_path)
			print total_profile_num

			output_file = "output.csv"

			with open(output_file,'wb') as output_csv:
				csv_writer = csv.writer(output_csv)
				for profile_name in range(total_profile_num):
					output_info = HashJoinParse(profile_path, str(profile_name+1)+".log")
					csv_writer.writerow(output_info)


	

if __name__ == '__main__':
	main()
