import sys
import os
import csv

from scripts.utility import *
def collect_statistics(profile_path, output_file_path, output_dir_path="."):
	
	print('calculating aggregation costs for file {0}'.format(profile_path))

	keys = ['Node Id',                  # index 0
			'Row Size (Bytes)',         # index 1			
			'Row Size (Build)',         # index 2
			'BuildTime',                # index 3
			'GetNewBlockTime',          # index 4
			'GetResultsTime',           # index 5
			'HashBuckets',              # index 6
			'LargestPartitionPercent',  # index 7
			'MaxPartitionLevel',        # index 8
			'NumRepartitions',          # index 9
			'PartitionsCreated',        # index 10
			'PeakMemoryUsage',          # index 11
			'PinTime',                  # index 12
			'RowsRepartitioned',        # index 13
			'RowsReturned',             # index 14
			'RowsReturnedRate',         # index 15
			'SpilledPartitions',        # index 16			
			'UnpinTime',                # index 17
			'Row Count (Build)',        # index 18
			'Rows Per NS',              # index 19
			'Bytes Per NS'              # index 20
	]

	func_map = {}
	func_map['BuildTime'] = get_time
	func_map['GetNewBlockTime'] = get_time
	func_map['GetResultsTime'] = get_time
	func_map['HashBuckets'] = get_count
	func_map['LargestPartitionPercent'] = get_count
	func_map['MaxPartitionLevel'] = get_count
	func_map['NumRepartitions'] = get_count
	func_map['PartitionsCreated'] = get_count
	func_map['PeakMemoryUsage'] = get_count
	func_map['PinTime'] = get_time
	func_map['RowsRepartitioned'] = get_count
	func_map['RowsReturned'] = get_count
	func_map['RowsReturnedRate'] = get_row_return_rate
	func_map['SpilledPartitions'] = get_count
	func_map['UnpinTime'] = get_time

	record = []
	prev_record = []
	row_size_map = {}
	build_row_size_map = {}
	
	within_plan_fragment_entries = False
	average_fragment_metrics_skipped = False
	within_aggregate_plan_node = False
	within_aggregate_entry = False
	last_entry_processed = False
	within_coordinator_fragment = False
	has_get_new_block_time = False

	is_merge_aggr = False

	node_id = 0
	prev_id = 0
	
	geomean_rows_per_ns = 1.0

	find_build_row_size = False
	find_rows_returned = False

	rows_per_ns_per_aggregate_node = {}

	for line in open(profile_path):
		stripped_line = line.strip()
		
		if stripped_line.startswith("Estimated Per-Host Requirements:"):
			within_plan_fragment_entries = True
		elif stripped_line.startswith("Estimated Per-Host Mem:"):
			within_plan_fragment_entries = False
		elif stripped_line.startswith("Instance") and not average_fragment_metrics_skipped:
			average_fragment_metrics_skipped = True
		elif stripped_line.startswith("Coordinator Fragment"):
			within_coordinator_fragment = True					
		elif stripped_line.startswith("Averaged Fragment"):
			within_coordinator_fragment = False
		elif stripped_line.find(":AGGREGATE") != -1 and within_plan_fragment_entries:				
			node_id = -1
			rgh_idx = stripped_line.find(":AGGREGATE")				
			lft_idx = 0
			if stripped_line.startswith('0'):
				lft_idx = 1
			node_id = stripped_line[lft_idx:rgh_idx]
			
			within_aggregate_plan_node = True
		elif within_aggregate_plan_node and stripped_line.find("row-size") != -1:
			lft_idx = stripped_line.find("row-size=")
			rgh_idx = stripped_line.find("B cardinality")
			row_size_map[node_id] = stripped_line[lft_idx+9:rgh_idx]

			if find_build_row_size:
				build_row_size_map[prev_id] = row_size_map[node_id]
				prev_id = -1

			prev_id = node_id
			within_aggregate_plan_node = False
			find_build_row_size = True
		elif find_build_row_size and stripped_line.find("row-size") != -1:
			find_build_row_size = False
			lft_idx = stripped_line.find("row-size=")
			rgh_idx = stripped_line.find("B cardinality")				
			build_row_size_map[node_id] = stripped_line[lft_idx+9:rgh_idx]	
		elif find_rows_returned and stripped_line.find("RowsReturned:") != -1:
			find_rows_returned = False

			# for this case, we have already encountered an aggregation node
			if len(prev_record) > 0:		
				curr_id = prev_record[0]
				tmp = record
				record = prev_record
				prev_record = tmp				
				find_rows_returned = True
			
			record.append(get_count(stripped_line))

			rows_per_ns = float(record[18]) / float(record[3])
			record.append(rows_per_ns)

			if curr_id not in rows_per_ns_per_aggregate_node.keys():
				rows_per_ns_per_aggregate_node[curr_id] = []
			rows_per_ns_per_aggregate_node[curr_id].append(rows_per_ns)

			if find_rows_returned:
				record = prev_record
				prev_record = []
				curr_id = record[0]
				find_rows_returned = False					
				record.append(get_count(stripped_line))	
				
			else:
				record = []

		elif stripped_line.startswith("AGGREGATION_NODE (id=") and (within_coordinator_fragment or average_fragment_metrics_skipped):	
			# +1 aggregation node
			if find_rows_returned:
				prev_record = record
				record = []
			
			curr_id = get_exec_node_id(stripped_line)
			
			record.append(curr_id)
			record.append(row_size_map[curr_id])
			record.append(build_row_size_map[curr_id])
			within_aggregate_entry = True				
		elif (within_coordinator_fragment or average_fragment_metrics_skipped) and within_aggregate_entry:
			
			key = get_label(stripped_line)
			if key != '' and key in func_map:
				record.append(func_map[key](stripped_line))
				
				if (key == 'PeakMemoryUsage' or key == 'SpilledPartitions' or key == 'GetResultsTime') and not has_get_new_block_time:
					record.append(0)

				if key == 'UnpinTime' or (key == 'SpilledPartitions' and not has_get_new_block_time):
					within_aggregate_entry = False
					find_rows_returned = True	
					has_get_new_block_time = False
				elif key == 'GetNewBlockTime':
					has_get_new_block_time = True
					
	# end for

	with open(output_file_path, 'a', newline='') as csv_file:		
		csv_writer = csv.writer(csv_file, delimiter=',')
		import operator
		sorted_rows_per_ns_per_aggregate_node = sorted(rows_per_ns_per_aggregate_node.items(), key=operator.itemgetter(0))
		for item in sorted_rows_per_ns_per_aggregate_node:
			csv_writer.writerow(['AGGREGATE_BUILD_COST', item[0], '{0:.8f}'.format(average(item[1]))])	
	
	global produce_combined_csv
	if 'produce_combined_csv' not in globals():		
		produce_combined_csv = False
	global csv_global_writer
	if produce_combined_csv:
		global start_execution_timestamp
		with open(os.path.join(output_dir_path, 'aggregation-all-' + start_execution_timestamp + '.csv'), 'a', newline='') as global_csv_file:
			csv_global_writer = csv.writer(global_csv_file, delimiter=',') 
			import operator
			sorted_rows_per_ns_per_aggregate_node = sorted(rows_per_ns_per_aggregate_node.items(), key=operator.itemgetter(0))
			for item in sorted_rows_per_ns_per_aggregate_node:
				csv_global_writer.writerow([os.path.basename(output_file_path), 'AGGREGATE_BUILD_COST', item[0], '{0:.8f}'.format(average(item[1]))])	

if __name__ == '__main__':
	import shutil
	import platform
	print(platform.python_version())

	global produce_combined_csv
	produce_combined_csv = True

	global start_execution_timestamp
	
	query_name = ''
	for task in ['']:		
		start_execution_timestamp = str(current_milli_time())

		path_prefix = r'W:\junliu\Benchmark\Impala\Baseline (Parquet)\profiles'
		profile = os.path.join(path_prefix, task, query_name)
		output = os.path.join(path_prefix, 'aggregation', task)
		
		if os.path.exists(output):		
			shutil.rmtree(output)
			print('remove folder: %s, %s' % (output, os.path.exists(output)))
		sys.argv = ['aggr_metrics.py', profile]

		if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
			print("usage: aggr_metrics.py <impala profile file path or directory>")
			sys.exit()

		handle_profiles(collect_statistics, os.path.normpath(profile), os.path.normpath(output))
		
		print('finish {0}'.format(task))
	