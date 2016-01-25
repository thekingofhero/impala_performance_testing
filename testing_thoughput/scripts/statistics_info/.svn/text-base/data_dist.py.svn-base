from scripts.utility import *
import sys
import os
import string
import csv
import re

server_idx_map = {'tracing024':0, 
				  'tracing025':1,
				  'tracing026':2,
				  'tracing027':3}

def collect_data_dist_info(profile_path, output_file_path, output_dir_path="."):

	if os.path.exists(output_file_path):
		os.remove(output_file_path)

	average_fragment_metrics_skipped = False
	within_scan_hdfs_entry = False
	within_plan_fragment_entries = False
	
	node_id = 0
	
	data_dist = []
	records = []
	server_id = 0
	per_node_data_size_in_mb = 0
	scanner_concurrency = 0
	with open(output_file_path, 'w', newline='') as csv_file:
		csv_writer = csv.writer(csv_file, delimiter=',')

		for line in open(profile_path):
			stripped_line = line.strip();

			if stripped_line.startswith("Estimated Per-Host Requirements:"):
				within_plan_fragment_entries = True
			elif stripped_line.startswith("Estimated Per-Host Mem:"):
				within_plan_fragment_entries = False				
			elif stripped_line.startswith("Instance"):
				lft_idx = stripped_line.find("(host=")
				rgh_idx = stripped_line.find(":22000):(")
				host_name = stripped_line[lft_idx+6:rgh_idx]
				server_id = server_idx_map[host_name]
				average_fragment_metrics_skipped = True
			elif stripped_line.startswith("HDFS_SCAN_NODE (id=") and average_fragment_metrics_skipped:
				curr_id = get_exec_node_id(stripped_line)
				within_scan_hdfs_entry = True
			elif average_fragment_metrics_skipped and within_scan_hdfs_entry:

				if stripped_line.startswith("Hdfs split stats"):					
					idx = stripped_line.find(">):")
					stripped_line = stripped_line[idx+4:].strip()
				
					while len(stripped_line) > 0:
						p = re.compile('\d+:\d+/\d+\.\d+ ((GB)|(MB)|(KB)|B)')
						m = p.search(stripped_line)

						start_index = m.span()[0]
						end_index = m.span()[1]

						disk_dist_info = stripped_line[start_index:end_index]
						
						idx = disk_dist_info.find(':')
						volume = disk_dist_info[0:idx]
						disk_dist_info = disk_dist_info[idx+1:]

						idx = disk_dist_info.find('/')
						num_splits = disk_dist_info[0:idx]
						disk_dist_info = disk_dist_info[idx+1:]
						
						data_size_in_mb = get_size_in_mb(disk_dist_info)
						per_node_data_size_in_mb += data_size_in_mb

						stripped_line = stripped_line[end_index:].strip()

						record = [server_id, int(curr_id), int(volume), int(num_splits), "{0:.8f}".format(data_size_in_mb)]
						
						records.append(record)
						data_dist.append(record)

				else:
					key = get_label(stripped_line)
					
					if key == 'TotalReadThroughput':
						within_scan_hdfs_entry = False
					elif key == 'BytesRead':
						bytes_read = get_count(stripped_line)			
						
						ratio = float(bytes_read)/1024.0/1024.0/per_node_data_size_in_mb		

						for record in records:								
							record.append("{0:.8f}".format(float(record[len(record)-1])*ratio))
						per_node_data_size_in_mb = 0
						
						csv_writer.writerows(records)
						
						records = []
	return data_dist
	
def calculate_scan_range_boundary_ratio(profile_path, output_file_path, output_dir_path=".", data_dist=[]):
	import sys
	global server_idx_map

	if os.path.exists(output_file_path):
		os.remove(output_file_path)

	print("processing file {0}".format(profile_path))
	server_idx = 0
	
	per_server_num_splits_map = {}	
	records = []

	for dd in data_dist:
		
		sid = str(dd[0])
		nid = str(dd[1])
		num_splits = float(dd[3])

		if sid not in per_server_num_splits_map.keys():
			per_server_num_splits_map[sid] = {}
		if nid not in per_server_num_splits_map[sid].keys():
			per_server_num_splits_map[sid][nid] = 0.0
		
		per_server_num_splits_map[sid][nid] += num_splits

	per_scan_node_stats = {}
	per_node_server_idx_min = {}
	per_node_server_idx_max = {}
	
	dist_factor = {}
	
	for sid in per_server_num_splits_map.keys():

		nid_count = {}
		for nid in per_server_num_splits_map[sid].keys():

			if nid not in nid_count.keys():
				nid_count[nid] = 1

			if nid not in per_scan_node_stats.keys():
				per_scan_node_stats[nid] = {}
				per_scan_node_stats[nid]['max'] = 0.0
				per_scan_node_stats[nid]['min'] = sys.maxsize
				per_scan_node_stats[nid]['avg'] = 0.0

			if per_scan_node_stats[nid]['max'] < per_server_num_splits_map[sid][nid]:
				per_scan_node_stats[nid]['max'] = per_server_num_splits_map[sid][nid]
				per_node_server_idx_max[nid] = sid
			
			if per_scan_node_stats[nid]['min'] > per_server_num_splits_map[sid][nid]:
				per_scan_node_stats[nid]['min'] = per_server_num_splits_map[sid][nid]
				per_node_server_idx_min[nid] = sid
				
			per_scan_node_stats[nid]['avg'] += per_server_num_splits_map[sid][nid]
		
		for nid in nid_count.keys():
			if nid not in dist_factor.keys():
				dist_factor[nid] = 0
			dist_factor[nid] += 1

		
	for nid in per_scan_node_stats.keys():
		per_scan_node_stats[nid]['avg'] /= dist_factor[nid]
		
	with open(output_file_path, 'w', newline='') as csv_file:
		csv_writer = csv.writer(csv_file, delimiter=',')

		for nid in per_scan_node_stats.keys():
			upper_boundary_server_idx = per_node_server_idx_max[nid]
			lower_boundary_server_idx = per_node_server_idx_min[nid]

			upper_boundary_scan_range_ratio = per_scan_node_stats[nid]['max'] / per_scan_node_stats[nid]['avg']
			lower_boundary_scan_range_ratio = per_scan_node_stats[nid]['min'] / per_scan_node_stats[nid]['avg']

			records.append(['SCAN_RANGE_UPPER_BOUNDARY_RATIO', nid, "{0:.8f}".format(upper_boundary_scan_range_ratio)])
			records.append(['SCAN_RANGE_LOWER_BOUNDARY_RATIO', nid, "{0:.8f}".format(lower_boundary_scan_range_ratio)])

			volume_splits_map = {}
			volume_data_size_map = {}
			for dd in data_dist:

				dd_sid = str(dd[0])
				dd_nid = str(dd[1])
				dd_vid = int(dd[2])				
				num_splits = int(dd[3])
				data_size = float(dd[4])

				if dd_sid == upper_boundary_server_idx and dd_nid == nid:
					if dd_vid not in volume_splits_map.keys():
						volume_splits_map[dd_vid] = 0
						volume_data_size_map[dd_vid] = 0
					volume_splits_map[dd_vid] += num_splits
					volume_data_size_map[dd_vid] = data_size

			max_splits = 0
			min_splits = sys.maxsize
			avg = 0

			for key in volume_splits_map:
				max_splits = max_splits if max_splits > volume_splits_map[key] else volume_splits_map[key]
				min_splits = min_splits if min_splits < volume_splits_map[key] else volume_splits_map[key]
				avg += volume_splits_map[key]
			avg /= len(volume_splits_map.keys())

			upper_boundary_disk_splits_ratio = max_splits / avg
			lower_boundary_disk_splits_ratio = min_splits / avg

			records.append(['DISK_SPLITS_BOUNDARY_RATIO', nid, "{0:.8f}".format(upper_boundary_disk_splits_ratio), "{0:.8f}".format(lower_boundary_disk_splits_ratio)])

			max_data_size = 0.0
			min_data_size = sys.maxsize
			avg_data_size = 0.0
			for current_data_size in volume_data_size_map.values():
				max_data_size = max_data_size if max_data_size > current_data_size else current_data_size
				min_data_size = min_data_size if min_data_size < current_data_size else current_data_size
				avg_data_size += current_data_size
			
			upper_boundary_data_size_ratio = max_data_size / avg_data_size
			lower_boundary_data_size_ratio = min_data_size / avg_data_size
			
			records.append(['DATA_SIZE_BOUNDARY_RATIO', nid, "{0:.8f}".format(upper_boundary_data_size_ratio), "{0:.8f}".format(lower_boundary_data_size_ratio)])

		records.sort()
		csv_writer.writerows(records)

if __name__ == '__main__':

	import platform
	print(platform.python_version())

	query_name = 'q68.sql.log'
	input_path = os.path.join(r'W:\junliu\Benchmark\Impala\Baseline (Parquet)\profiles', query_name)
	output_dir_path = r'C:\Development\IntelCoFluentStudio_v5.0.1\workspace\Impala\ImpalaFrameworkDebug'

	if not os.path.exists(output_dir_path):
		print('output directory path {0}'.format(output_dir_path))
		os.makedirs(output_dir_path)

	file_list = []

	if os.path.isfile(input_path):
		file_splits = os.path.split(input_path)
		filename = file_splits[1]
		data_dist = collect_data_dist_info(input_path, os.path.join(output_dir_path, get_output_name(filename, "dist")), output_dir_path)
		calculate_scan_range_boundary_ratio(input_path, os.path.join(output_dir_path, get_output_name(filename, "distest")), output_dir_path, data_dist)
	elif os.path.exists(input_path):
		for subdir, dirs, files in os.walk(input_path):
			for file in files:				
				if file.endswith("sql.log"):
					# print('subdir: {0}, filename: {1}'.format(subdir, file))
					file_list.append(os.path.join(subdir, file))		
		
		file_list.sort()

		for input_file in file_list:			
			# print("----->",input_file)
			data_dist = collect_data_dist_info(input_file, os.path.join(output_dir_path, get_output_name(input_file, "dist")), output_dir_path)
			calculate_scan_range_boundary_ratio(input_file, os.path.join(output_dir_path, get_output_name(input_file, "distest")), output_dir_path, data_dist)

