
def handle_profiles(fn_ref, input_path=".", output_dir_path=".", output_file_type="csv"):
	import os
	if not os.path.exists(output_dir_path):
		os.makedirs(output_dir_path)

	if os.path.isfile(input_path):
		file_splits = os.path.split(input_path)
		filename = file_splits[1]
		output_file_path = os.path.join(output_dir_path, get_output_name(filename, output_file_type))
		fn_ref(input_path, output_file_path, output_dir_path);
	elif os.path.exists(input_path):
		for subdir, dirs, files in os.walk(input_path):
			for file in files:				
				if file.endswith(".log"):
					fn_ref(os.path.join(subdir, file), os.path.join(output_dir_path, get_output_name(file, output_file_type)), output_dir_path);

def get_output_name(profile_name, output_file_type):
	import time
	#millis = int(round(time.time() * 1000))
	#output_file_name = profile_name + '_' + str(millis) + '.' + output_file_type
	profile_name = profile_name.replace(".sql.log", "")
	output_file_name = profile_name + '.' + output_file_type
	return output_file_name

def get_time_by_unit(number, unit):
	if unit == 'us':
		return number * 1000
	elif unit == 'ms':
		return number * 1000000
	elif unit == 'ns':
		return number
	elif unit == 'm':
		return number * 1000000000 * 60
	else: #second
		return number * 1000000000

def get_time(stripped_line):	
	
	tmp_idx = stripped_line.find(': ')
	tmp = stripped_line[tmp_idx+2:].strip()
	
	time = 0
	number = ''
	
	for index in range(len(tmp)):
		
		if (tmp[index] == 's' and not tmp[index-1].isdigit()):
			continue

		if tmp[index].isdigit() or tmp[index] == '.':
			number += tmp[index]
			if number.startswith('0') and len(number) > 1:
				number = number[1:]
		elif tmp[index] == 'u' and tmp[index+1] == 's':
			time += get_time_by_unit(float(number), 'us')
			number = ''
		elif tmp[index] == 'n' and tmp[index+1] == 's':
			time += get_time_by_unit(float(number), 'ns')
			number = ''
		elif tmp[index] == 'm' and (index+1 == len(tmp) or tmp[index+1] != 's'):
		 	time += get_time_by_unit(float(number), 'm')
		 	number = ''
		elif tmp[index] == 'm' and tmp[index+1] == 's':
			time += get_time_by_unit(float(number), 'ms')
			number = ''		
		elif tmp[index] == 's' and (index+1 == len(tmp) or tmp[index+1] != 's'):
			time += get_time_by_unit(float(number), 's')		
			number = ''	
	return str(time)

def get_count(stripped_line):
	tmp_lb_idx = stripped_line.find('(')
	if tmp_lb_idx > 0:
		tmp_rb_idx = stripped_line.find(')')					
		tmp = stripped_line[tmp_lb_idx+1:tmp_rb_idx]					
	else:
		tmp_lb_idx = stripped_line.find(': ')
		tmp = stripped_line[tmp_lb_idx+2:]
	return tmp

def get_row_return_rate(stripped_line):
	tmp_lb_idx = stripped_line.find(': ')
	tmp = stripped_line[tmp_lb_idx+2:]
	if tmp == '0':
		return tmp

	splits = tmp.split("/")
	rate_str = splits[0].strip()
	time_unit = splits[1].strip()
	
	row_per_sec = 0.0
	if rate_str.endswith('K'):
		row_per_sec = float(rate_str.replace('K', '').strip()) * 1000
	elif rate_str.endswith('M'):
		row_per_sec = float(rate_str.replace('M', '').strip()) * 1000000
	else:
		row_per_sec = float(rate_str.strip())

	return str(row_per_sec)

def get_exec_node_id(stripped_line):
	start_idx = stripped_line.find('(id=')
	if start_idx == -1:
		start_idx = stripped_line.find('(dst_id')
	end_idx = stripped_line.find(')')
	return stripped_line[start_idx+4:end_idx].strip()

def get_label(stripped_line, start_st='- ', end_str=': '):

	label = ''

	lft_idx = stripped_line.find('- ')
	rgt_idx = stripped_line.find(': ')

	if lft_idx != -1 and rgt_idx != -1:
		label = stripped_line[lft_idx+2:rgt_idx]
	

	return label

def get_node_id(stripped_line):
	label = ''
	rgt_idx = stripped_line.find(':')
	if rgt_idx != -1:
		label = stripped_line[0: rgt_idx]
	return label

def get_size_in_bytes_by_unit(size, unit):
	if unit == 'gb':
		return size * 1024 * 1024 * 1024
	elif unit == 'mb':
		return size * 1024 * 1024
	elif unit == 'kb':
		return size * 1024
	else: #second
		return size

def get_size_in_bytes(size_with_unit_in_str):
	import re

	p = re.compile('\d+(\.\d+)?', re.IGNORECASE)
	m = p.match(size_with_unit_in_str)
	start_index = m.span()[0]
	end_index = m.span()[1]

	size_in_str = size_with_unit_in_str[start_index: end_index]
	unit_in_str = size_with_unit_in_str[end_index:]
	
	return get_size_in_bytes_by_unit(float(size_in_str), unit_in_str.lower())

def get_size_in_mb_by_unit(size, unit):
	
	if unit == 'gb':
		return size * 1024.0
	elif unit == 'b':
		return size / 1024.0 / 1024.0
	elif unit == 'kb':
		return size / 1024.0
	else:
		return size

def get_size_in_mb(size_with_unit_in_str):
	import re

	p = re.compile('\d+(\.\d+)?', re.IGNORECASE)
	m = p.match(size_with_unit_in_str)
	start_index = m.span()[0]
	end_index = m.span()[1]

	size_in_str = size_with_unit_in_str[start_index: end_index].strip()
	unit_in_str = size_with_unit_in_str[end_index:].strip()

	return get_size_in_mb_by_unit(float(size_in_str), unit_in_str.lower())

def get_throughput_in_mb(stripped_line):
	import re

	throughput_in_str = ''
	idx = stripped_line.find(':')
	if idx != -1:
		throughput_in_str = stripped_line[idx+1:].strip()

	p = re.compile('\d+(\.\d+)?', re.IGNORECASE)
	m = p.match(throughput_in_str)
	start_index = m.span()[0]
	end_index = m.span()[1]

	unit_in_str = throughput_in_str[end_index+1:].lower()
	throughput_in_str = throughput_in_str[start_index:end_index]
	if float(throughput_in_str) == 0:
		return 0.0	

	idx = unit_in_str.find("/");
	size_in_str = unit_in_str[0:idx]
	time_in_str = unit_in_str[idx+1:]

	time = get_time_by_unit(1, time_in_str)
	throughput = get_throughput_in_mb_by_unit(float(throughput_in_str), size_in_str)

	return float(throughput) * float(time)


def get_throughput_in_mb_by_unit(throughput, unit):

	if unit == 'gb':
		return throughput * 1024.0
	elif unit == 'b':
		return throughput / 1024.0 / 1024.0
	elif unit == 'kb':
		return throughput / 1024.0
	else:
		return throughput

def get_partition_size(stripped_line):
	
	partition_size = []

	lft_idx = stripped_line.find('partitions=')
	rgt_idx = stripped_line.find(' size=')

	if lft_idx != -1 and rgt_idx != -1:
		partition_info = stripped_line[lft_idx+len('partitions='):rgt_idx]
		partition_size = partition_info.split("/")

	return partition_size
