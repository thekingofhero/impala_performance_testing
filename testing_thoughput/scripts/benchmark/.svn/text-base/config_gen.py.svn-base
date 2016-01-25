import sys
import os
import string
import xml.dom.minidom
from string import Template
import time
current_milli_time = lambda: int(round(time.time() * 1000))


def generate_config(template_file_path, params_file_path, output_dir_path):

	print('template file path: %s' % template_file_path)
	print('params file path: %s' % params_file_path)

	config_output_dir = os.path.join(output_dir_path)

	input_dir_path = os.path.split(params_file_path)[0]

	if not os.path.exists(config_output_dir):
		os.makedirs(config_output_dir)

	str_template = ''
	
	if os.path.isfile(template_file_path):
		template_reader = None
		if sys.version_info >= (3,0,0):
			template_reader = open(template_file_path, 'r', newline='')
		else:
			template_reader = open(template_file_path, 'r')

		for line in template_reader:
			str_template += line.strip()
			
	else:
		sys.exit("cannot find path %s" % template_file_path)

	output_file_name_index = 0
	if os.path.isfile(params_file_path):
		config_template = Template(str_template)

		props = [
			'file_format',
			'scaling_factor',
			'query_name',
			'compression_codec',
			'enable_data_distribution_estimation',
			'execution_plan_location',
			'execution_plan_name',
			'execution_plan_statistics',
			'data_distribution',
			'data_distribution_estimation',
			'metadata_file_path',
			'tuple_descriptor_file_path',
			'disk_model_type',
			'built_in_disk_model_file',
			'enable_firespring_poc',
			'enable_remote_cache',
			'enable_hdfs_cache',
			'pert_hits',
			'num_data_disks',
			'total_num_disks',
			'default_disk_speed_in_mb',
			'proc_perf_indicator',
			'server_num',
			'link_type',
			'parquet_row_group_size_in_bytes',
			'disk_type',
			'block_mode_speed_factor',
			'ignore_materialization_cost',
			'ignore_data_decompression_cost'
			]

		param_map = {}

		params_reader = None
		if sys.version_info >= (3,0,0):
			params_reader = open(params_file_path, 'r', newline='')
		else:
			params_reader = open(params_file_path, 'r')

		for line in params_reader:

			line = line.strip()

			if len(line) == 0:
				continue

			splits = line.strip().split('=')
			key = splits[0]
			value = splits[1]

			param_map[key] = value

		max_row_batch_size=param_map['total_num_disks'] if 'total_num_disks' in param_map.keys() else '-1'
		max_row_batch_size=str(int(max_row_batch_size)*10)

		for prop in props:
			if prop not in param_map.keys():
				sys.exit('missing property -> %s' % (prop))


		new_template = config_template.substitute(
						file_format=param_map['file_format'],
						scaling_factor=param_map['scaling_factor'],
						query_name=param_map['query_name'],
						compression_codec=param_map['compression_codec'],
						enable_data_distribution_estimation=param_map['enable_data_distribution_estimation'],
						execution_plan_location=os.path.join(input_dir_path, param_map['execution_plan_location']),
						execution_plan_name=param_map['execution_plan_name'],
						execution_plan_statistics=param_map['execution_plan_statistics'],
						data_distribution=param_map['data_distribution'],
						data_distribution_estimation=param_map['data_distribution_estimation'],
						metadata_file_path=os.path.join(input_dir_path, param_map['metadata_file_path']),
						tuple_descriptor_file_path=os.path.join(input_dir_path, param_map['tuple_descriptor_file_path']),
						disk_model_type=param_map['disk_model_type'],
						built_in_disk_model_file=param_map['built_in_disk_model_file'],
						enable_firespring_poc=param_map['enable_firespring_poc'],
						enable_remote_cache=param_map['enable_remote_cache'],
						enable_hdfs_cache=param_map['enable_hdfs_cache'],
						pert_hits=param_map['pert_hits'],
						disk_num=param_map['num_data_disks'],							
						max_row_batch_size=max_row_batch_size,
						default_disk_speed_in_mb=param_map['default_disk_speed_in_mb'],
						proc_perf_indicator=param_map['proc_perf_indicator'],
						server_num=param_map['server_num'],
						link_type=param_map['link_type'],
						parquet_row_group_size_in_bytes=param_map['parquet_row_group_size_in_bytes'],
						disk_type=param_map['disk_type'],
						block_mode_speed_factor=param_map['block_mode_speed_factor'],
						ignore_materialization_cost=param_map['ignore_materialization_cost'],
						ignore_data_decompression_cost=param_map['ignore_data_decompression_cost']
						)

		
		query_name = param_map['query_name']
		output_file_name = '_'.join(['config', 
									 query_name, 
									 param_map['file_format'], 
									 param_map['compression_codec'], 
									 param_map['scaling_factor'], 
									 str(current_milli_time())]
								)
		output_file_name = output_file_name.replace('.', '_')
		output_file_name = '.'.join([output_file_name, 'xml'])
		
		if not os.path.exists(os.path.join(config_output_dir, query_name)):
			os.makedirs(os.path.join(config_output_dir, query_name))

		output_file_name = os.path.join(config_output_dir, query_name, output_file_name)

		print('output file name %s' % (output_file_name))

		config_dom = xml.dom.minidom.parseString(new_template)
		with open(output_file_name, 'w') as config_writer:
			config_writer.write(config_dom.toprettyxml())

		cofs_dp_file = '.'.join(['cofs_dp', 'csv'])
		cofs_dp_file = os.path.join(config_output_dir, query_name ,cofs_dp_file)
		print('cofluent design parameter file name %s' % (cofs_dp_file))
		with open(cofs_dp_file, 'w') as cofs_dp_file_writer:
			cofs_dp_file_writer.write('component_name,design_parameter_name,value\n')
			cofs_dp_file_writer.write('/Impala,DP_NumImpalaClients,1\n')
			
			cofs_dp_file_writer.write('/Impala,DP_NumBackends,{0}\n'.format( str(int(param_map['server_num'])-1) ))

			if param_map['disk_type'] == 'DISK_SSD_INTEL_S3700':
				cofs_dp_file_writer.write('/Impala,DP_NumDiskWorkers,{0}\n'.format( str(int(param_map['num_data_disks']) * 8 -1) ))
			elif param_map['disk_type'] == 'DISK_HDD':				
				cofs_dp_file_writer.write('/Impala,DP_NumDiskWorkers,{0}\n'.format( str(int(param_map['num_data_disks'])-1) ))

			cofs_dp_file_writer.write('/Impala,DP_PlanFragmenExecutors,99\n')
			cofs_dp_file_writer.write('/Impala,DP_NumRequestContext,99\n')
			cofs_dp_file_writer.write('/Impala,DP_NumScanners,99\n')

	else:
		sys.exit("cannot find path %s" % params_file_path)

	return [output_file_name, cofs_dp_file]

if __name__ == '__main__':

	sys.argv = ['conf_gen.py', 
				r'config.template', 
			    os.path.join(r'D:\Workspace\2LM-CrystalRidge\simulation\TPCDS_WFI\C1200_M1333_TPCDS_WFI_q98.params'), 
			    r'D:\Workspace\2LM-CrystalRidge\simulation\configs']

	if len(sys.argv) < 3:
		sys.exit("usage: config_template.py <template file> <params file> <output_dir_path")

	generate_config(sys.argv[1], sys.argv[2], sys.argv[3])