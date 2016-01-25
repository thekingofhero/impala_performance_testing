import sys
import os
import string
import shutil
import time
from config_gen import generate_config

def runBenchmark(execution_config_dirpath=r'configs',
				 config_template_path=r'config.template',
				 config_params_path=r'config.params',
				 execution_directory_path=r'.',
				 log_level='info'):

	if os.path.exists(execution_config_dirpath):
		print('remove folder: %s' % execution_config_dirpath)
		shutil.rmtree(execution_config_dirpath)

	[generated_config_path, generated_cof_dp_file] = generate_config(config_template_path, config_params_path, execution_config_dirpath)
	print('generated config path: %s' % generated_config_path)
	print('generated design parameters path: %s' % generated_cof_dp_file)

	current_milli_time = lambda: int(round(time.time() * 1000))
	
	log_file_name = os.path.basename(config_params_path.replace('.params', ''))
	log_file_name = ''.join([log_file_name, '_', str(current_milli_time()), '.log'])

	shutil.move(generated_config_path, 'config.xml')
	shutil.move(generated_cof_dp_file, 'cofs_dp.csv')

	symlink_name = 'simulation.log'
	os.system('unlink %s' % (symlink_name))
	os.system('ln -s %s %s' % (log_file_name, symlink_name))

	cmd='''
		./ImpalaSimulatorRemote.exe --cf-gui-connect=no --cf-gui-time-scale=ns --cf-mon-on-time=\"0.0 us\" --cf-sim-duration=\"1 d\" \
		--cf-verbosity=%s --cf-hpf-enable=no --cf-apf-enable=no  --cf-gui-trace-enable=no \
		--cf-lic-location=28518@plxs0415.pdx.intel.com --cf-log-file=%s 
			''' % (log_level, log_file_name)
	print(cmd)
	os.system(cmd)
	

if __name__ == '__main__':

	benchmark_paths = ['']

	for benchmark_path in benchmark_paths:
		if os.path.isfile(benchmark_path):
			if benchmark_path.endswith('params'):
				print(benchmark_path)
				runBenchmark(config_params_path=benchmark_path) 
		elif os.path.exists(benchmark_path):
			for subdir, dirs, files in os.walk(benchmark_path):
				for param_file in files:	
					if param_file.endswith('params'):
						print(os.path.join(benchmark_path,param_file))
						runBenchmark(config_params_path=os.path.join(benchmark_path,param_file)) 
	