import re
import time
import sys
import socket
import os
def main(log_path,cur_time,output_dir):
    with open(log_path,'r') as fp:
        lines = fp.readlines()
        for line_i ,line in enumerate(lines):
            if re.search('\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d',line):
                if time.mktime(time.strptime(re.findall('\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d',line)[0],"%Y-%m-%d %H:%M:%S")) >= cur_time:
                    with open('%s/%s'%(output_dir,os.path.basename(log_path)),'w') as fp_w: fp_w.writelines(lines[line_i:])
                    break

if __name__ == "__main__":
    if len(sys.argv) > 3:
        main(sys.argv[1],float(sys.argv[2]),sys.argv[3])
