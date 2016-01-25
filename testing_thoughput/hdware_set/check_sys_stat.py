import os
from config import local_config

class check_sys_state:
    def __init__(self):
        self.impalad_nodes = local_config()['impalad_nodes']
    
    def check_sys_io(self):
        t_flag = True
        while t_flag:
            for i,datanode_i in enumerate(self.impalad_nodes):
                cmd = 'ssh %s "export TERM=linux;top -bn 2"'%(datanode_i)
                fp = os.popen(cmd,'r')
                top_part = 0
                for line in fp.readlines():
                    if line.find('Cpu(s)') > -1 :
                #        print line
                        if top_part == 1:
                            cpu_s= line 
                        else:
                            top_part += 1
    
                #cpu(s) state
                us,sy,ni,id,wa,hi,si,st, = cpu_s.split(',')
                if float(wa[wa.find('  ') + 2:wa.find('%')]) < 1 :
                    print "%s's IO is OK... "%(datanode_i)
                    if i == len(self.impalad_nodes) - 1:
                        t_flag = False
                    
                    continue
                else:
                    print "%s's IO is busy... "%(datanode_i)
                    break
        return True

if __name__ == '__main__':
    check_sys_state_obj = check_sys_state()
    check_sys_state_obj.check_sys_io()
