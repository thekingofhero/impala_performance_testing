import os
from config import local_config
class CPUFreqSet:
    def __init__(self,):
        self.datanode_list = local_config()['datanode_list']
        self.cpu_freq_dict = local_config()['cpu_freq_dict']

    def set(self,freq):
        if self.check_cpu_stat(self.cpu_freq_dict[str(freq)]/1000):
            print 'CURRENT CPU_FREQ IS OK'
            return True;

        while True:
            if str(freq) in self.cpu_freq_dict.keys():
                cpu_freq = self.cpu_freq_dict[str(freq)]
            else:
                cpu_freq = 2700000
            for datanode in self.datanode_list:
                os.system('ssh %s cpupower -c all frequency-set -f %s'%(datanode,str(cpu_freq)))
            if self.check_cpu_stat(cpu_freq/1000):
                print 'CPU_FREQ_SET is OK'
                break
            else:
                print 'CPU_FREQ_SET failed.Please waiting...'


    def check_cpu_stat(self,current_cpufreq):
        for i,datanode_i in enumerate(self.datanode_list):
            cmd = 'ssh %s "cat /proc/cpuinfo | grep \'cpu MHz\'"'%(datanode_i)
            fp = os.popen(cmd,'r')
            for line in fp.readlines():
                cpu_freq = float(line.split(':')[1])
                if cpu_freq <> current_cpufreq:
                    return False
            fp.close()
        return True

if __name__ == '__main__':
    CPUFreqSet_OBJ = CPUFreqSet()
    CPUFreqSet_OBJ.set(2.5)

        
