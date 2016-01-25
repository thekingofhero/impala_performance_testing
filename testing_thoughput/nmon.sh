#!/bin/bash
for i in 024 025 026 027
do
ssh tracing$i 'ntpdate 172.18.0.17 &> /etc/null;mkdir -p /home/shihongl/nmon_log;rm -rf * /home/shihongl/nmon_log/*'
done
mkdir /home/shihongl/impala_pt/queries/nmon_logs

echo 'start nmom ............'
for i in 024 025 026 027
do
ssh tracing$i '/home/chenwenwen/nmon_linux_x86_64 -s 1 -c 36000 -f -m /home/shihongl/nmon_log'
done


echo 'run script .............'
python main.py

echo 'stop nmon ..............'
#rm -rf /home/shihongl/impala_pt/queries/nmon_logs/*
#mkdir /home/shihongl/impala_pt/queries/nmon_logs
for i in 024 025 026 027
do
ssh tracing$i 'sh /home/chenwenwen/stop_nmon.sh &>/etc/null '
ssh tracing$i ' scp /home/shihongl/nmon_log/*.nmon tracing017:/home/shihongl/impala_pt/queries/nmon_logs; rm -rf /home/shihongl/nmon_log/*.nmon '
done
echo ' complete '
