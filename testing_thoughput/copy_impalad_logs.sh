scp -r tracing024:/home/shihongl/impalad/level2/*.INFO.* tracing017:/home/shihongl/impala_pt/queries/logs
scp -r tracing025:/home/shihongl/impalad/level2/*.INFO.* tracing017:/home/shihongl/impala_pt/queries/logs
scp -r tracing026:/home/shihongl/impalad/level2/*.INFO.* tracing017:/home/shihongl/impala_pt/queries/logs
scp -r tracing027:/home/shihongl/impalad/level2/*.INFO.* tracing017:/home/shihongl/impala_pt/queries/logs


scp -r tracing024:/var/log/hadoop-yarn/*-NODE* tracing017:/home/shihongl/impala_pt/queries/logs
scp -r tracing025:/var/log/hadoop-yarn/*-NODE* tracing017:/home/shihongl/impala_pt/queries/logs
scp -r tracing026:/var/log/hadoop-yarn/*-NODE* tracing017:/home/shihongl/impala_pt/queries/logs
scp -r tracing027:/var/log/hadoop-yarn/*-NODE* tracing017:/home/shihongl/impala_pt/queries/logs

cp -r /var/log/hadoop-yarn/*-RESOURCE* /home/shihongl/impala_pt/queries/logs/
cp -r /var/log/impala-llama/*-LLAMA* /home/shihongl/impala_pt/queries/logs/


