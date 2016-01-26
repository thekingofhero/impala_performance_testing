# impala_performance_testing
    通过tpcds tool，生成tpcds数据至HDFS，并基于此数据创建impala数据库（文件格式支持text/parquet，压缩方式支持 非压缩/snappy）。
对生成好的数据库进行performance testing，用于研究不同硬件/软件参数对impala执行效率的影响。测试结果导出到excel文件供临时分析，保存至RDBMS
供日后查询。

1.程序结构：
  整体上分为三个部分：数据生成(data_generator) / 性能测试（performance_testing）/测试结果保存
  performance_testing又分为两种方式：query_onebyone：同一时刻，在impala上只执行一个query，用于研究不同硬件/软件参数对impala执行效率的影响。
                                     query_throughput（独立运行，尚未整合至本工程）:同一时刻，在impala上执行多个query，在一段时间后，统计执行完成的query个数，以确定impala的并发性能。
  测试结果保存：测试结果导出到excel供临时分析
                测试结果导入RDBMS 持久化保存
  
2.模块说明：
  main.py:程序主入口
  config.py:RDBMS/数据生成/性能测试所需的全部配置
  switch.config：执行步骤开关
  data_generator：数据生成模块
  testing_onebyone:onebyone性能测试模块
  testing_thoughput：throughput测试模块
  testing_result:测试结果根目录
  excel_relate：测试结果导出为excel
  utilities：测试结果导入RDBMS及相关脚本
  
