# -*- coding: utf-8 -*-
from time import sleep

from commons import *


def install_spark():
    namenode_list = params_dict['namenode.list']
    jvm_heapsize = params_dict["jvm.heapsize"]
    dfs_nameservice = params_dict["dfs.nameservice"]
    zk_addr = params_dict["zookeeper.address"]
    spark_master_ips = params_dict["spark.master.ip"]
    is_valid_ip(local_ip, spark_master_ips)
    spark_cluster_id = params_dict["spark.cluster.id"]
    spark_masters = ",".join([f"{ip}:7077" for ip in spark_master_ips])
    spark_conf_dir = os.path.join(app_home_dir, "conf")
    spark_env_file = os.path.join(spark_conf_dir, "spark-env.sh")
    spark_defaults_file = os.path.join(spark_conf_dir, "spark-defaults.conf")
    spark_sbin_dir = os.path.join(app_home_dir, "sbin")

    # 生成配置
    generate_config_file(template_str=spark_env_template,
                         conf_file=spark_env_file,
                         install_role=install_role,
                         local_ip=local_ip,
                         spark_home_dir=app_home_dir,
                         spark_conf_dir=spark_conf_dir,
                         jvm_heapsize=jvm_heapsize,
                         dfs_nameservice=dfs_nameservice,
                         zk_addr=zk_addr,
                         spark_cluster_id=spark_cluster_id
                         )
    generate_config_file(template_str=spark_defaults_template,
                         conf_file=spark_defaults_file,
                         install_role=install_role,
                         spark_masters=spark_masters,
                         dfs_nameservice=dfs_nameservice,
                         spark_cluster_id=spark_cluster_id)

    active_namenode_ip = check_namenode_status(namenode_list)
    download_from_hdfs(active_namenode_ip, "/hadoop/share/conf/", f"{spark_conf_dir}")
    set_permissions(app_home_dir)
    if install_role != "yarn":
        exec_shell_command(f"{spark_sbin_dir}/start-master.sh",
                           "spark master start", output=True)
        exec_shell_command(f"{spark_sbin_dir}/start-worker.sh spark://{spark_masters}",
                           "spark worker start", output=True)
        exec_shell_command(f"{spark_sbin_dir}/start-history-server.sh",
                           "spark history server start", output=True)
    configure_environment("SPARK_HOME", app_home_dir)
    print("Spark installation completed")


if __name__ == '__main__':
    spark_env_template = """
export HADOOP_CONF_DIR={{ spark_conf_dir }}
export SPARK_MASTER_HOST={{ local_ip }}
export SPARK_LOCAL_DIRS={{ spark_home_dir }}/tmp
export SPARK_CONF_DIR={{ spark_conf_dir }}
export YARN_CONF_DIR={{ spark_conf_dir }}
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_REST_ENABLED=true
export SPARK_MASTER_REST_PORT=6066
export SPARK_MASTER_WEBUI_PORT=10090
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=10091
export SPARK_WORKER_DIRS={{ spark_home_dir }}/work
export SPARK_LOG_DIR={{ spark_home_dir }}/log
export SPARK_PID_DIR={{ spark_home_dir }}/pid
export SPARK_DAEMON_MEMORY={{ jvm_heapsize }}
export GC_OPTS="-XX:+UseG1GC -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime -XX:+HeapDumpOnOutOfMemoryError"
export SPARK_SUBMIT_OPTS="-Djava.io.tmpdir={{ spark_home_dir }}/tmp -Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=3600 -Dspark.worker.cleanup.appDataTtl=86400 -Dspark.history.ui.port=18080 -Dspark.history.fs.cleaner.enabled=true -Dspark.history.fs.cleaner.interval=3d -Dspark.history.fs.cleaner.maxAge=14d -Dspark.history.fs.logDirectory=hdfs://{{ dfs_nameservice }}/spark/history/logs"
export SPARK_MASTER_OPTS="${GC_OPTS} -Xloggc:{{ spark_home_dir }}/logs/master-gc.log -XX:HeapDumpPath={{ spark_home_dir }}/master-heapdump.hprof"
export SPARK_WORKER_OPTS="${GC_OPTS} -Xloggc:{{ spark_home_dir }}/logs/worker-gc.log -XX:HeapDumpPath={{ spark_home_dir }}/worker-heapdump.hprof"
export SPARK_HISTORY_OPTS="${GC_OPTS} -Xloggc:{{ spark_home_dir }}/logs/historyserver-gc.log -XX:HeapDumpPath={{ spark_home_dir }}/historyserver-heapdump.hprof"
{% if cluster_role == "cluster" %}
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=zookeeper://{{ zk_addr }} -Dspark.deploy.zookeeper.dir=/{{ spark_cluster_id }}"
{% endif %}
"""
    spark_defaults_template = """
{% if install_role == "standalone" or install_role == "cluster" %}
spark.master                            spark://{{ spark_masters }}
{% endif %}
{% if cluster_role == "yarn" %}
spark.master                            yarn
spark.dynamicAllocation.enabled         true                                  
spark.dynamicAllocation.minExecutors    1                                
spark.dynamicAllocation.maxExecutors    200                              
spark.dynamicAllocation.initialExecutors                    1                          
spark.dynamicAllocation.schedulerBacklogTimeout             1s                   
spark.dynamicAllocation.sustainedSchedulerBacklogTimeout    1s  
spark.yarn.jars                         hdfs://{{ dfs_nameservice }}/spark/share/jars 
{% endif %}
# 作业提交模式
spark.submit.deployMode                 cluster
spark.hadoop.fs.defaultFS               hdfs://{{ dfs_nameservice }}
# 事件日志 
spark.history.ui.port                   18080
spark.eventLog.enabled                  true
spark.history.retainedApplications      100
spark.executor.logs.rolling.maxSize     1048576
spark.executor.logs.rolling.maxRetainedFiles    10
spark.executor.logs.rolling.enableCompression   true
spark.eventLog.dir                      hdfs://{{ dfs_nameservice }}/spark/history/logs
spark.history.fs.logDirectory           hdfs://{{ dfs_nameservice }}/spark/history/logs

# shuffle 相关
spark.sql.shuffle.partitions            200                         
spark.shuffle.compress                  true                                  
spark.shuffle.spill.compress            true                                                   
spark.shuffle.file.buffer               32k                                 
spark.shuffle.io.maxRetries             10                                  
spark.shuffle.io.retryWait              30s    
spark.sql.files.openCostInBytes         4194304       
spark.sql.files.maxPartitionBytes       134217728                     
spark.sql.autoBroadcastJoinThreshold    10485760  

# 压缩
spark.rdd.compress                      true
spark.broadcast.compress                true
spark.kafka.consumer.cache.enabled      true

# Spark SQL 相关配置
spark.sql.shuffle.partitions            200                                                   
spark.sql.parquet.cacheMetadata         true
spark.sql.parquet.filterPushdown        true
spark.sql.files.maxPartitionBytes       134217728 
spark.sql.inMemoryColumnarStorage.compressed  true
spark.sql.cache.serializer              org.apache.spark.storage.SnappyCompressionCodec 

# Spark MLlib 配置
spark.mllib.regParam                    0.01                                
spark.mllib.numIterations               10                                 
"""

    spark_class = ["org.apache.spark.deploy.master.Master",
                   "org.apache.spark.deploy.worker.Worker",
                   "org.apache.spark.deploy.history.HistoryServer"]
    kill_service(spark_class)
    unzip_package()
    install_spark()
