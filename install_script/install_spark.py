# -*- coding: utf-8 -*-
import os
from commons import *


def install_spark():
    spark_env_template = """
export HADOOP_CONF_DIR={{ spark_conf_dir }}
export SPARK_MASTER_HOST={{ local_ip }}
export SPARK_LOCAL_DIRS={{ spark_home_dir }}/tmp
export SPARK_CONF_DIR={{ spark_conf_dir }}
export YARN_CONF_DIR={{ spark_conf_dir }}
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=10090
export SPAR_MASTER_OPTS="-Dspark.master.rest.enabled=true -Dspark.master.rest.port=6066"
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=10091
export SPAR_WORKER_PORT="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=3600 -Dspark.worker.cleanup.appDataTtl=86400"
export SPARK_WORKER_DIRS={{ spark_home_dir }}/work
export SPARK_LOG_DIR={{ spark_home_dir }}/logs
export SPARK_PID_DIR={{ spark_home_dir }}/pids
export SPARK_DAEMON_MEMORY={{ jvm_heapsize }}
export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 -Dspark.history.fs.cleaner.enabled=true -Dspark.history.fs.cleaner.interval=3d -Dspark.history.fs.cleaner.maxAge=14d -Dspark.history.fs.logDirectory=hdfs://{{ dfs_nameservice }}/spark-logs"
{% if cluster_role == "standalone" %}
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=zookeeper://{{zk_addr }} -Dspark.deploy.zookeeper.dir=/{{ spark_cluster_id }}"
{% endif %}
"""
    spark_defaults_template = """
{% if install_role == "standalone" %}
spark.master    spark://{{ spark_masters }}
{% endif %}
{% if cluster_role == "yarn" %}
spark.master    yarn
{% endif %}
spark.eventLog.enabled  true
spark.eventLog.dir  hdfs://{{ dfs_nameservice }}/spark-logs
spark.executor.logs.rolling.enableCompression   true
spark.executor.logs.rolling.maxSize 1048576
spark.executor.logs.rolling.maxRetainedFiles 10
"""
    param_dict = get_install_config()
    module_name = param_dict["module"]
    jvm_heapsize = param_dict["jvm.heapsize"]
    dfs_nameservice =  param_dict["dfs.nameservice"]
    install_role = param_dict["install.role"]
    zk_addr = param_dict["zk.addr"]
    local_ip = param_dict["local.ip"]
    spark_master_ips = param_dict["spark.master.ip"]

    is_valid_ip(local_ip,spark_master_ips)

    dfs_nameservice = param_dict["dfs.nameservice"]
    spark_cluster_id = param_dict["spark.cluster.id"]
    spark_masters = ",".join([f"{ip}:7077" for ip in spark_master_ips])
    


    spark_home_dir = os.path.join(get_app_home_dir(), module_name)
    spark_conf_dir = os.path.join(spark_home_dir, "conf")
    spark_env_file = os.path.join(spark_conf_dir, "spark-env.sh")
    spark_defaults_file = os.path.join(spark_conf_dir, "spark-defaults.conf")
    spark_sbin_dir = os.path.join(spark_home_dir, "sbin")
    generate_config_file(template_str=spark_env_template,
                         conf_file=spark_env_file,
                         install_role=install_role,
                         keyword="",
                         local_ip=local_ip,
                         spark_home_dir=spark_home_dir,
                         spark_conf_dir=spark_conf_dir,
                         jvm_heapsize=jvm_heapsize,
                         dfs_nameservice=dfs_nameservice,
                         zk_addr=zk_addr,
                         spark_cluster_id=spark_cluster_id
                         )
    generate_config_file(template_str=spark_defaults_template,
                         conf_file=spark_defaults_file,
                         keyword="",
                         install_role=install_role,
                         spark_masters=spark_masters,
                         dfs_nameservice=dfs_nameservice)
    print("生成配置文件完成")

    set_permissions(spark_home_dir)
    exec_shell_command(f"{spark_sbin_dir}/start-master.sh")
    exec_shell_command(f"{spark_sbin_dir}/start-worker.sh spark://{spark_masters}")
    exec_shell_command(f"{spark_sbin_dir}/start-history-server.sh")


if __name__ == '__main__':
    unzip_package()
    install_spark()