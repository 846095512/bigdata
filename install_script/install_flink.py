# -*- coding=utf-8 -*-
from commons import *


def install_flink():
    namenode_list = params_dict['namenode.list']
    dfs_nameservice = params_dict["dfs.nameservice"]
    flink_cluster_id = params_dict["flink.cluster.id"]
    if install_role == "cluster":
        zk_addr = ",".join([f"{ip}:2181" for ip in install_ip])
    else:
        zk_addr = params_dict["zookeeper.address"]
    task_slots, stderr, code = exec_shell_command("nproc")
    history_server_port = 8082
    prom_port = 9249
    jm_rpc_port = 6123
    jm_rest_port = 8081
    parallelism = 1
    flink_home_dir = os.path.join(get_app_home_dir(), 'flink')
    flink_conf_file = os.path.join(flink_home_dir, 'conf', 'config.yaml')
    zk_conf_file = os.path.join(flink_home_dir, 'conf', 'zoo.cfg')
    flink_bin_dir = os.path.join(flink_home_dir, 'bin')
    jvm_options = f"-XX:+UseG1GC -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime -XX:+HeapDumpOnOutOfMemoryError"
    exec_shell_command(f"mkdir -p {flink_home_dir}/tmp")
    exec_shell_command(f"mkdir -p {flink_home_dir}/data/upload")
    exec_shell_command(f"mkdir -p {flink_home_dir}/data/archive")
    exec_shell_command(f"mkdir -p {flink_home_dir}/conf/hadoop")
    exec_shell_command(f"mkdir -p {flink_home_dir}/data/checkpoints")
    exec_shell_command(f"mkdir -p {flink_home_dir}/data/savepoints")
    exec_shell_command(f"mv {flink_conf_file} {flink_conf_file}.template")
    exec_shell_command(f"mv {zk_conf_file} {zk_conf_file}.template")
    total_mem, stderr, code = exec_shell_command("free -g | awk '/Mem:/ {print $2}'")
    jm_heapsize = f"{total_mem / 8}g"
    tm_heapsize = f"{total_mem / 2}g"
    generate_config_file(
        template_str=flink_conf_template,
        conf_file=flink_conf_file,
        install_role=install_role,
        local_ip=local_ip,
        jm_rpc_port=jm_rpc_port,
        jm_rest_port=jm_rest_port,
        jm_heapsize=jm_heapsize,
        tm_heapsize=tm_heapsize,
        task_slots=task_slots,
        parallelism=parallelism,
        jvm_options=jvm_options,
        flink_home_dir=flink_home_dir,
        dfs_nameservice=dfs_nameservice,
        flink_cluster_id=flink_cluster_id,
        zk_addr=zk_addr,
        history_server_port=history_server_port,
        prom_port=prom_port
    )

    active_namenode_ip = check_namenode_status(namenode_list)
    download_from_hdfs(active_namenode_ip, "/hadoop/share/jars/", f"{flink_home_dir}/lib")
    download_from_hdfs(active_namenode_ip, "/hadoop/share/conf/", f"{flink_home_dir}/conf/hadoop")
    set_permissions(flink_home_dir)

    if install_role == "cluster" or install_role == "standalone":
        stdout, stderr, code = exec_shell_command(f"{flink_bin_dir}/jobmanager.sh start")
        check_cmd_output(stdout, stderr, code, "flink jobmanager 启动", check=True)
        stdout, stderr, code = exec_shell_command(f"{flink_bin_dir}/taskmanager.sh start")
        check_cmd_output(stdout, stderr, code, "flink taskmanager 启动", check=True)
        stdout, stderr, code = exec_shell_command(f"{flink_bin_dir}/historyserver.sh start")
        check_cmd_output(stdout, stderr, code, "flink historyserver 启动", check=True)
    print("flink 安装完成")


if __name__ == '__main__':
    flink_conf_template = """
# 基本参数
jobmanager.rpc.address: {{ local_ip }}
jobmanager.rpc.port: {{ jm_rpc_port }}
rest.port: {{ jm_rest_port }}
jobmanager.memory.process.size: {{ jm_heapsize }}
taskmanager.memory.process.size: {{ tm_heapsize }}
taskmanager.numberOfTaskSlots: {{ task_slots }}
parallelism.default: {{ parallelism }}
env.log.dir: {{ flink_home_dir }}/log
env.pid.dir: {{ flink_home_dir }}/pid
env.java.opts: {{ jvm_options }}
env.hadoop.conf.dir: {{ flink_home_dir }}/conf/hadoop
io.tmp.dirs: {{ flink_home_dir }}/tmp

# checkpoint 状态后端
execution.checkpointing.interval: 30000
execution.checkpointing.min-pause: 2000
execution.checkpointing.timeout: 60000
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
state.backend: rocksdb
state.backend.incremental: true
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.localdir: {{ flink_home_dir }}/data
state.backend.rocksdb.timer-service.factory: rocksdb

# 重启策略
restart-strategy: failure-rate
restart-strategy.failure-rate.max-failures-per-interval: 3
restart-strategy.failure-rate.failure-rate-interval: 600s
restart-strategy.failure-rate.delay: 10s

# 历史服务器
historyserver.archive.clean-expired-jobs.retention-time: 30d
historyserver.archive.fs.refresh-interval: 300s
historyserver.archive.clean-expired-jobs: true
historyserver.web.address: 0.0.0.0
historyserver.web.port: {{ history_server_port }}

# 监控
metrics.reporters: prom
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: {{ prom_port }}
{% if install_role == "standalone" or install_role == "cluster" %}
# web UI
web.submit.enable: true
web.cancel.enable: true
web.upload.dir: {{ flink_home_dir }}/data/upload
rest.profiling.enabled: true
{% endif %}
{% if install_role == "standalone" %}
state.checkpoints.dir: file://{{ flink_home_dir }}/data/checkpoints
state.savepoints.dir: file://{{ flink_home_dir }}/data/savepoints
{% else %}
state.checkpoints.dir: hdfs://{{ dfs_nameservice }}/flink/checkpoints
state.savepoints.dir: hdfs://{{ dfs_nameservice }}/flink/savepoints
{% endif %}
{% if install_role == "yarn" %}
# yarn
yarn.maximum-failed-containers: 5
yarn.application-attempts: 3
yarn.container-start-timeout: 300000
{% endif %}
{% if install_role == "yarn" or install_role == "cluster" %}
# 高可用
high-availability.type: ZOOKEEPER
high-availability.cluster-id: {{ flink_cluster_id }}
high-availability.zookeeper.path.root: /{{ flink_cluster_id }}
high-availability.storageDir: hdfs://{{ dfs_nameservice }}/flink/storage
high-availability.zookeeper.quorum: {{ zk_addr }}
{% endif %}
{% if install_role == "standalone" %}
jobmanager.archive.fs.dir: file://{{ flink_home_dir }}/data/jobmanager/archive
historyserver.archive.fs.dir: file://{{ flink_home_dir }}/data/historyserver/archive
{% endif %}
{% if install_role == "yarn" or install_role == "cluster" %}
jobmanager.archive.fs.dir: hdfs://{{ dfs_nameservice }}/flink/jobmanager/archive
historyserver.archive.fs.dir: hdfs://{{ dfs_nameservice }}/flink/historyserver/archive
{% endif %}
"""
    flink_class = ["org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint",
                        "org.apache.flink.runtime.taskexecutor.TaskManagerRunner",
                        "org.apache.flink.runtime.webmonitor.history.HistoryServer"]
    kill_service(flink_class)
    unzip_package()
    install_flink()
