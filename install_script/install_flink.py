# -*- coding=utf-8 -*-
from commons import *


def install_flink():
    flink_conf_template = """
jobmanager.rpc.address= {{ local_ip }}
jobmanager.rpc.port={{ jm_rpc_port }}
jobmanager.memory.process.size={{ jvm_heapsize }}
taskmanager.memory.process.size={{ jvm_heapsize }}
taskmanager.numberOfTaskSlots={{ task_slots }}
parallelism.default={{ parallelism }}
env.log.dir={{ flink_home_dir }}/log
env.pid.dir={{ flink_home_dir }}/pid
io.tmp.dirs={{ flink_home_dir }}/tmp

{% if install_role = "standalone" or install_role = "cluster"%}
web.submit.enable=true
web.cancel.enable=true
web.upload.dir={{ flink_home_dir }}/upload
{% endif %}

execution.checkpointing.interval=30000
execution.checkpointing.min-pause=2000
execution.checkpointing.timeout=60000
execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION
state.backend=rocksdb
state.backend.incremental=true
state.backend.rocksdb.memory.managed=true
state.backend.rocksdb.localdir={{ flink_home_dir }}/data
state.backend.rocksdb.timer-service.factory=rocksdb

{% if install_role = "standalone"%}
state.checkpoints.dir=file://{{ flink_home_dir }}/data/checkpoints
state.savepoints.dir=file://{{ flink_home_dir }}/data/savepoints
{% else %}
state.checkpoints.dir=hdfs://{{ dfs_nameservice }}/{{ flink_cluster_id }}/checkpoints
state.savepoints.dir=hdfs://{{ dfs_nameservice }}/{{ flink_cluster_id }}/savepoints
{% endif %}

restart-strategy=failure-rate
restart-strategy.failure-rate.max-failures-per-interval=3
restart-strategy.failure-rate.failure-rate-interval=600s
restart-strategy.failure-rate.delay=10s



env.hadoop.conf.dir={{ flink_home_dir }}/hadoop
env.yarn.conf.dir={{ flink_home_dir }}/hadoop

{% if install_role = "yarn"%}
yarn.maximum-failed-containers=5
yarn.application-attempts=3
yarn.container-start-timeout=300000
{% endif %}
{% if install_role = "yarn" or install_role = "cluster" %}
high-availability.type=ZOOKEEPER
high-availability.cluster-id={{ flink_cluster_id }}
high-availability.storageDir=hdfs://{{ dfs_nameservice }}/{{ flink_cluster_id }}/ha
high-availability.zookeeper.path.root=/{{ flink_cluster_id }}
high-availability.zookeeper.quorum={{ zk_addr}}
{% endif %}

# history server
{% if install_role = "standalone"%}
jobmanager.archive.fs.dir=file://{{ flink_home_dir }}/data/archive
historyserver.archive.fs.dir=file://{{ flink_home_dir }}/data/archive
{% else %}
jobmanager.archive.fs.dir=hdfs://{{ dfs_nameservice }}/{{ flink_cluster_id }}/archive
historyserver.archive.fs.dir=hdfs://{{ dfs_nameservice }}/{{ flink_cluster_id }}/archive
{% endif %}

historyserver.archive.clean-expired-jobs.retention-time=30d
historyserver.archive.fs.refresh-interval=300s
historyserver.archive.clean-expired-jobs=true
historyserver.web.address=0.0.0.0
historyserver.web.port={{ history_server_port }}


metrics.reporters=prom
metrics.reporter.prom.class=org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port={{ prom_port }}
"""
    zk_conf_template = """
initLimit=10
syncLimit=5
clientPort=2181
dataDir={{ flink_home_dir }}/data/zookeeper
autopurge.snapRetainCount=5
autopurge.purgeInterval=12
maxClientCnxns=1000
minSessionTimeout=10000
maxSessionTimeout=60000
admin.enableServer="false"
admin.serverPort=9999
{% if install_role = "cluster" %}
{% for ip in install_ip %}
server.{{ install_ip.index(ip) }}={{ ip }}}:2888:3888
{% endfor %}
{% endif %}    
"""
    dfs_nameservice = params_dict["dfs.nameservice"]
    flink_cluster_id = params_dict["flink.cluster.id"]
    zk_addr = params_dict["zk.addr"]
    jvm_heapsize = params_dict["jvm.heapsize"]
    task_slots, stderr, code = exec_shell_command("nproc")
    history_server_port = 8082
    prom_port = 9249
    jm_rpc_port = 8081
    parallelism = 1
    flink_home_dir = os.path.join(get_app_home_dir(), 'flink')
    flink_conf_file = os.path.join(flink_home_dir, 'conf', 'config.yaml')
    zk_conf_file = os.path.join(flink_home_dir, 'conf', 'zoo.cfg')
    bin_dir = os.path.join(flink_home_dir, 'bin')

    exec_shell_command(f"mkdir -p {flink_home_dir}/data/checkpoints")
    exec_shell_command(f"mkdir -p {flink_home_dir}/data/savepoints")
    exec_shell_command(f"mkdir -p {flink_home_dir}/hadoop")
    exec_shell_command(f"mkdir -p {flink_home_dir}/upload")
    exec_shell_command(f"mkdir -p {flink_home_dir}/tmp")
    exec_shell_command(f"mkdir -p {flink_home_dir}/archive")
    exec_shell_command(f"mkdir -p {flink_home_dir}/conf/task_conf_tmp")

    exec_shell_command(f"mv {flink_conf_file} {flink_conf_file}.template")
    generate_config_file(template_str=flink_conf_template,
                         conf_file=flink_conf_file,
                         keyword="",
                         install_role=install_role,
                         local_ip=local_ip,
                         jm_rpc_port=jm_rpc_port,
                         jvm_heapsize=jvm_heapsize,
                         task_slots=task_slots,
                         parallelism=parallelism,
                         flink_home_dir=flink_home_dir,
                         dfs_nameservice=dfs_nameservice,
                         flink_cluster_id=flink_cluster_id,
                         zk_addr=zk_addr,
                         history_server_port=history_server_port,
                         prom_port=prom_port)
    generate_config_file(
        template_str=zk_conf_template,
        conf_file=zk_conf_file,
        keyword="",
        install_role=install_role,
        install_ip=install_ip
    )

if __name__ == '__main__':
    unzip_package()
    install_flink()