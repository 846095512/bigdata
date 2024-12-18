# -*- coding: utf-8 -*-
import math

from commons import *


def install_kafka():
    if len(install_ip) % 2 == 0:
        partitions_default = math.ceil((len(install_ip) / 2) + 1)
    else:
        partitions_default = math.ceil(len(install_ip) / 2)
    broker_list = params_dict["broker.list"]
    broker = f"PLAINTEXT://{local_ip}:9092"
    server_conf = os.path.join(app_home_dir, "config", "server.properties")
    kraft_conf = os.path.join(app_home_dir, "config", "kraft", "server.properties")
    bin_dir = os.path.join(app_home_dir, "bin")
    broker_id = 0
    for i in range(len(install_ip)):
        if local_ip == install_ip[i]:
            broker_id = i
    exec_shell_command(f"mv {server_conf} {server_conf}.template")
    exec_shell_command(f"mv {kraft_conf} {kraft_conf}.template")

    kraft_enable = params_dict["kraft.enable"]
    if kraft_enable == "true":
        cluster_id = generate_uuid(params_dict["kafka.cluster.id"])
        node_id = broker_id
        controller = f"CONTROLLER://{local_ip}:9093"
        controller_list = params_dict["controller.list"]
        controller_quorums = ",".join([f"{controller_list.index(ip)}@{ip}:9093" for ip in controller_list])

        if local_ip in broker_list and local_ip in controller_list:
            process_roles = "broker,controller"
            listeners = f"{broker},{controller}"
        elif local_ip in broker_list:
            process_roles = "broker"
            listeners = f"{broker}"
        elif local_ip in controller_list:
            process_roles = "controller"
            listeners = f"{controller}"
        else:
            print("The local IP is not in the broker or controller list. Please check the installation parameters")
            sys.exit(1)

        generate_config_file(
            template_str=kafka_conf_template,
            conf_file=kraft_conf,
            broker=broker,
            process_roles=process_roles,
            controller_quorums=controller_quorums,
            controller=controller,
            listeners=listeners,
            kraft_enable=kraft_enable,
            node_id=node_id,
            partitions_default=partitions_default,
            kafka_home_dir=app_home_dir
        )
        exec_shell_command(
            f"{bin_dir}/kafka-storage.sh format --config {kraft_conf} --cluster-id {cluster_id}",
            "kafka storage format", output=True)
        exec_shell_command(f"{bin_dir}/kafka-server-start.sh -daemon {kraft_conf}",
                           "kafka server start", output=True)
    else:
        zk_addr = params_dict["zookeeper.address"]
        generate_config_file(
            template_str=kafka_conf_template,
            conf_file=server_conf,
            broker_id=broker_id,
            partitions_default=partitions_default,
            zk_addr=zk_addr,
            broker=broker,
            kraft_enable=kraft_enable,
            kafka_home_dir=app_home_dir
        )
        exec_shell_command(f"{bin_dir}/kafka-server-start.sh -daemon {server_conf}",
                           "kafka server start", output=True)

    configure_environment("KAFKA_HOME", app_home_dir)
    print("Kafka installation completed")


if __name__ == '__main__':
    kafka_conf_template = """
# 基础配置
{% if kraft_enable == "true" %}
node.id={{ node_id }}
process.roles={{ process_roles }}
listeners={{ listeners }}
advertised.listeners={{ broker }}
controller.quorum.voters={{ controller_quorums }}
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
{% else %}
broker.id={{ broker_id }}
listeners={{ broker }}
advertised.listeners={{ broker }}
zookeeper.connect={{ zk_addr }}
zookeeper.connection.timeout.ms=3000
listener.security.protocol.map=PLAINTEXT:PLAINTEXT
{% endif %}
# 分区
num.partitions={{ partitions_default }}
min.insync.replicas={{ partitions_default }}
default.replication.factor={{ partitions_default }}
offsets.topic.replication.factor={{ partitions_default }}
transaction.state.log.replication.factor={{ partitions_default }}
transaction.state.log.min.isr={{ partitions_default }}
# 日志存储
log.dirs={{ kafka_home_dir }}/data/message
metadata.log.dir={{ kafka_home_dir }}/data/metadata
log.retention.hours=3
log.segment.bytes=1073741824
log.retention.bytes=10737418240
log.flush.interval.ms=10000
log.cleanup.policy=delete
log.retention.check.interval.ms=300000
log.flush.interval.messages=10000
unclean.leader.election.enable=false

# 线程数
num.network.threads=3
num.io.threads=8

# 元数据刷新
metadata.max.age.ms=300000

# 副本同步
replica.fetch.min.bytes=524288
replica.fetch.max.bytes=2097152
replica.lag.time.max.ms=10000

# 最大消息大小
message.max.bytes=20971520

# 受控关闭
controlled.shutdown.enable=true
controlled.shutdown.max.retries=3
"""
    kafka_class_name = ["kafka.Kafka"]
    kill_service(kafka_class_name)
    unzip_package()
    install_kafka()
