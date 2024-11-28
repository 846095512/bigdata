# -*- coding: utf-8 -*-
import math

from commons import *


def install_kafka():
    kafka_conf_template="""
# 基础配置
broker.id={{ broker_id }}
{% if kraft_enable == "true" %}
process.roles=broker,controller
listeners=PLAINTEXT://{{ broker_list }},CONTROLLER://{{ controller_list }}
advertised.listeners=PLAINTEXT://{{ local_ip }},
controller.quorum.voters={{ broker_id }}@{{ local_ip }}
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
{% else %}
listeners=PLAINTEXT://{{ broker_list }}
advertised.listeners=PLAINTEXT://{{ local_ip }}
zookeeper.connect={{ zk_addr }}
zookeeper.connection.timeout.ms=3000
listener.security.protocol.map=PLAINTEXT:PLAINTEXT
{% endif %}
# 分区
num.partitions={{ partitions_default }}
min.insync.replicas={{ partitions_default }}
default.replication.factor={{ partitions_default }}

# 日志存储
log.dirs={{ kafka_home_dir }}/message
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
    app_home_dir = get_app_home_dir()
    kafka_home_dir = os.path.join(app_home_dir, module_name)
    partitions_default = math.ceil(len(install_ip) / 2)
    for i in len(install_ip):
        if local_ip == install_ip.index(i):
            broker_id = i
    if params_dict["kraft.enable"]:
        pass
    else:
        zk_addr = params_dict["zookeeper.address"]
