# -*- coding: utf-8 -*-
import math

from commons import *


def install_hadoop():
    namenode_list = params_dict["namenode.list"]
    nn_list = ",".join([f"nn{i + 1}" for i in range(len(namenode_list))])
    resourcemanager_list = params_dict["resourcemanager.list"]
    rm_list = ",".join([f"rm{i + 1}" for i in range(len(resourcemanager_list))])
    journal_quorm = ";".join([f"{i}:8485" for i in params_dict["journalnode.list"]])
    dfs_nameservice = params_dict["dfs.nameservice"]
    yarn_cluster_id = params_dict["yarn.cluster.id"]
    zk_addr = params_dict["zookeeper.address"]
    yarn_mem = params_dict["yarn.mem"]
    yarn_cpu = params_dict["yarn.cpu"]
    jvm_heap_size = params_dict["jvm.heapsize"]
    if len(install_ip) % 2 == 0:
        dfs_replication = math.ceil((len(install_ip) / 2) + 1)
    else:
        dfs_replication = math.ceil(len(install_ip) / 2)
    app_home_dir = get_app_home_dir()
    hadoop_home_dir = os.path.join(app_home_dir, module_name)
    hadoop_conf_dir = os.path.join(hadoop_home_dir, "etc/hadoop")
    hadoop_bin_dir = os.path.join(hadoop_home_dir, "bin")
    hadoop_data_dir = os.path.join(hadoop_home_dir, "data")
    hadoop_env_file = os.path.join(hadoop_conf_dir, "hadoop-env.sh")
    core_conf = os.path.join(hadoop_conf_dir, "core-site.xml")
    hdfs_conf = os.path.join(hadoop_conf_dir, "hdfs-site.xml")
    yarn_conf = os.path.join(hadoop_conf_dir, "yarn-site.xml")
    mapred_conf = os.path.join(hadoop_conf_dir, "mapred-site.xml")
    hadoop_classpath = exec_shell_command(f"{hadoop_bin_dir}/hadoop classpath")

    # 生成 core-site hdfs-site yarn-site mapred-site文件
    generate_config_file(
        template_str=core_conf_template,
        conf_file=core_conf,
        keyword="<configuration>",
        install_role=install_role,
        dfs_nameservice=dfs_nameservice,
        hadoop_data_dir=hadoop_data_dir,
        zk_addr=zk_addr
    )

    generate_config_file(
        template_str=hdfs_conf_template,
        conf_file=hdfs_conf,
        keyword="<configuration>",
        install_role=install_role,
        dfs_nameservice=dfs_nameservice,
        hadoop_conf_dir=hadoop_conf_dir,
        journal_quorm=journal_quorm,
        namenode_list=namenode_list,
        nn_list=nn_list,
        zk_addr=zk_addr,
        dfs_replication=dfs_replication
    )

    generate_config_file(
        template_str=yarn_conf_template,
        conf_file=yarn_conf,
        keyword="<configuration>",
        install_role=install_role,
        local_ip=local_ip,
        hadoop_conf_dir=hadoop_conf_dir,
        resourcemanager_list=resourcemanager_list,
        yarn_mem=yarn_mem,
        yarn_cpu=yarn_cpu,
        dfs_nameservice=dfs_nameservice,
        zk_addr=zk_addr,
        yarn_cluster_id=yarn_cluster_id,
        rm_list=rm_list,
        hadoop_classpath=hadoop_classpath
    )

    generate_config_file(
        template_str=mapred_conf_template,
        conf_file=mapred_conf,
        keyword="<configuration>",
        dfs_nameservice=dfs_nameservice
    )

    generate_config_file(
        template_str=env_conf_template,
        conf_file=hadoop_env_file,
        keyword="# export HADOOP_REGISTRYDNS_SECURE_EXTRA_OPTS",
        current_user=current_user,
        hadoop_home_dir=hadoop_home_dir,
        hadoop_conf_dir=hadoop_conf_dir,
        jvm_heap_size=jvm_heap_size
    )
    exec_shell_command(f"touch {hadoop_conf_dir}/hdfs_exclude")
    exec_shell_command(f"touch {hadoop_conf_dir}/nodemanager_exclude")
    set_permissions(hadoop_home_dir)
    exec_shell_command(f"rm -rf  {hadoop_data_dir}")

    if install_role == "standalone":
        exec_shell_command(f"{hadoop_bin_dir}/hdfs namenode -format -force",
                           "namenode format", output=True)
        exec_shell_command(f"{hadoop_bin_dir}/hdfs --daemon start namenode",
                           "namenode start", output=True)
        exec_shell_command(f"{hadoop_bin_dir}/hdfs --daemon start datanode",
                           "datanode start", output=True)
        exec_shell_command(f"{hadoop_bin_dir}/yarn --daemon start resourcemanager",
                           "resourcemanager start", output=True)
        exec_shell_command(f"{hadoop_bin_dir}/yarn --daemon start nodemanager",
                           "nodemanager start", output=True)
        exec_shell_command(f"{hadoop_bin_dir}/yarn --daemon start timelineserver",
                           "timelineserver(historyserver) start", output=True)

    if install_role == "cluster":
        exec_shell_command(f"{hadoop_bin_dir}/hdfs --daemon start journalnode",
                           "journalnode start", output=True)
        check_service(8485, "journalnode")

        if local_ip == namenode_list[0]:
            exec_shell_command(f"{hadoop_bin_dir}/hdfs namenode -format -force",
                               "namenode format", output=True)
            exec_shell_command(f"{hadoop_bin_dir}/hdfs zkfc -formatZK -force",
                               "zkfc format", output=True)
            exec_shell_command(f"{hadoop_bin_dir}/hdfs --daemon start namenode",
                               "namenode start", output=True)
            exec_shell_command(f"{hadoop_bin_dir}/hdfs --daemon start zkfc",
                               "zkfc start", output=True)
        elif local_ip in namenode_list:
            check_service(8020, "namenode", [install_ip[0]])
            exec_shell_command(f"{hadoop_bin_dir}/hdfs namenode -bootstrapStandby",
                               "namenode Synchronize metadata", output=True)
            exec_shell_command(f"{hadoop_bin_dir}/hdfs --daemon start namenode",
                               "namenode start", output=True)
            exec_shell_command(f"{hadoop_bin_dir}/hdfs --daemon start zkfc",
                               "zkfc start", output=True)
        else:
            print(f"The current host {local_ip} is not a namenode node, skipping namenode initialization.")

        exec_shell_command(f"{hadoop_bin_dir}/hdfs --daemon start datanode",
                           "datanode start", output=True)
        if local_ip in resourcemanager_list:
            exec_shell_command(f"{hadoop_bin_dir}/yarn --daemon start resourcemanager",
                               "resourcemanager start", output=True)
        exec_shell_command(f"{hadoop_bin_dir}/yarn --daemon start nodemanager",
                           "nodemanager start", output=True)
        exec_shell_command(f"{hadoop_bin_dir}/yarn --daemon start timelineserver",
                           "timelineserver(historyserver) start", output=True)
        # 创建 hdfs 目录
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /hadoop/mapreduce/event")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /hadoop/mapreduce/history")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /hadoop/share/jars")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /hadoop/share/conf")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -put {hadoop_home_dir}/share/hadoop/*/*.jar /hadoop/share/jars")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -put {hadoop_conf_dir}/core-site.xml /hadoop/share/conf")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -put {hadoop_conf_dir}/hdfs-site.xml /hadoop/share/conf")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -put {hadoop_conf_dir}/yarn-site.xml /hadoop/share/conf")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -put {hadoop_conf_dir}/mapred-site.xml /hadoop/share/conf")

        # 创建 spark 目录
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /spark/share/jars")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /spark/history/logs")

        # 创建 flink 目录
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /flink/checkpoints")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /flink/savepoints")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /flink/storage")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /flink/jobmanager/archive")
        exec_shell_command(f"{hadoop_bin_dir}/hdfs dfs -mkdir -p /flink/historyserver/archive")

        print("Hadoop installation completed")


if __name__ == '__main__':
    core_conf_template = """
    <!--  核心配置  -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://{{ dfs_nameservice }}</value>
    </property>


    <!--  通用配置  -->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>{{ hadoop_data_dir }}</value>
    </property>
    <property>
        <name>hadoop.http.filter.initializers</name>
        <value>org.apache.hadoop.security.HttpCrossOriginFilterInitializer</value>
    </property>
    <property>
        <name>hadoop.http.cross-origin.allowed-origins</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.http.cross-origin.allowed-methods</name>
        <value>GET,POST,HEAD</value>
    </property>
    <property>
        <name>hadoop.http.cross-origin.allowed-headers</name>
        <value>X-Requested-With,Content-Type,Accept,Origin</value>
    </property>
    <property>
        <name>hadoop.http.cross-origin.max-age</name>
        <value>1800</value>
    </property>
     <property>
        <name>hadoop.proxyuser.*.groups</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.*.hosts</name>
        <value>*</value>
    </property>

    {% if install_role == 'cluster' %}
    <!--  ha配置  -->
    <property>
        <name>ha.zookeeper.parent-znode</name>
        <value>/{{ dfs_nameservice }}</value>
    </property>
    <property>
        <name>ha.zookeeper.quorum</name>
        <value>{{ zk_addr }}</value>
    </property>
    {% endif %}
"""
    hdfs_conf_template = """
    <!-- 核心配置 -->
    <property>
        <name>dfs.replication</name>
        <value>{{ dfs_replication }}</value>
    </property>

        <!-- 通用配置 -->
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
        <value>false</value>
    </property>
    {% if install_role == 'standalone' %}
    <property>
        <name>dfs.namenode.rpc-address</name>
        <value>0.0.0.0:8020</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>0.0.0.0:50070</value>
    </property>
    {% endif %}
    <property>
        <name>dfs.datanode.address</name>
        <value>0.0.0.0:9866</value>
    </property>
    <property>
        <name>dfs.datanode.http.address</name>
        <value>0.0.0.0:9864</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>${hadoop.tmp.dir}/namenode-data</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>${hadoop.tmp.dir}/datanode-data</value>
    </property>
    <property>
        <name>dfs.journalnode.edits.dir</name>
        <value>${hadoop.tmp.dir}/journalnode-data</value>
    </property>
    {% if install_role == 'cluster' %}
    <!-- ha配置 -->
    <property>
        <name>dfs.nameservices</name>
        <value>{{ dfs_nameservice }}</value>
    </property>
    <property>
        <name>dfs.ha.namenodes.{{ dfs_nameservice }}</name>
        <value>{{ nn_list }}</value>
    </property>

    {% for namenode in namenode_list %}
    <property>
        <name>dfs.namenode.rpc-address.{{ dfs_nameservice }}.nn{{ namenode_list.index(namenode) + 1 }}</name>
        <value>{{ namenode }}:8020</value>
    </property>
    <property>
        <name>dfs.namenode.http-address.{{ dfs_nameservice }}.nn{{ namenode_list.index(namenode) + 1 }}</name>
        <value>{{ namenode }}:50070</value>
    </property>
    {% endfor %}

    <property>
        <name>dfs.journalnode.rpc-address</name>
        <value>0.0.0.0:8485</value>
    </property>
    <property>
        <name>dfs.journalnode.http-address</name>
        <value>0.0.0.0:8480</value>
    </property>
    <property>
        <name>dfs.ha.automatic-failover.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.ha.fencing.methods</name>
        <value>shell(/bin/true)</value>
    </property>
    <property>
        <name>dfs.client.failover.proxy.provider.{{ dfs_nameservice }}</name>
        <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
    </property>
    <property>
        <name>dfs.namenode.shared.edits.dir</name>
        <value>qjournal://{{ journal_quorm }}/{{ dfs_nameservice }}</value>
    </property>
    {% endif %}
    <property>
        <name>dfs.hosts.exclude</name>
        <value>{{ hadoop_conf_dir }}/hdfs_exclude</value>
    </property>
"""
    yarn_conf_template = """
    {% if install_role == 'standalone' %}
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>{{ local_ip }}</value>
    </property>
    {% endif %}
    {% if install_role == 'cluster' %}
    <property>
        <name>yarn.resourcemanager.cluster-id</name>
        <value>{{ yarn_cluster_id }}</value>
    </property>
    <property>
        <name>yarn.resourcemanager.ha.rm-ids</name>
        <value>{{ rm_list }}</value>
    </property>
    {% for resourcemanager in resourcemanager_list %}
    <property>
        <name>yarn.resourcemanager.hostname.rm{{ resourcemanager_list.index(resourcemanager) + 1 }}</name>
        <value>{{ resourcemanager }}</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.address.rm{{ resourcemanager_list.index(resourcemanager) + 1 }}</name>
        <value>{{ resourcemanager }}:8030</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address.rm{{ resourcemanager_list.index(resourcemanager) + 1 }}</name>
        <value>{{ resourcemanager }}:8032</value>
    </property>
    <property>
        <name>yarn.resourcemanager.admin.address.rm{{ resourcemanager_list.index(resourcemanager) + 1 }}</name>
        <value>{{ resourcemanager }}:8033</value>
    </property>
    <property>
        <name>yarn.web-proxy.address.rm{{ resourcemanager_list.index(resourcemanager) + 1 }}</name>
        <value>{{ resourcemanager }}:8034</value>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address.rm{{ resourcemanager_list.index(resourcemanager) + 1 }}</name>
        <value>{{ resourcemanager }}:8088</value>
    </property>
    {% endfor %}
    <property>
        <name>yarn.resourcemanager.ha.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.resourcemanager.ha.automatic-failover.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.resourcemanager.zk-address</name>
        <value>{{ zk_addr }}</value>
    </property>
    <property>
        <name>yarn.resourcemanager.ha.automatic-failover.zk-base-path</name>
        <value>/{{ dfs_nameservice }}/{{ yarn_cluster_id }}/yarn-leader-election</value>
    </property>
    <property>
        <name>yarn.resourcemanager.recovery.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.resourcemanager.zk-state-store.parent-path</name>
        <value>/{{ dfs_nameservice }}/{{ yarn_cluster_id }}/yarn-store</value>
    </property>
    <property>
        <name>yarn.resourcemanager.store.class</name>
        <value>org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore</value>
    </property>
    {% endif %}
    <!-- nodemanager配置一 -->
    <property>
        <name>yarn.resourcemanager.nodes.exclude-path</name>
        <value>{{ hadoop_conf_dir }}/nodemanager_exclude</value>
    </property>
    <property>
        <name>yarn.nodemanager.localizer.address</name>
        <value>0.0.0.0:8040</value>
    </property>
    <property>
        <name>yarn.nodemanager.address</name>
        <value>0.0.0.0:8041</value>
    </property>
    <property>
        <name>yarn.nodemanager.webapp.address</name>
        <value>0.0.0.0:8042</value>
    </property>
    <property>
        <name>yarn.resourcemanager.am.max-attempts</name>
        <value>4</value>
    </property>

    <!-- 日志聚合配置 -->
    <property>
        <name>yarn.log-aggregation-enable</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.log-aggregation.retain-seconds</name>
        <value>604800</value>
    </property>
    <property>
        <name>yarn.log-aggregation.retain-check-interval-seconds</name>
        <value>3600</value>
    </property>
    <property>
        <name>yarn.nodemanager.log-aggregation.compression-type</name>
        <value>GZIP</value>
    </property>
    <property>
        <name>yarn.nodemanager.local-dirs</name>
        <value>${hadoop.tmp.dir}/yarn</value>
    </property>
    <property>
        <name>yarn.log.server.url</name>
        <value>http://{{ resourcemanager_list[0] }}:19888/jobhistory/logs</value>
    </property>

    <!-- yarn资源配置 -->
    <property>
        <name>yarn.application.classpath</name>
        <value>{{ hadoop_classpath }}</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>{{ yarn_mem }}</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>{{ yarn_cpu }}</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>{{ yarn_mem }}</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-vcores</name>
        <value>{{ yarn_cpu }}</value>
    </property>

    <!-- yarn外部拓展 -->
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle,spark_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.spark_shuffle.class</name>
        <value>org.apache.spark.network.yarn.YarnShuffleService</value>
    </property>
"""
    mapred_conf_template = """
    <!-- 核心配置 -->
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>0.0.0.0:10020</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>0.0.0.0:19888</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.intermediate-done-dir</name>
        <value>hdfs://{{ dfs_nameservice }}/hadoop/mapreduce/event</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.done-dir</name>
        <value>hdfs://{{ dfs_nameservice }}/hadoop/mapreduce/history</value>
    </property>

    <!-- 作业压缩配置 -->
    <property>
        <name>mapreduce.map.output.compress</name>
        <value>true</value>
    </property>
    <property>
        <name>mapreduce.map.output.compress.codec</name>
        <value>org.apache.hadoop.io.compress.GzipCodec</value>
    </property>
    <property>
        <name>mapreduce.map.output.compress.type</name>
        <value>BLOCK</value>
    </property>
    <property>
        <name>mapreduce.output.fileoutputformat.compress</name>
        <value>true</value>
    </property>
    <property>
        <name>mapreduce.output.fileoutputformat.compress.codec</name>
        <value>org.apache.hadoop.io.compress.GzipCodec</value>
    </property>
    <property>
        <name>mapreduce.output.fileoutputformat.compress.type</name>
        <value>BLOCK</value>
    </property>
    <property>
        <name>mapreduce.reduce.output.compress</name>
        <value>true</value>
    </property>
    <property>
        <name>mapreduce.reduce.output.compress.codec</name>
        <value>org.apache.hadoop.io.compress.GzipCodec</value>
    </property>
    <property>
        <name>mapreduce.reduce.output.compress.type</name>
        <value>BLOCK</value>
    <!-- 作业重启配置 -->
    </property>
        <property>
        <name>mapreduce.job.failures.tolerated</name>
        <value>3</value>
    </property>
    <property>
        <name>mapreduce.map.maxattempts</name>
        <value>3</value>
    </property>
    <property>
        <name>mapreduce.reduce.maxattempts</name>
        <value>3</value>
    </property>
"""
    env_conf_template = """
export HADOOP_HOME={{ hadoop_home_dir }}
export HADOOP_CONF_DIR={{ hadoop_conf_dir }}
export HADOOP_LOG_DIR=${HADOOP_HOME}/logs
export HADOOP_PID_DIR=${HADOOP_HOME}/pid
export HADOOP_TMP_DIR=${HADOOP_HOME}/tmp
export HDFS_NAMENODE_USER={{ current_user }}
export HDFS_DATANODE_USER={{ current_user }}
export HDFS_SECONDARYNAMENODE_USER={{ current_user }}
export YARN_RESOURCEMANAGER_USER={{ current_user }}
export YARN_NODEMANAGER_USER={{ current_user }}
export MAPRED_HISTORYSERVER_USER={{ current_user }}

export GC_OPTS="-XX:+UseG1GC -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime -XX:+HeapDumpOnOutOfMemoryError"
export HDFS_NAMENODE_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} ${GC_OPTS} -Xloggc:{{ hadoop_home_dir }}/logs/namenode-gc.log -XX:HeapDumpPath={{ hadoop_home_dir }}/logs/namenode-heapdump.hprof"
export HDFS_DATANODE_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} ${GC_OPTS} -Xloggc:{{ hadoop_home_dir }}/logs/datanode-gc.log -XX:HeapDumpPath={{ hadoop_home_dir }}/logs/datanode-heapdump.hprof"
export HDFS_SECONDARYNAMENODE_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} ${GC_OPTS} -Xloggc:{{ hadoop_home_dir }}/logs/secondarynamenode-gc.log -XX:HeapDumpPath={{ hadoop_home_dir }}/logs/secondarynamenode-heapdump.hprof"
export YARN_RESOURCEMANAGER_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} ${GC_OPTS} -Xloggc:{{ hadoop_home_dir }}/logs/datanode-gc.log -XX:HeapDumpPath={{ hadoop_home_dir }}/logs/datanode-heapdump.hprof"
export YARN_NODEMANAGER_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} ${GC_OPTS} -Xloggc:{{ hadoop_home_dir }}/logs/resourcemanager-gc.log -XX:HeapDumpPath={{ hadoop_home_dir }}/logs/resourcemanager-heapdump.hprof"
export MAPRED_HISTORYSERVER_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} ${GC_OPTS} -Xloggc:{{ hadoop_home_dir }}/logs/historyserver-gc.log -XX:HeapDumpPath={{ hadoop_home_dir }}/logs/historyserver-heapdump.hprof"
"""
    hadoop_class = ["org.apache.hadoop.hdfs.server.namenode.NameNode",
                    "org.apache.hadoop.hdfs.server.datanode.DataNode",
                    "org.apache.hadoop.hdfs.qjournal.server.JournalNode",
                    "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager",
                    "org.apache.hadoop.yarn.server.nodemanager.NodeManager",
                    "org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer",
                    "org.apache.hadoop.hdfs.tools.DFSZKFailoverController"]

    kill_service(hadoop_class)
    unzip_package()
    install_hadoop()
