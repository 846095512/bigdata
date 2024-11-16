# -*- coding: utf-8 -*-
import ipaddress
import json
import math
import os
import shutil
import subprocess
import sys
import tarfile
import zipfile

from jinja2 import Template


class Commons:

    def is_valid_ip_nums(self, *args):
        for arg in args:
            if isinstance(arg, str):
                self.is_valid_ip(arg)
            else:
                ip_nums = len(arg)
                if ip_nums >= 1 and ip_nums % 2 != 0:
                    for ip in arg:
                        self.is_valid_ip(ip)
                    return True
                else:
                    print("ip地址格式错误或ip地址个数不正确,请检查ip配置参数")
                    sys.exit(1)

    def is_valid_ip(self, ip):
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            print("ip 地址不合法")
            sys.exit(1)

    def get_user_env_filename(self):
        os_name = self.get_os_name()
        current_user = os.getlogin()

        if os_name == "ubuntu":
            return f"/home/{current_user}/.profile"
        else:
            return f"/home/{current_user}/.bash_profile"

    def get_os_name(self):
        command = "cat /etc/*-release | grep -wi 'id' | awk -F '=' '{print $2}' | sed 's/\"//g' | tr 'A-Z' 'a-z' "
        result = str(subprocess.run(command, shell=True, capture_output=True, text=True).stdout).strip()
        return result

    def get_install_config(self):
        print("开始获取安装参数")
        with open('install_conf.json', "r", encoding="utf-8") as f:
            params_dit = json.load(f)
        print("获取安装参数完成")
        return params_dit

    def get_root_dir(self):
        current_user = os.getlogin()
        if current_user == "root":
            root_dir = "/opt/app"
        else:
            root_dir = "/home/" + current_user + "/app"
        if not os.path.exists(root_dir):
            os.makedirs(root_dir)
        print(f"当前用户为{current_user},安装目录为{root_dir}")
        return root_dir

    def exec_shell_command(self, command):
        print(f"开始执行shell指令,指令{command}")
        try:
            result = subprocess.run(command, shell=True, capture_output= True, text=True, check=True)
            return str(result.stdout).strip()
        except subprocess.CalledProcessError as e:
            print(f"指令{command}执行出错")
            print(e)

    def set_permissions(self, path):
        current_user = os.getlogin()
        print("开始设置目录权限")
        for dirpath, dirnames, filenames in os.walk(path):
            for dirname in dirnames:
                os.chmod(os.path.join(dirpath, dirname), 0o750)
            for filename in filenames:
                if filename.endswith(".sh"):
                    os.chmod(os.path.join(dirpath, filename), 0o750)
                os.chmod(os.path.join(dirpath, filename), 0o550)
        self.exec_shell_command(f"chown -R {current_user}:{current_user} {path}")
        print("设置目录权限结束")

    def unzip_package(self):
        # 解析参数
        args = self.get_install_config()
        filename = args["file"]
        module_name = args["module"]

        root_dir = self.get_root_dir()
        print(f"root_dir is {root_dir}")

        # 获取文件后缀
        if filename.endswith('.tar.gz'):
            filename_suffix = ".tar.gz"
        elif filename.endswith('.zip'):
            filename_suffix = ".zip"
        else:
            print("不支持解压的类型！！")
            sys.exit(1)
        print(f"文件为{filename_suffix}压缩类型")

        if filename_suffix == ".tar.gz" or filename_suffix == ".tgz":
            with tarfile.open(filename, 'r') as tar_ref:
                tar_ref.extractall(root_dir)
        elif filename_suffix == ".zip":
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(root_dir)
        else:
            print("不支持的压缩包类型")

        print(f"文件解压完成")
        
        command = "tar -tzf " + filename + " | head -1 | cut -d'/' -f1"
        unpack_name = self.exec_shell_command(command)
        unpack_name = str(unpack_name.stdout).strip()
        old_path = os.path.join(root_dir, unpack_name)
        new_path = os.path.join(root_dir, module_name)

        shutil.move(old_path, new_path)

        print(f"目录移动完成，{old_path} -> {new_path}")



    def generate_config_file(self, template_str, conf_file, keyword, **kwargs):
        print(f"开始生成{conf_file}文件")
        template = Template(template_str)
        config_content = template.render(**kwargs)
        if keyword == "":
            insert_line_num = 1
        insert_line_num = self.exec_shell_command(f"sed -n \"/{keyword}/=\" {conf_file}")
        with open(conf_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            lines.insert(int(insert_line_num),config_content)
        with open(conf_file, "w", encoding="utf-8") as f:
            f.writelines(lines)
        print(f"生成{conf_file}文件完成")


    def install_jdk(self):
        args = self.get_install_config()
        module_name = args["module"]

        env_file = self.get_user_env_filename()
        app_home = os.path.join(self.get_root_dir(), module_name)

        self.set_permissions(app_home)

        print("设置环境变量中")
        with open(env_file, "a+", encoding="UTF-8") as f:
            f.write(f"export JAVA_HOME={app_home}\n")
            f.write(f"export PATH=$PATH:$JAVA_HOME/bin\n")
        print(f"source {env_file}")
        self.exec_shell_command(f"source {env_file}")
        print("jdk 安装完成!!!!")

    def install_zk(self):
        conf_items = {
            "tickTime": 2000,
            "initLimit": 10,
            "syncLimit": 5,
            "clientPort": 2181,
            "dataDir": "",
            "autopurge.snapRetainCount": 5,
            "autopurge.purgeInterval": 12,
            "maxClientCnxns": 1000,
            "minSessionTimeout": 10000,
            "maxSessionTimeout": 60000,
            "admin.enableServer": "false",
            "admin.serverPort": 9999
        }

        params_dit = self.get_install_config()
        module = params_dit["module"]
        jvm_heap_size = params_dit["jvm_heap_size"]
        root_dir = self.get_root_dir()
        zk_home_dir = os.path.join(root_dir, module)
        zk_conf_file = os.path.join(zk_home_dir, "conf", "zoo.cfg")
        zk_log4j_file = os.path.join(zk_home_dir, "conf", "log4j.properties")
        java_env_file = os.path.join(zk_home_dir, "conf", "java-env")
        zk_env_file = os.path.join(zk_home_dir, "bin", "zkEnv.sh")
        zk_myid_file = os.path.join(zk_home_dir, "myid")


        if params_dit["install_role"] == "standalone":
            with open(zk_conf_file, "w", encoding="UTF-8") as f:
                for item in conf_items:
                    if item == "dataDir":
                        conf_items["dataDir"] = zk_home_dir
                    f.write(f"{item}={conf_items.get(item)}\n")

        elif params_dit["install_role"] == "cluster":
            with open(zk_conf_file, "w", encoding="UTF-8") as f:
                for item in conf_items:
                    if item == "dataDir":
                        conf_items["dataDir"] = zk_home_dir
                    f.write(f"{item}={conf_items.get(item)}\n")
                id = 1
                for ip in params_dit["install_ip"]:
                    f.write(f"server.{id}={ip}:2888:3888\n")
                    if ip == params_dit["local_ip"]:
                        self.exec_shell_command(f"echo {id} > {zk_myid_file}")
                    id += 1


        self.exec_shell_command(f"sed -i \"s/zookeeper.root.logger=.*/zookeeper.root.logger=INFO, ROLLINGFILE/g\" {zk_log4j_file}")
        self.exec_shell_command(f"echo \"export JVMFLAGS='-Xms{jvm_heap_size} -Xmx{jvm_heap_size} -XX:+UseG1GC -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime -XX:+HeapDumpOnOutOfMemoryError -Xloggc:{zk_home_dir}/logs/gc.log -XX:HeapDumpPath:{zk_home_dir}/logs/heapdump.hprof'\" > {java_env_file}")
        self.exec_shell_command(f"sed -i \"s/ZOO_LOG4J_PROP=.*/ZOO_LOG4J_PROP=\"INFO,ROLLINGFILE\"/g\" {zk_env_file}")

        with open(zk_env_file, "r", encoding="UTF-8") as f:
            env_lines = f.readlines()
        with open(zk_env_file, "w", encoding="UTF-8") as f:
            for line in env_lines:
                f.write(line)
                if line.startswith("# default heap for zookeeper server"):
                    break
        self.set_permissions(zk_home_dir)
        print("zk 安装完成")
        # print("zk 启动中...")
        # subprocess.run(f"{zk_home_dir}/bin/zkServer.sh start", shell=True)
        # print("zk 启动完成")
        
    def install_hadoop(self):
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

    <property>
        <name>dfs.namenode.secondary.http-address.{{ dfs_nameservice }}.nn{{ namenode_list.index(namenode) + 1 }}</name>
        <value>{{ namenode }}:9868</value>
    </property>
    {% endfor %}

    <property>
        <name>dfs.journalnode.rpc-address</name>
        <value>0.0.0.0:8485</value>
    </property>
    <property>
        <name>dfs.journalnode.http-addres</name>
        <value>0.0.0.0:8480</value>
    </property>
    <property>
        <name>dfs.ha.automatic-failover.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.client.failover.proxy.provider.{{ dfs_nameservice }}</name>
        <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
    </property>
    <property>
        <name>dfs.ha.fencing.methods</name>
        <value>shell({{ hadoop_conf_dir }}/fencing.sh)</value>
    </property>
    <property>
        <name>dfs.namenode.shared.edits.dir</name>
        <value>qjournal://{{ journal_quorm }}/{{ dfs_nameservice }}</value>
    </property>
    {% endif %}
    <property>
        <name>dfs.hosts.exclude</name>
        <value>{{ hadoop_conf_dir }}</value>
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
        <name>yarn.resourcemanager.ha.automatic-failover.zk-base-path</name>
        <value>/{{ yarn_cluster_id }}/yarn-leader-election</value>
    </property>
    <property>
        <name>yarn.resourcemanager.recovery.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.resourcemanager.zk-state-store.parent-path</name>
        <value>/{{ yarn_cluster_id }}/yarn-store</value>
    </property>
    <property>
        <name>yarn.resourcemanager.store.class</name>
        <value>org.apache.hadoop.yarn.server.resourcmanager.recovery.ZKRMStateStore</value>
    </property>
    {% endif %}
    <!-- nodemanager配置一 -->
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
        mmapred_conf_template = """
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
        <value>hdfs://{{ dfs_nameservice }}/mr-history/tmp</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.done-dir</name>
        <value>hdfs://{{ dfs_nameservice }}/mr-history/done</value>
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
export HADOOP_LOG_DIR={{ hadoop_home_dir }}/logs
export HADOOP_PID_DIR={{ hadoop_home_dir }}/pids
export HADOOP_TMP_DIR={{ hadoop_home_dir }}/tmp
export HDFS_NAMENODE_USER={{ current_user }}
export HDFS_DATANODE_USER={{ current_user }}
export HDFS_SECONDARYNAMENODE_USER={{ current_user }}
export YARN_RESOURECEMANAGER_USER={{ current_user }}
export YARN_NODEMANAGER_USER={{ current_user }}
export MAPRED_HISTORYSERVER_USER={{ current_user }}


export HADOOP_OPTS="{{ hadoop_opts }}"
export HDFS_NAMENODE_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} {hadoop_opts }}"
export HDFS_SECONDARYNAMENODE_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} {hadoop_opts }}"
export YARN_RESOURECEMANAGER_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} {hadoop_opts }}"
export YARN_NODEMANAGER_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} {hadoop_opts }}"
export YARN_PROXYSERVER_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} {hadoop_opts }}"
export MAPRED_HISTORYSERVER_OPTS="-Xms{{ jvm_heap_size }} -Xmx{{ jvm_heap_size }} {hadoop_opts }}"
"""


        param_dict = self.get_install_config()
        namenode_list = param_dict["namenode.list"]
        nn_list = ",".join([f"nn{i + 1}" for i in range(len(namenode_list))])
        resourcemanager_list  = param_dict["resourcemanager.list"]
        rm_list = ",".join([f"rm{i + 1}" for i in range(len(resourcemanager_list))])
        journal_quorm = ";".join([f"{i}:8485" for i in param_dict["journalnode.list"]])
        dfs_nameservice = param_dict["dfs.nameservice"]
        yarn_cluster_id = param_dict["yarn.cluster.id"]
        install_role = param_dict["install.role"]
        zk_addr = param_dict["zk.addr"]
        module =  param_dict["module"]
        yarn_mem =  param_dict["yarn.mem"]
        yarn_cpu =  param_dict["yarn.cpu"]
        local_ip =  param_dict["local.ip"]
        jvm_heap_size = param_dict["jvm.heapsize"]
        install_ip = param_dict["install.ip"]
        if len(install_ip) == 1:
            dfs_replication = 1
        else:
          dfs_replication =  math.ceil(len(install_ip) / 2)
        
        

        root_dir = self.get_root_dir()
        current_user = os.getlogin()
        hadoop_home_dir = os.path.join(root_dir, module)
        hadoop_conf_dir = os.path.join(hadoop_home_dir, "etc/hadoop")
        hadoop_bin_dir = os.path.join(hadoop_home_dir,"bin")
        hadoop_data_dir = os.path.join(hadoop_home_dir,"data")
        fencing_file = os.path.join(hadoop_conf_dir,"fencing.sh")
        hadoop_env_file = os.path.join(hadoop_conf_dir,"hadoop-env.sh")
        core_conf = os.path.join(hadoop_conf_dir,"core-site.xml")
        hdfs_conf = os.path.join(hadoop_conf_dir,"hdfs-site.xml")
        yarn_conf = os.path.join(hadoop_conf_dir,"yarn-site.xml")
        mapred_conf = os.path.join(hadoop_conf_dir,"mapred-site.xml")
        hadoop_opts = "-XX:+UseG1GC -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime -XX:+HeapDumpOnOutOfMemoryError"

        with open(fencing_file, "w", encoding="utf-8") as f:
            f.write("#!/bin/bash\n\n\n")
            f.write(f"{hadoop_bin_dir}/hdfs --daemon stop namenode\n")
            f.write(f"{hadoop_bin_dir}/hdfs --daemon stop resourcemanager\n")

        # 生成 core-site hdfs-site yarn-site mapred-site文件
        self.generate_config_file(template_str=core_conf_template,
                                  conf_file=core_conf,
                                  keyword="<configuration>",
                                  install_role=install_role,
                                  dfs_nameservice=dfs_nameservice,
                                  hadoop_data_dir=hadoop_data_dir,
                                  zk_addr=zk_addr)
        
        self.generate_config_file(template_str=hdfs_conf_template,
                            conf_file=hdfs_conf,
                            keyword="<configuration>",
                            install_role=install_role,
                            dfs_nameservice=dfs_nameservice,
                            hadoop_conf_dir=hadoop_conf_dir,
                            journal_quorm=journal_quorm,
                            namenode_list=namenode_list,
                            nn_list=nn_list,
                            dfs_replication=dfs_replication)

        self.generate_config_file(template_str=yarn_conf_template,
                    conf_file=yarn_conf,
                    keyword="<configuration>",
                    install_role=install_role,
                    local_ip=local_ip,
                    hadoop_conf_dir=hadoop_conf_dir,
                    resourcemanager_list=resourcemanager_list,
                    yarn_mem=yarn_mem,
                    yarn_cpu=yarn_cpu,
                    yarn_cluster_id=yarn_cluster_id,
                    rm_list=rm_list)
        
        self.generate_config_file(template_str=mmapred_conf_template,
                                conf_file=mapred_conf,
                                keyword="<configuration>",
                                dfs_nameservice=dfs_nameservice)
        
        self.generate_config_file(template_str=env_conf_template,
                                conf_file=hadoop_env_file,
                                keyword="<configuration>",
                                current_user=current_user,
                                hadoop_home_dir=hadoop_home_dir,
                                hadoop_conf_dir=hadoop_conf_dir,
                                hadoop_opts=hadoop_opts,
                                jvm_heap_size=jvm_heap_size)

        self.set_permissions(hadoop_home_dir)






if __name__ == '__main__':

    def test(*args):
        for arg in args:
            print(arg)
    test(1,2,2,3,4,5)
    # obj = Commons()
    # obj.unzip_package()
    # obj.install_jdk()
    # obj.install_zk()
    # obj.install_hadoop()

    
    # print(obj.exec_shell_command("diasasdr"))
  








 


