# -*- coding: utf-8 -*-
from commons import *


def install_zk():
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

    module = params_dict["module.name"]
    jvm_heap_size = params_dict["jvm.heapsize"]

    app_home_dir = get_app_home_dir()
    zk_home_dir = os.path.join(app_home_dir, module)
    zk_conf_file = os.path.join(zk_home_dir, "conf", "zoo.cfg")
    zk_log4j_file = os.path.join(zk_home_dir, "conf", "log4j.properties")
    java_env_file = os.path.join(zk_home_dir, "conf", "java.env")
    zk_env_file = os.path.join(zk_home_dir, "bin", "zkEnv.sh")
    zk_server_file = os.path.join(zk_home_dir, "bin", "zkServer.sh")
    zk_myid_file = os.path.join(zk_home_dir, "myid")

    if params_dict["install.role"] == "standalone":
        with open(zk_conf_file, "w", encoding="UTF-8") as f:
            for item in conf_items:
                if item == "dataDir":
                    conf_items["dataDir"] = zk_home_dir
                f.write(f"{item}={conf_items.get(item)}\n")
    if params_dict["install.role"] == "cluster":
        with open(zk_conf_file, "w", encoding="UTF-8") as f:
            for item in conf_items:
                if item == "dataDir":
                    conf_items["dataDir"] = zk_home_dir
                f.write(f"{item}={conf_items.get(item)}\n")
            id = 1
            for ip in params_dict["install_ip"]:
                f.write(f"server.{id}={ip}:2888:3888\n")
                if ip == params_dict["local_ip"]:
                    exec_shell_command(f"echo {id} > {zk_myid_file}")
                id += 1

    exec_shell_command(f"sed  -i \"44 i JMXDISABLE=true\" {zk_server_file}")
    exec_shell_command(
        f"sed -i \"s/zookeeper.root.logger=.*/zookeeper.root.logger=INFO, ROLLINGFILE/g\" {zk_log4j_file}")
    exec_shell_command(
        f"echo \"export JVMFLAGS='-Xms{jvm_heap_size} -Xmx{jvm_heap_size} -XX:+UseG1GC -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime -XX:+HeapDumpOnOutOfMemoryError -Xloggc:{zk_home_dir}/logs/gc.log -XX:HeapDumpPath={zk_home_dir}/logs/heapdump.hprof $JVMFLAGS'\" > {java_env_file}")
    exec_shell_command(f"sed -i \"s/ZOO_LOG4J_PROP=.*/ZOO_LOG4J_PROP=\"INFO,ROLLINGFILE\"/g\" {zk_env_file}")

    with open(zk_env_file, "r", encoding="UTF-8") as f:
        env_lines = f.readlines()

    with open(zk_env_file, "w", encoding="UTF-8") as f:
        for line in env_lines:
            f.write(line)
            if line.startswith("# default heap for zookeeper server"):
                break
    set_permissions(zk_home_dir)
    print("zk 安装完成")
    exec_shell_command(f"{zk_home_dir}/bin/zkServer.sh start")
    print("zk 启动完成")


if __name__ == '__main__':
    unzip_package()
    install_zk()
