# -*- coding=utf-8 -*-
from commons import *


def install_zk():
    zoo_conf_template = """
tickTime=2000
initLimit=10
syncLimit=5
clientPort=2181
dataDir={{ zk_home_dir }}
autopurge.snapRetainCount=5
autopurge.purgeInterval=12
maxClientCnxns=1000
minSessionTimeout=10000
maxSessionTimeout=60000
admin.enableServer="false"
admin.serverPort=9999
{% if install_role == "cluster" %}
{% for ip in install_ip %}
server.{{ install_ip.index(ip) }}={{ ip }}}:2888:3888
{% endfor %}
{% endif %}
"""

    jvm_heap_size = params_dict["jvm.heapsize"]

    app_home_dir = get_app_home_dir()
    zk_home_dir = os.path.join(app_home_dir, module_name)
    zk_conf_file = os.path.join(zk_home_dir, "conf", "zoo.cfg")
    java_env_file = os.path.join(zk_home_dir, "conf", "java.env")
    zk_env_file = os.path.join(zk_home_dir, "bin", "zkEnv.sh")
    zk_server_file = os.path.join(zk_home_dir, "bin", "zkServer.sh")
    zk_myid_file = os.path.join(zk_home_dir, "myid")

    generate_config_file(
        template_str=zoo_conf_template,
        conf_file=zk_conf_file,
        install_role=install_role,
        install_ip=install_ip,
        zk_home_dir=zk_home_dir,
    )
    if install_role == "cluster":
        for id in range(len(install_ip)):
            if local_ip == install_ip[id]:
                with open(zk_myid_file, "w") as f1:
                    f1.write(str(id))
    exec_shell_command(f"mkdir -p {zk_home_dir}/tmp")
    exec_shell_command(f"sed  -i \"44 i JMXDISABLE=true\" {zk_server_file}")

    exec_shell_command(
        f"echo \"export GC_OPTS='-XX:+UseG1GC -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime -XX:+HeapDumpOnOutOfMemoryError -Xloggc:{zk_home_dir}/logs/gc.log -XX:HeapDumpPath={zk_home_dir}/logs/heapdump.hprof' -Djava.io.tmpdir={zk_home_dir}/tmp\" >> {java_env_file}")
    exec_shell_command(
        f"echo \"export JVMFLAGS='-Xms{jvm_heap_size} -Xmx{jvm_heap_size} $GC_OPTS $JVMFLAGS'\" >> {java_env_file}")

    exec_shell_command(f"head -n -7 {zk_env_file} > {zk_env_file}")
    set_permissions(zk_home_dir)
    print("zk 安装完成")
    exec_shell_command(f"{zk_home_dir}/bin/zkServer.sh start")
    print("zk 启动完成")


if __name__ == '__main__':
    unzip_package()
    install_zk()
