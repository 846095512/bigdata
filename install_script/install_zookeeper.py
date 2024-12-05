# -*- coding=utf-8 -*-
from commons import *


def install_zk():
    jvm_heap_size = params_dict["jvm.heapsize"]
    zk_conf_file = os.path.join(app_home_dir, "conf", "zoo.cfg")
    java_env_file = os.path.join(app_home_dir, "conf", "java.env")
    zk_env_file = os.path.join(app_home_dir, "bin", "zkEnv.sh")
    zk_myid_file = os.path.join(app_home_dir, "myid")

    generate_config_file(
        template_str=zoo_conf_template,
        conf_file=zk_conf_file,
        install_role=install_role,
        install_ip=install_ip,
        zk_home_dir=app_home_dir,
    )
    if install_role == "cluster":
        for id in range(len(install_ip)):
            if local_ip == install_ip[id]:
                with open(zk_myid_file, "w") as f1:
                    f1.write(str(id))
                exec_shell_command(f"echo 'export ZOO_MYID={id}' >> {zk_env_file}")
    exec_shell_command(f"mkdir -p {app_home_dir}/tmp")
    exec_shell_command(
        f"""echo 'export GC_OPTS="-XX:+UseG1GC -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime -XX:+HeapDumpOnOutOfMemoryError  -Djava.io.tmpdir={app_home_dir}/tmp" '  >> {java_env_file} """)
    exec_shell_command(
        f"""echo 'export JVMFLAGS="-Xms{jvm_heap_size} -Xmx{jvm_heap_size} ${{GC_OPTS}} ${{JVMFLAGS}}" ' >> {java_env_file}""")
    exec_shell_command(f"sed -i '/ZK_SERVER_HEAP=/s/^/#/'  {zk_env_file}")
    exec_shell_command(f"sed -i '/ZK_CLIENT_HEAP=/s/^/#/'  {zk_env_file}")
    exec_shell_command(
        f"sed -i 's|^export SERVER_JVMFLAGS=.*$|export SERVER_JVMFLAGS=\"-Xloggc:{app_home_dir}/logs/server-gc.log -XX:HeapDumpPath={app_home_dir}/logs/server-heapdump.hprof ${{SERVER_JVMFLAGS}}\"| '  {zk_env_file}")
    exec_shell_command(
        f"sed -i 's|^export CLIENT_JVMFLAGS=.*$|export CLIENT_JVMFLAGS=\"-Xloggc:{app_home_dir}/logs/cli-gc.log -XX:HeapDumpPath={app_home_dir}/logs/cli-heapdump.hprof/ ${{CLIENT_JVMFLAGS}}\"|'  {zk_env_file}")
    exec_shell_command(f"echo 'export ZOOKEEPER_HOME={app_home_dir}' >> {zk_env_file}")
    set_permissions(app_home_dir)
    exec_shell_command(f"{app_home_dir}/bin/zkServer.sh start",
                       "zookeeper start", output=True)

    configure_environment("ZOOKEEPER_HOME", app_home_dir)
    print("zookeeper installation completed")


if __name__ == '__main__':
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
admin.enableServer=false
admin.serverPort=9999
{% if install_role == "cluster" %}
{% for ip in install_ip %}
server.{{ install_ip.index(ip) }}={{ ip }}:2888:3888
{% endfor %}
{% endif %}
"""
    zookeeper_class = ["org.apache.zookeeper.server.quorum.QuorumPeerMain"]
    kill_service(zookeeper_class)
    unzip_package()
    install_zk()
