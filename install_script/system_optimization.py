# -*- coding: utf-8 -*-
import subprocess

from commons import *


def init_os_conf():
    # 时区设置
    exec_shell_command("timedatectl set-timezone Asia/Shanghai")

    # 交换分区
    exec_shell_command("swapoff -a")
    exec_shell_command("sed -i '/swap/s/^/#/' /etc/fstab")

    # cpu 透明大页关闭
    exec_shell_command("echo never > /sys/kernel/mm/transparent_hugepage/enabled")
    exec_shell_command("echo never > /sys/kernel/mm/transparent_hugepage/defrag")

    # 系统文件描述符 进程限制
    res = subprocess.run("grep -q '* soft nofile 1000000' /etc/security/limits.conf", shell=True, capture_output=True, text=True)
    if res.returncode:
        exec_shell_command("echo '* soft nofile 1000000'  >> /etc/security/limits.conf")
        exec_shell_command("echo '* hard nofile 1000000'  >> /etc/security/limits.conf")
        exec_shell_command("echo '* soft nproc 1000000'  >> /etc/security/limits.conf")
        exec_shell_command("echo '* hard nproc 1000000'  >> /etc/security/limits.conf")

    # cpu性能调度模式
    exec_shell_command("echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor")

    # 启用lvs支持
    exec_shell_command("modprobe ip_vs")
    exec_shell_command("modprobe ip_vs_rr")
    exec_shell_command("modprobe ip_vs_wrr")
    exec_shell_command("modprobe ip_vs_sh")

    exec_shell_command("sed -i 's|X11Forwarding yes|X11Forwarding no|g' /etc/ssh/sshd_config")
    exec_shell_command("systemctl restart sshd")
    res = subprocess.run("grep -q 'cpu.shares=1024' /etc/sysctl.conf", shell=True, capture_output=True, text=True)
    if res.returncode:
        generate_config_file(sys_conf_template, "/etc/sysctl.conf",
                             line_num=exec_shell_command("wc -l < /etc/sysctl.conf"))
        exec_shell_command("sysctl -p")

    if get_os_name() == "ubuntu" or get_os_name() == "debian":
        exec_shell_command("ufw disable")
        exec_shell_command("echo 'LANG=en_GB.UTF-8' > /etc/default/locale")
    elif get_os_name() == "centos" or get_os_name() == "redhat" or get_os_name() == "kylin":
        exec_shell_command("systemctl stop firewalld")
        exec_shell_command("systemctl disable firewalld")
        exec_shell_command("echo 'LANG=en_GB.UTF-8' > /etc/locale.conf")
    else:
        print("System type not supported temporarily.")
        sys.exit(1)

    print("System optimization parameters configuration completed.")


if __name__ == '__main__':
    sys_conf_template = """
# CPU 调优
cpu.shares=1024
kernel.sched_min_granularity_ns=10000000
kernel.sched_wakeup_granularity_ns=15000000
kernel.sched_latency_ns=4000000
kernel.sched_rt_period_us=1000000
kernel.sched_rt_runtime_us=950000
kernel.sched_child_runs_first=1
kernel.pid_max=4194303
kernel.threads-max=2097152

# 内存调优
vm.swappiness=0
vm.overcommit_memory=1
vm.max_map_count=262144
vm.dirty_ratio=20
vm.dirty_background_ratio=10
vm.min_free_kbytes=65536
vm.memory_failure_early_kill=1
vm.lowmem_reserve_ratio="4096 8192 16384"
vm.page-cluster=3
vm.compact_memory=1
vm.panic_on_oom=1


# 文件系统调优
fs.file-max=1000000
fs.nr_open=1048576
fs.aio-max-nr=1048576
fs.inotify.max_user_watches=524288
fs.inotify.max_user_instances=8192
fs.dentry-state=1024
fs.async_page_cache=1
fs.max_pipesize=1MB
fs.inotify.max_queued_events=8192
fs.inotify.max_user_instances=8192
fs.epoll.max_user_watches=524288
fs.cache_pressure=50
fs.mmap_minaddr=65536
fs.mmap_maxaddr=8192MB

# 网络调优
net.core.rmem_max=16777216
net.core.wmem_max=16777216
net.core.somaxconn=65535
net.ipv4.tcp_max_syn_backlog=4096
net.ipv4.tcp_rmem="4096 87380 16777216"
net.ipv4.tcp_wmem="4096 87380 16777216"
net.ipv4.tcp_mtu_probing=1
net.ipv4.ip_local_port_range="1024 65535"
net.ipv4.ip_forward=1
net.ipv4.conf.all.rp_filter=0
net.ipv4.conf.default.rp_filter=0
net.ipv4.tcp_syncookies=1
net.core.message_cost=1
net.ipv4.conf.all.arp_filter=1
net.ipv4.conf.default.arp_filter=1
net.ipv4.conf.all.promote_secondaries=1
net.ipv4.conf.default.promote_secondaries=1
net.ipv4.conf.all.arp_announce=2
net.ipv4.conf.default.arp_announce=2
net.ipv4.tcp_fin_timeout=15
net.ipv4.tcp_keepalive_time=600
net.ipv4.tcp_keepalive_intvl=60
net.ipv4.tcp_keepalive_probes=9
net.ipv4.tcp_max_tw_buckets=5000
net.core.netdev_budget=300
net.ipv4.conf.all.accept_source_route=0
net.ipv4.conf.default.accept_source_route=0
"""
    init_os_conf()
