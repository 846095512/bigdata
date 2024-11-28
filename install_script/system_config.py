# -*- coding: utf-8 -*-
from commons import exec_shell_command


def init_os_conf():
    # 交换分区
    exec_shell_command("swapoff -a")
    exec_shell_command("sed -i '/swap/s/^/#/' /etc/fstab")

    # cpu 透明大页关闭
    exec_shell_command("echo never > /sys/kernel/mm/transparent_hugepage/enabled")
    exec_shell_command("echo never > /sys/kernel/mm/transparent_hugepage/defrag")

    # 系统文件描述符 进程限制
    exec_shell_command("echo '* soft nofile 1000000'  >> /etc/security/limits.conf")
    exec_shell_command("echo '* hard nofile 1000000'  >> /etc/security/limits.conf")
    exec_shell_command("echo '* soft nproc 1000000'  >> /etc/security/limits.conf")
    exec_shell_command("echo '* hard nproc 1000000'  >> /etc/security/limits.conf")

    # 防火墙
    exec_shell_command("sudo systemctl stop firewalld")
    exec_shell_command("sudo systemctl disable firewalld")

    # cpu性能调度模式
    exec_shell_command("echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor")

if __name__ == '__main__':
    init_os_conf()