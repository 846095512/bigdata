# -*- coding: utf-8 -*-
from commons import exec_shell_command, os, sys, get_os_name


def init_os_conf():
    if os.getlogin() != 'root':
        print("Please execute this script as the root user.")
        sys.exit(1)

    # 时区设置
    exec_shell_command("timedatectl set-timezone Asia/Shanghai")

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

    # cpu性能调度模式
    exec_shell_command("echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor")

    # 启用lvs支持
    exec_shell_command("modprobe ip_vs")
    exec_shell_command("modprobe ip_vs_rr")
    exec_shell_command("modprobe ip_vs_wrr")
    exec_shell_command("modprobe ip_vs_sh")

    exec_shell_command("sed -i 's|X11Forwarding yes|X11Forwarding no|g' /etc/ssh/sshd_config")
    exec_shell_command("systemctl restart sshd")

    if get_os_name() == "ubuntu" or get_os_name() == "debian":
        exec_shell_command("ufw disable")
        exec_shell_command("echo 'LANG=en_GB.UTF-8' >> /etc/default/locale")
    elif get_os_name() == "centos" or get_os_name() == "redhat":
        exec_shell_command("systemctl stop firewalld")
        exec_shell_command("systemctl disable firewalld")
        exec_shell_command("echo 'LANG=en_GB.UTF-8' >> /etc/locale.conf")


if __name__ == '__main__':
    init_os_conf()
