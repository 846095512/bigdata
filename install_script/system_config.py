# -*- coding: utf-8 -*-
from commons import *


def init_os_conf():
    if os.getlogin() != 'root':
        print("请用root用户执行系统优化脚本")
        sys.exit(1)

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
    exec_shell_command("systemctl stop firewalld")
    exec_shell_command("systemctl disable firewalld")

    # cpu性能调度模式
    exec_shell_command("echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor")

    # 启用lvs支持
    exec_shell_command("modprobe ip_vs")
    exec_shell_command("modprobe ip_vs_rr")
    exec_shell_command("modprobe ip_vs_wrr")
    exec_shell_command("modprobe ip_vs_sh")
    if get_os_name() == "ubuntu" or get_os_name() == "debian":
        exec_shell_command("apt install ipvsadm -y")
        exec_shell_command("sudo apt remove mariadb* -y")
        exec_shell_command("sudo apt install libncurses5 -y")
    elif get_os_name() == "centos" or get_os_name() == "redhat":
        exec_shell_command("yum install ipvsadm -y")
        exec_shell_command("sudo yum remove mariadb* -y")
        exec_shell_command("sudo yum install ncurses-compat-libs -y")
    else:
        print("暂时不支持的系统类型")
        sys.exit(1)

if __name__ == '__main__':
    init_os_conf()
