# -*- coding: utf-8 -*-
import os
from commons import *


def install_mysql():
    my_cnf_template = """
[client]
port=3306
socket={{ mysql_home_dir }}/mysql.sock

[mysql]
port=3306
socket={{ mysql_home_dir }}/mysql.sock

[mysqld]
server_id={{ server_id }}
report_host={{ local_ip }}
user={{ current_user }}
bind-address=0.0.0.0
port=3306
socket={{ mysql_home_dir }}/mysql.sock
gtid_mode=ON
enforce_gtid_consistency=ON
log_replica_updates=ON
default-time-zone='+08:00'
basedir={{ mysql_home_dir }}
datadir={{ mysql_home_dir }}/data
tmpdir={{ mysql_home_dir }}/tmp
character-set_server=utf8mb4
log-error={{ mysql_home_dir }}/logs/mysql-err.log
pid-file={{ mysql_home_dir }}/mysqld.pid

{% if install_role == "cluster" %}
#################### replcation  ######################
loose-group_replication_group_name=781c51f4-24c9-11ef-b2d1-000c2963108d
loose-group_replication_start_on_boot=off
loose-group_replication_local_address={{ local_ip }}:33061
loose-group_replication_group_seeds={{ replication_group_seeds }}
loose-group_replication_ip_whitelist={{ ip_whitelist }}
loose-group_replication_bootstrap_group=off
loose-group_replication_single_primary_mode=ON
loose-group_replication_enforce_update_everywhere_checks=ON
{% endif %}

##################### innodb #########################
transaction_isolation=READ-COMMITTED
log_bin_trust_function_creators=ON
innodb_buffer_pool_size={{ innodb_buffer_size }}
innodb_lock_wait_timeout=600
innodb_print_all_deadlocks=ON
innodb_read_io_threads=32
innodb_write_io_threads=32
innodb_io_capacity=80


##################### slow ##########################
slow_query_log=ON
slow_query_log_file={{ mysql_home_dir }}/logs/mysql-slow.log
long_query_time=1

#################### binlog ######################
log_bin={{ mysql_home_dir }}/binlog/mysql-bin
log_bin_index={{ mysql_home_dir }}/binlog/mysql-bin.index
binlog_format=ROW
binlog_expire_logs_seconds=259200
"""
    current_user = os.getlogin()
    if current_user == "root":
        if get_os_name() == "ubuntu":
            exec_shell_command("apt remove mariadb* -y")
            exec_shell_command("apt install libncurses5 -y")
        elif get_os_name() == "centos":
            exec_shell_command("yum remove mariadb* -y")
            exec_shell_command("yum install ncurses-compat-libs -y")
        else:
            print("暂时不支持的系统类型")
            sys.exit(1)
    module_name = params_dict["module"]
    install_role =  params_dict["install.role"]
    local_ip =  params_dict["local.ip"]
    install_ip = params_dict["install.ip"]
    is_master = params_dict["is.master"]
    innodb_buffer_size = params_dict["innodb.buffer.size"]
    ip_whitelist = ",".join([f"{ip}" for ip in install_ip])
    replication_group_seeds = ",".join([f"{ip}:33061" for ip in install_ip])
    is_valid_ip(local_ip,install_ip)
    if install_role == "cluster":
        is_vaild_nums(install_ip)

    server_id = 10
    for ip in install_ip:
        if ip == local_ip:
            server_id += 1
    mysql_home_dir = os.path.join(get_app_home_dir(), module_name)
    my_cnf_file = os.path.join(mysql_home_dir,"my.cnf")
    exec_shell_command(f"mkdir -p {mysql_home_dir}/logs")
    exec_shell_command(f"mkdir -p {mysql_home_dir}/binlog")
    exec_shell_command(f"mkdir -p {mysql_home_dir}/tmp")
    exec_shell_command(f"mkdir -p {mysql_home_dir}/data")

    generate_config_file(template_str=my_cnf_template,
                         conf_file=my_cnf_file,
                         keyword="",
                         mysql_home_dir=mysql_home_dir,
                         current_user=current_user,
                         local_ip=local_ip,
                         install_role=install_role,
                         server_id=server_id,
                         ip_whitelist=ip_whitelist,
                         replication_group_seeds=replication_group_seeds,
                         innodb_buffer_size=innodb_buffer_size)
    
    set_permissions(mysql_home_dir)
    
    # 初始化mysql并修改root用户密码 启动组复制
    exec_shell_command(f"{mysql_home_dir}/bin/mysqld --defaults-file={mysql_home_dir}/my.cnf  --initialize  --user={current_user}  --basedir={mysql_home_dir} --datadir={mysql_home_dir}/data")
    exec_shell_command(f"{mysql_home_dir}/bin/mysqld_safe --defaults-file={mysql_home_dir}/my.cnf --user={current_user} &")
    temp_passwd=exec_shell_command("grep 'temporary password' {mysql_home_dir}/logs/mysql-err.log | awk '{print $NF}'")
    print(temp_passwd)
    # if install_role == "cluster":
    #     exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'SET SQL_LOG_BIN=0;'")
    #     exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'CREATE USER repl@'%' IDENTIFIED WITH sha256_password BY 'repl@147!$&';'")
    #     exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'GRANT REPLICATION,SLAVE,CONNECTION_ADMIN,BACKUP_ADMINON,CLONE_ADMIN *.* TO repl@'%';'")
    #     exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'INSTALL PLUGIN clone SONAME 'mysql_clone.so';'")
    #     exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'INSTALL PLUGIN group_replication SONAME 'group_replication.so';'")
    #     exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'CHANGE MASTER TO MASTER_USER='repl',MASTER_PASSWORD='repl@147!$&' FOR CHANNEL 'group_replication_recovery';'")
    #     exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'SET SQL_LOG_BIN=1;'")
    #     exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'FLUSH PRIVILEGES;'")

    #     if is_master == "true":
    #         exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'SET GLOBAL group_replication_bootstrap_group=ON;'")
    #         exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'START GROUP_REPLICATION;'")
    #         exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'SET GLOBAL group_replication_bootstrap_group=OFF;'")
    #     else:
    #         exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e  'START GROUP_REPLICATION;'")
    # exec_shell_command(f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S '{mysql_home_dir}/mysql.sock' -e 'ALTER USER 'root'@'localhost' IDENTIFIED BY 'DBuser@123_!@#';'")

if __name__ == '__main__':
    unzip_package()
    install_mysql()