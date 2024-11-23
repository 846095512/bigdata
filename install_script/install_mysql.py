# -*- coding: utf-8 -*-
import time

from commons import *


def install_mysql():
    my_cnf_template = """
[client]
port=3306
socket={{ mysql_home_dir }}/mysql.sock

[mysql]
init_command=set names utf8mb4
port=3306
socket={{ mysql_home_dir }}/mysql.sock
prompt=\\u@\\h:\\R:\\m:\\s[\\d]>


[mysqld]
server_id={{ server_id }}
user={{ current_user }}
character_set_server=utf8mb4
collation_server=utf8mb4_general_ci
bind-address=0.0.0.0
port=3306
socket={{ mysql_home_dir }}/mysql.sock
max_connect_errors=18446744073709551615
explicit_defaults_for_timestamp=1
local-infile=0
secure_file_priv=''
wait_timeout=1800
interactive_timeout=600
log_timestamps=SYSTEM
report_host={{ local_ip }}
safe-user-create=ON
allow-suspicious-udfs=ON
skip-slave-start
skip-symbolic-links
skip_external_locking
skip_name_resolve
slave_skip_errors=1007,1008,1050,1051,1062,1032

basedir={{ mysql_home_dir }}
datadir={{ mysql_home_dir }}/data
tmpdir={{ mysql_home_dir }}/tmp
pid-file={{ mysql_home_dir }}/mysqld.pid


################# thread ################
max_connections=5000
key_buffer_size=256M
max_allowed_packet=128M
table_open_cache=6000
sort_buffer_size=8M
read_rnd_buffer_size=16M
join_buffer_size=2M
tmp_table_size=64M
max_heap_table_size=64M

# min(cpu_cores, 64)
table_open_cache_instances=16


################# innodb ################
log_bin_trust_function_creators=ON
transaction_isolation=READ-COMMITTED
innodb_data_file_path=ibdata1:1024M:autoextend
innodb_buffer_pool_size={{ innodb_buffer_size }}
innodb_log_file_size=1G
innodb_log_files_in_group=4
innodb_log_buffer_size=32M
innodb_lock_wait_timeout=600
innodb_print_all_deadlocks=ON
innodb_flush_method=O_DIRECT
innodb_read_io_threads=32
innodb_write_io_threads=32
innodb_io_capacity=800
innodb_temp_data_file_path=ibtmp1:512M:autoextend:max:32G
innodb_flush_log_at_timeout=2
innodb_purge_rseg_truncate_frequency=16
innodb_numa_interleave=ON
innodb_online_alter_log_max_size=2G
innodb_flush_log_at_trx_commit=2

# min(cpu_cores, 64)
innodb_buffer_pool_instances=8
innodb_thread_concurrency=4
innodb_page_cleaners=4


################# binlog #################
log_bin={{ mysql_home_dir }}/binlog/mysql-bin
log_bin_index={{ mysql_home_dir }}/binlog/mysql-bin.index
binlog_cache_size=2M
binlog_rows_query_log_events=1
binlog_expire_logs_seconds=864000
binlog_format=row
sync_binlog=10
binlog_group_commit_sync_delay=1000


################# replication ############
gtid_mode=ON
enforce_gtid_consistency=ON
relay_log={{ mysql_home_dir }}/binlog/relay/relay-bin
relay_log_index={{ mysql_home_dir }}/binlog/relay/relay-bin.index
relay_log_recovery=ON
log_replica_updates=ON
replica_parallel_type=LOGICAL_CLOCK
replica_preserve_commit_order=1
replica_parallel_workers=32
replica_pending_jobs_size_max=128M

{% if install_role == "cluster" %}
################# group replication #######
binlog_checksum=NONE
loose-group_replication_group_name=ee70929b-7aa7-4151-8880-130b1b62ff97
loose-group_replication_start_on_boot=OFF
loose-group_replication_local_address={{ local_ip }}:33061
loose-group_replication_group_seeds={{ replication_group_seeds }}
loose-group_replication_ip_whitelist={{ ip_whitelist }}
loose-group_replication_bootstrap_group=OFF
loose-group_replication_unreachable_majority_timeout=30
loose-group_replication_member_expel_timeout=30
loose-group_replication_autorejoin_tries=5
loose-group_replication_compression_threshold=131072
loose-group_replication_transaction_size_limit=209715200
{% endif %}

################# error log #############
log-error={{ mysql_home_dir }}/logs/mysql_error.log


################# slow log ##############
slow_query_log=ON
slow_query_log_file={{ mysql_home_dir }}/logs/mysql_slow.log
long_query_time=1


################# other ##################
default-time-zone='+08:00'
performance_schema_session_connect_attrs_size=2048
sql_mode="STRICT_ALL_TABLES,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"


[mysqldump]
quick
max_allowed_packet=128M


[mysqlhotcopy]
interactive_timeout
"""
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
    module_name = params_dict["module.name"]
    install_role = params_dict["install.role"]
    local_ip = params_dict["local.ip"]
    install_ip = params_dict["install.ip"]
    is_master = params_dict["is.master"]
    innodb_buffer_size = params_dict["innodb.buffer.size"]
    ip_whitelist = ",".join([f"{ip}" for ip in install_ip])
    replication_group_seeds = ",".join([f"{ip}:33061" for ip in install_ip])
    is_valid_ip(local_ip, install_ip)
    if install_role == "cluster":
        is_valid_nums(install_ip)

    server_id = 1
    for ip in install_ip:
        if ip == local_ip:
            server_id += 10
    mysql_home_dir = os.path.join(get_app_home_dir(), module_name)
    my_cnf_file = os.path.join(mysql_home_dir, "my.cnf")
    exec_shell_command(f"mkdir -p {mysql_home_dir}")
    exec_shell_command(f"mkdir -p {mysql_home_dir}/data")
    exec_shell_command(f"mkdir -p {mysql_home_dir}/binlog/relay")
    exec_shell_command(f"mkdir -p {mysql_home_dir}/tmp")
    exec_shell_command(f"mkdir -p {mysql_home_dir}/logs")

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
    exec_shell_command(
        f"{mysql_home_dir}/bin/mysqld --defaults-file={mysql_home_dir}/my.cnf  --initialize  --user={current_user}  --basedir={mysql_home_dir} --datadir={mysql_home_dir}/data")
    exec_shell_command(
        f"{mysql_home_dir}/bin/mysqld_safe --defaults-file={mysql_home_dir}/my.cnf --user={current_user} > /dev/null 2>&1 &")
    temp_passwd = exec_shell_command(
        f"grep 'temporary password' {mysql_home_dir}/logs/mysql_error.log | awk '{{print $NF}}'")
    print(temp_passwd)
    time.sleep(5)
    if install_role == "cluster":
        exec_shell_command(
            f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'SET SQL_LOG_BIN=0;'")
        exec_shell_command(
            f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'CREATE USER repl@'%' IDENTIFIED WITH sha256_password BY \"repl@147!$&\";'")
        exec_shell_command(
            f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'GRANT REPLICATION SLAVE,CONNECTION_ADMIN,BACKUP_ADMIN,CLONE_ADMIN  ON *.* TO repl@'%';'")
        exec_shell_command(
            f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'INSTALL PLUGIN clone SONAME 'mysql_clone.so';'")
        exec_shell_command(
            f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'INSTALL PLUGIN group_replication SONAME 'group_replication.so';'")
        exec_shell_command(
            f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'CHANGE MASTER TO MASTER_USER='repl',MASTER_PASSWORD='repl@147!$&' FOR CHANNEL 'group_replication_recovery';'")
        exec_shell_command(
            f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'SET SQL_LOG_BIN=1;'")
        exec_shell_command(
            f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'FLUSH PRIVILEGES;'")

        if is_master == "true":
            exec_shell_command(
                f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'SET GLOBAL group_replication_bootstrap_group=ON;'")
            exec_shell_command(
                f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'START GROUP_REPLICATION;'")
            exec_shell_command(
                f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'SET GLOBAL group_replication_bootstrap_group=OFF;'")
        else:
            exec_shell_command(
                f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e  'START GROUP_REPLICATION;'")
    exec_shell_command(
        f"{mysql_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {mysql_home_dir}/mysql.sock --connect-expired-password -e 'ALTER USER 'root'@'localhost' IDENTIFIED BY \"DBuser@123_!@#\";'")
    print("mysql 安装完成")

if __name__ == '__main__':
    unzip_package()
    install_mysql()
