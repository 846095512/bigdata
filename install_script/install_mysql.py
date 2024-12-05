# -*- coding: utf-8 -*-
import math

from commons import *


def install_mysql():
    ip_whitelist = ",".join([f"{ip}" for ip in install_ip])
    replication_group_seeds = ",".join([f"{ip}:33061" for ip in install_ip])
    is_valid_ip(local_ip, install_ip)
    if install_role == "cluster":
        is_valid_nums(install_ip)

    server_id = 1
    for ip in install_ip:
        if ip == local_ip:
            server_id += 10

    my_cnf_file = os.path.join(app_home_dir, "my.cnf")
    total_mem = int(exec_shell_command("free -g | awk '/Mem:/ {print $2}'"))
    innodb_buffer_pool_size = f"{math.ceil(total_mem / 2)}G"
    mysql_group_id = params_dict["mysql.group.id"]
    group_id = generate_uuid(mysql_group_id)
    exec_shell_command(f"mkdir -p {app_home_dir}")
    exec_shell_command(f"mkdir -p {app_home_dir}/data")
    exec_shell_command(f"mkdir -p {app_home_dir}/binlog/relay")
    exec_shell_command(f"mkdir -p {app_home_dir}/tmp")
    exec_shell_command(f"mkdir -p {app_home_dir}/logs")

    generate_config_file(template_str=my_cnf_template,
                         conf_file=my_cnf_file,
                         mysql_home_dir=app_home_dir,
                         current_user=current_user,
                         local_ip=local_ip,
                         install_role=install_role,
                         server_id=server_id,
                         group_id=group_id,
                         ip_whitelist=ip_whitelist,
                         replication_group_seeds=replication_group_seeds,
                         innodb_buffer_pool_size=innodb_buffer_pool_size)

    set_permissions(app_home_dir)
    new_password = "DBuser@123_!@#"
    repl_password = "repl@146_!$&"
    # 初始化mysql并修改root用户密码 启动组复制
    exec_shell_command(
        f"""{app_home_dir}/bin/mysqld --defaults-file={app_home_dir}/my.cnf  --initialize  --user={current_user}  --basedir={app_home_dir} --datadir={app_home_dir}/data""",
        "mysql format", output=True)
    exec_shell_command(
        f"""{app_home_dir}/bin/mysqld_safe --defaults-file={app_home_dir}/my.cnf --user={current_user} > /dev/null 2>&1 & """,
        "mysql 启动", output=True)
    temp_passwd = exec_shell_command(
        f"""grep 'temporary password' {app_home_dir}/logs/mysql_error.log | awk '{{print $NF}}' """)
    print(f"Temporary root password is {temp_passwd}")
    print(f"new root password is {new_password}")
    print(f"repl password is {repl_password}")
    check_service("3306", "mysql server")
    time.sleep(5)
    exec_shell_command(
        f"""{app_home_dir}/bin/mysql -uroot -p'{temp_passwd}' -S {app_home_dir}/mysql.sock  -e "ALTER USER 'root'@'localhost' IDENTIFIED BY '{new_password}';" ""","change mysql root password", output=True)
    if install_role == "cluster":
        exec_shell_command(
            f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "CREATE USER 'repl'@'%' IDENTIFIED BY '{repl_password}'; " """)
        exec_shell_command(
            f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "GRANT REPLICATION SLAVE on *.* to 'repl'@'%'; " """)
        exec_shell_command(
            f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "SET SQL_LOG_BIN=0;" """)
        exec_shell_command(
            f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "INSTALL PLUGIN clone SONAME 'mysql_clone.so';" """)
        exec_shell_command(
            f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "INSTALL PLUGIN group_replication SONAME 'group_replication.so';" """)
        exec_shell_command(
            f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "SET SQL_LOG_BIN=1;" """)
        exec_shell_command(
            f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "FLUSH PRIVILEGES;" """)

        if local_ip == install_ip[0]:
            exec_shell_command(
                f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "SET GLOBAL group_replication_bootstrap_group=ON;" """)
            exec_shell_command(
                f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "START GROUP_REPLICATION;" """,
                "Start MySQL Group Replication", output=True)
            exec_shell_command(
                f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "SET GLOBAL group_replication_bootstrap_group=OFF;" """)
        else:
            exec_shell_command(
                f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "CHANGE MASTER TO MASTER_USER='repl',MASTER_PASSWORD='repl@147_!$&' FOR CHANNEL 'group_replication_recovery';" """)
            exec_shell_command(
                f"""{app_home_dir}/bin/mysql -uroot -p'{new_password}' -S {app_home_dir}/mysql.sock -e  "START GROUP_REPLICATION;" """,
                "Start MySQL Group Replication", output=True)
    configure_environment("MYSQL_HOME", app_home_dir)
    print("MySQL  installation completed")


if __name__ == '__main__':
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
server-id={{ server_id }}
user={{ current_user }}
character_set_server=utf8mb4
collation_server=utf8mb4_general_ci
bind-address=0.0.0.0
port=3306
socket={{ mysql_home_dir }}/mysql.sock
explicit_defaults_for_timestamp=1
local-infile=0
wait_timeout=1800
interactive_timeout=600
log_timestamps=SYSTEM
safe-user-create=ON
allow-suspicious-udfs=ON
skip-replica-start
skip_external_locking
skip_name_resolve
replica_skip_errors=1007,1008,1050,1051,1062,1032

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
innodb_buffer_pool_size={{ innodb_buffer_pool_size }}
innodb_redo_log_capacity=1G
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
log-bin={{ mysql_home_dir }}/binlog/mysql-bin
log_bin_index={{ mysql_home_dir }}/binlog/mysql-bin.index
binlog_cache_size=2M
binlog_rows_query_log_events=1
binlog_expire_logs_seconds=864000
sync_binlog=10
binlog_group_commit_sync_delay=1000


################# replication ############
gtid-mode=ON
enforce-gtid-consistency=ON
relay_log={{ mysql_home_dir }}/binlog/relay/relay-bin
relay_log_index={{ mysql_home_dir }}/binlog/relay/relay-bin.index
relay_log_recovery=ON
log_replica_updates=ON
replica_preserve_commit_order=1
replica_parallel_workers=32
replica_pending_jobs_size_max=128M

{% if install_role == "cluster" %}
################# group replication #######
binlog_checksum=NONE
report_host={{ local_ip }}
loose-group_replication_group_name={{ group_id }}
loose-group_replication_start_on_boot=OFF
loose-group_replication_local_address={{ local_ip }}:33061
loose-group_replication_group_seeds={{ replication_group_seeds }}
loose-group_replication_ip_whitelist={{ ip_whitelist }}
loose-group_replication_bootstrap_group=OFF
loose-group_replication_unreachable_majority_timeout=30
loose-group_replication_member_expel_timeout=30
loose-group_replication_autorejoin_tries=5
loose-group_replication_compression_threshold=131072
loose-group_replication_transaction_size_limit=4294967296
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
max_allowed_packet=1073741824
"""
    exec_shell_command("pkill mysql", "kill mysql", output=True)
    unzip_package()
    install_mysql()
