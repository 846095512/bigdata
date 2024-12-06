# -*- coding: utf-8 -*-

from commons import *


def install_chrony():
    ntp_conf = os.path.join(app_home_dir, "conf", 'ntp.conf')
    cird_notation = params_dict["cird.notation"]
    chrony_server_ip = params_dict["chrony.server.ip"]
    generate_config_file(template_str=chrony_conf_template,
                         conf_file=ntp_conf,
                         cired_notation=cird_notation,
                         chrony_server_ip=chrony_server_ip)
    exec_shell_command(f"{app_home_dir}/sbin/chronyd -f {app_home_dir}/conf/chrony.conf",
                       "chrony start", output=True)
    print("chrony installation completed")


if __name__ == '__main__':
    chrony_conf_template = """
server ntp1.aliyun.com iburst
server ntp2.aliyun.com iburst
server ntp3.aliyun.com iburst
server {{ chrony_server_ip }} stratum 10
driftfile {{ chrony_home_dir }}/chrony.drift
logdir {{ chrony_home_dir }}/log
log measurements statistics tracking
allow {{ cird_notation }}
makestep 1.0 3
"""
    unzip_package()
    install_chrony()
