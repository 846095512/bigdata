# -*- coding: utf-8 -*-

from commons import *


def install_ntp():
    ntp_home = os.path.join(get_app_home_dir(), module_name)
    ntp_conf = os.path.join(ntp_home, "conf", 'ntp.conf')

    if install_role == "server":
        server_host = "127.127.1.0"
    else:
        server_host = params_dict["server.host"]
    exec_shell_command(f"sed -i 's|SERVER_HOST|{server_host}|g' {ntp_conf}")
    exec_shell_command(f"sed -i 's|NTP_HOME_DIR|{ntp_home}|g' {ntp_conf}")

    exec_shell_command(f"{ntp_home}/bin/ntpd -g -d -c {ntp_conf}",
                       "ntpd start", output=True)

    print("Ntp installation completed")

if __name__ == '__main__':
    unzip_package()
    install_ntp()