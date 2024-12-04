# -*- coding: utf-8 -*-

from commons import *


def install_ntp():
    ntp_conf = os.path.join(app_home_dir, "conf", 'ntp.conf')

    if install_role == "server":
        server_host = "127.127.1.0"
    else:
        server_host = params_dict["server.host"]
    exec_shell_command(f"sed -i 's|SERVER_HOST|{server_host}|g' {ntp_conf}")
    exec_shell_command(f"sed -i 's|NTP_HOME_DIR|{app_home_dir}|g' {ntp_conf}")

    exec_shell_command(f"{app_home_dir}/bin/ntpd -g -d -c {ntp_conf}",
                       "ntpd start", output=True)

    print("Ntp installation completed")


if __name__ == '__main__':
    unzip_package()
    install_ntp()
