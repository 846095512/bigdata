# -*- coding: utf-8 -*-
import os
from commons import *

def install_jdk():
    args = get_install_config()
    module_name = args["module"]
    env_file = get_user_env_filename()
    app_home = os.path.join(get_app_home_dir(), module_name)
    set_permissions(app_home)

    with open(env_file, "a+", encoding="UTF-8") as f:
        f.write(f"export JAVA_HOME={app_home}\n")
        f.write(f"export PATH=$PATH:$JAVA_HOME/bin\n")
    exec_shell_command(f"source {env_file}")
    print(exec_shell_command("java -version"))

    print("jdk 安装完成!!!!")

if __name__ == '__main__':
    unzip_package()
    install_jdk()