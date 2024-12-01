# -*- coding: utf-8 -*-
from commons import *


def install_jdk():
    env_file = get_user_env_filename()
    app_home = os.path.join(get_app_home_dir(), module_name)
    set_permissions(app_home)
    with open(env_file, "a+", encoding="UTF-8") as f1:
        f1.write(f"export JAVA_HOME={app_home}\n")
        f1.write(f"export PATH=$PATH:$JAVA_HOME/bin\n")
    exec_shell_command(f"source {env_file}")
    stdout, stderr, code = exec_shell_command("java -version")
    check_cmd_output(stdout, stderr, code, "jdk 环境变量设置", check=True)
    print("jdk 安装完成")


if __name__ == '__main__':
    unzip_package()
    install_jdk()
