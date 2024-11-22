# -*- coding: utf-8 -*-
from commons import *


def install_jdk():
    module_name = params_dict["module.name"]
    env_file = get_user_env_filename()
    app_home = os.path.join(get_app_home_dir(), module_name)
    jvm_tmp = os.path.join(get_root_dir(), "tmp")
    if not os.path.exists(jvm_tmp):
        exec_shell_command(f"mkdir -p {jvm_tmp}")
    set_permissions(app_home)
    with open(env_file, "a+", encoding="UTF-8") as f1:
        f1.write(f"export _JAVA_OPTIONS=-Djava.io.tmpdir={jvm_tmp}\n")
        f1.write(f"export JAVA_HOME={app_home}\n")
        f1.write(f"export PATH=$PATH:$JAVA_HOME/bin\n")
    exec_shell_command(f"source {env_file}")
    exec_shell_command("java -version")

    print("jdk 安装完成!!!!")


if __name__ == '__main__':
    unzip_package()
    install_jdk()
