# -*- coding: utf-8 -*-
from commons import *

app_home = os.path.join(get_app_home_dir(), module_name)


def install_jdk():
    set_permissions(app_home)
    configure_environment("JAVA_HOME", app_home)
    exec_shell_command("java -version", "Jdk Installation", output=True)


if __name__ == '__main__':
    delete_dir(app_home)
    unzip_package()
    install_jdk()
