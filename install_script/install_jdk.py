# -*- coding: utf-8 -*-
from commons import *


def install_jdk():
    set_permissions(app_home_dir)
    configure_environment("JAVA_HOME", app_home_dir)


if __name__ == '__main__':
    unzip_package()
    install_jdk()
