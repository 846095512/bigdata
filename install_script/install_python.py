# -*- coding: utf-8 -*-

from commons import *


def install_python():
    python_home = os.path.join(get_app_home_dir(), module_name)
    configure_environment("PYTHON_HOME", python_home)


if __name__ == '__main__':
    unzip_package()
    install_python()
