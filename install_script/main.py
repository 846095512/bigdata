# -*- coding=utf-8 -*-

import re
import sys
from commons import *


def install_modules(file_array):
    if type(file_array) is not list:
        print("please input a list")
        sys.exit(1)
    function_name = []
    for file in file_array:
        if re.search(r'(jdk)', file):
            function_name.append("install_jdk")
        elif re.search(r'(zookeeper)', file):
            function_name.append("install_zookeeper")
        elif re.search(r'(hadoop)', file):
            function_name.append("install_hadoop")
        elif re.search(r'(spark)', file):
            function_name.append("install_spark")
        elif re.search(r'(flink)', file):
            function_name.append("install_flink")
        elif re.search(r'(kafka)', file):
            function_name.append("install_kafka")
        elif re.search(r'(mysql)', file):
            function_name.append("install_mysql")
    return function_name


if __name__ == '__main__':
    dependencies = {
        'install_jdk': [],
        'install_mysql': [],
        'install_keepalived': [],
        'install_zookeeper': ['install_jdk'],
        'install_hadoop': {
            'standalone': ['install_jdk'],
            'cluster': ['install_jdk', 'install_zookeeper']
        },
        'install_spark': ['install_jdk', 'install_hadoop'],
        'install_flink': ['install_jdk', 'install_hadoop']
    }

    # 存储已安装的组件
    installed = []


    # 检查组件是否安装
    def is_installed(component):
        return component in installed


    # 安装组件
    def install_component(component, mode=None):
        # 获取该组件的依赖
        required_dependencies = dependencies.get(component, {})

        # 如果组件依赖是字典，则根据模式获取依赖
        if isinstance(required_dependencies, dict):
            if mode not in required_dependencies:
                print(f"{component} 不支持该模式: {mode}。")
                return
            required_dependencies = required_dependencies[mode]

        # 检查是否已安装所有依赖
        for dep in required_dependencies:
            if not is_installed(dep):
                print(f"需要先安装 {dep} 才能安装 {component}。")
                return  # 如果依赖未安装，则无法安装当前组件

    install_component('install_jdk')
