# -*- coding: utf-8 -*-
import ipaddress
import json
import os
import shutil
import subprocess
import sys
import tarfile
import time

import requests
from hdfs import InsecureClient
from jinja2 import Template

current_user = os.getlogin()
script_path = os.path.dirname(os.path.abspath(__file__))
with open(f'{script_path}/conf.json', "r", encoding="utf-8") as f:
    params_dict = json.load(f)

filename = params_dict["filename"]
module_name = params_dict["module.name"]
local_ip = params_dict["local.ip"]
install_role = params_dict["install.role"]
install_ip = params_dict["install.ip"]


def is_valid_ip(*args):
    try:
        for arg in args:
            if isinstance(arg, str):
                ipaddress.ip_address(arg)
            elif isinstance(arg, list):
                for ip in arg:
                    ipaddress.ip_address(ip)
            else:
                print("不支持的参数类型")
                sys.exit(1)
    except ValueError:
        print("ip 地址不合法")
        sys.exit(1)


def is_valid_nums(ip_list):
    if len(ip_list) > 1 and len(ip_list) % 2 != 0:
        return True
    else:
        print("集群模式下ip个数必须大于1并且为奇数")
        sys.exit(1)


def get_user_env_filename():
    os_name = get_os_name()

    if os_name == "ubuntu":
        return f"/home/{current_user}/.profile"
    else:
        return f"/home/{current_user}/.bash_profile"


def get_os_name():
    command = "cat /etc/*-release | grep -wi 'id' | awk -F '=' '{print $2}' | sed 's/\"//g' | tr 'A-Z' 'a-z' "
    result = str(subprocess.run(command, shell=True, capture_output=True, text=True).stdout).strip()
    return result


def get_root_dir():
    if current_user == "root":
        root_dir = "/opt"
    else:
        root_dir = "/home/" + current_user
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    return root_dir


def get_app_home_dir():
    root_dir = get_root_dir()
    app_home_dir = os.path.join(root_dir, "app")
    if not os.path.exists(app_home_dir):
        os.makedirs(app_home_dir)
    return app_home_dir


def exec_shell_command(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip(), result.stderr.strip(), result.returncode
    except subprocess.CalledProcessError as e:
        print(e)
        sys.exit(1)


def set_permissions(path):
    exec_shell_command(f"chmod -R 750 {path}")
    exec_shell_command(f"chown -R {current_user}:{current_user} {path}")
    print(f"{path}  设置目录权限完成")


def unzip_package():
    file_path = get_download_dir()
    app_home_dir = get_app_home_dir()
    print(f"应用家目录 -> {app_home_dir}")

    with tarfile.open(f"{file_path}", 'r') as tar_ref:
        tar_ref.extractall(app_home_dir)
    print(f"文件解压完成  ->  {filename}")

    unpack_name, stderr, code = exec_shell_command(f"tar -tf {file_path}  | head -1 | cut -d'/' -f1")
    old_path = os.path.join(app_home_dir, unpack_name)
    new_path = os.path.join(app_home_dir, module_name)
    shutil.move(old_path, new_path)
    print(f"目录移动完成  {old_path} -> {new_path}")


def generate_config_file(template_str, conf_file, keyword="", **kwargs):
    template = Template(template_str)
    config_content = template.render(kwargs)
    if keyword == "":
        insert_line_num = 1
    else:
        insert_line_num, stderr, code = exec_shell_command(f"sed -n \"/{keyword}/=\" {conf_file}")
    exec_shell_command(f"touch {conf_file}")
    with open(conf_file, "r", encoding="utf-8") as f1:
        lines = f1.readlines()
        lines.insert(int(insert_line_num), config_content)
    with open(conf_file, "w", encoding="utf-8") as f2:
        f2.writelines(lines)
    print(f"文件配置完成    ->    {conf_file} ")


def get_download_dir():
    root_dir = get_root_dir()
    package_dir = os.path.join(root_dir, "package", filename)
    if not os.path.exists(package_dir):
        print(f"安装包文件下载路径    ->    {package_dir}  不存在,请先将安装包上传至    ->    {package_dir}")
        sys.exit(1)
    return package_dir


def check_service(service_port, service_name, ip_list=install_ip):
    for ip in ip_list:
        while True:
            result = subprocess.run(
                f"nc -zv {ip} {service_port}", shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"等待 {ip} {service_name}  服务启动")
                time.sleep(3)
            else:
                break


def kill_service(service_class):
    for class_name in service_class:
        stdout, stderr, code = exec_shell_command(f"ps -ef | grep {class_name} | grep -v grep | awk '{{print $2}}'")
        if stdout != "":
            stdout, stderr, code = exec_shell_command(f"kill -9 {stdout}")
            check_cmd_output(stdout, stderr, code, f"kill  残留进程 {class_name}", check=True)


def check_cmd_output(stdout, stderr, code, msg, check=False):
    if check:
        print(f"{msg} 成功 -> {stdout}" if code == 0 else f"{msg} 失败   ->  {stderr}")


def download_from_hdfs(hdfs_host, hdfs_path, local_dir, hdfs_port=50070, recursive=False):
    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')
    os.makedirs(local_dir, exist_ok=True)
    files = client.list(hdfs_path)
    for file in files:
        hdfs_file_path = f"{hdfs_path}/{file}"
        local_file_path = os.path.join(local_dir, file)
        if client.status(hdfs_file_path)['type'] == 'FILE':
            client.download(hdfs_file_path, local_file_path)
        elif client.status(hdfs_file_path)['type'] == 'DIRECTORY':
            if recursive:
                os.makedirs(local_file_path, exist_ok=True)
                download_from_hdfs(hdfs_host, hdfs_file_path, local_file_path, hdfs_port=hdfs_port)
    print(f"从hdfs {hdfs_path} -> {local_dir}  文件下载完毕")


def upload_from_local(hdfs_host, hdfs_path, local_dir, hdfs_port=50070, recursive=False):
    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')
    if not os.path.exists(local_dir):
        print(f"本地目录 {local_dir} 不存在！")
        return
    for item in os.listdir(local_dir):
        local_item_path = os.path.join(local_dir, item)
        hdfs_item_path = f"{hdfs_path}/{item}"
        if os.path.isfile(local_item_path):
            client.upload(hdfs_item_path, local_item_path, overwrite=True)
        elif os.path.isdir(local_item_path):
            if recursive:
                upload_from_local(hdfs_host, hdfs_item_path, local_item_path)
    print(f"从本地  {local_dir} ->  {hdfs_path} 文件上传完毕")


def check_namenode_status(namenode_list, port=50070):
    for ip in namenode_list:
        url = f'http://{ip}:{port}/jmx'
        try:
            response = requests.get(url)
            response.raise_for_status()
            jmx_data = response.json()
            for bean in jmx_data.get('beans', []):
                if bean.get('name') == 'Hadoop:service=NameNode,name=NameNodeStatus':
                    state = bean.get('State')
                    if state == 'active':
                        return ip
        except requests.exceptions.RequestException as e:
            print(f"Node {ip} is {e}")

