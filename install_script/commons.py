# -*- coding: utf-8 -*-
import hashlib
import ipaddress
import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
import uuid

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
                print("Unsupported parameter. Please check the parameters")
                sys.exit(1)
    except ValueError:
        print("Invalid IP address")
        sys.exit(1)


def is_valid_nums(ip_list):
    if len(ip_list) > 1 and len(ip_list) % 2 != 0:
        return True
    else:
        print("the number of IPs must be greater than 1 and be an odd number")
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
    home_dir = os.path.join(root_dir, "app")
    if not os.path.exists(home_dir):
        os.makedirs(home_dir)
    return home_dir


def exec_shell_command(cmd, msg=None, output=False):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if output:
        print(
            f"{msg} Succeeded -> {result.stdout}" if result.returncode == 0 else f"{msg} Failed  ->  {result.stderr}")
    return result.stdout.strip()


def set_permissions(path):
    exec_shell_command(f"chmod -R 750 {path}")
    exec_shell_command(f"chown -R {current_user}:{current_user} {path}")
    print(f"The permissions for the {path} directory have been set")


def unzip_package():
    file_path = get_download_dir()
    home_dir = get_app_home_dir()
    print(f"PREFIX={home_dir}")

    with tarfile.open(f"{file_path}", 'r') as tar_ref:
        tar_ref.extractall(home_dir)
    print(f"The {filename} file has been extracted")

    unpack_name = exec_shell_command(f"tar -tf {file_path}  | head -1 | cut -d'/' -f1")
    old_path = os.path.join(home_dir, unpack_name)
    new_path = os.path.join(home_dir, module_name)
    shutil.move(old_path, new_path)
    print(f"The directory has been moved from {old_path} to {new_path}")


def generate_config_file(template_str, conf_file, keyword=None, line_num=None, **kwargs):
    template = Template(template_str)
    config_content = template.render(kwargs)
    exec_shell_command(f"touch {conf_file}")
    if keyword is None and line_num is None:
        with open(conf_file, "w", encoding="utf-8") as f1:
            f1.writelines(config_content)
    elif line_num is not None:
        with open(conf_file, "r", encoding="utf-8") as f1:
            lines = f1.readlines()
            lines.insert(line_num, config_content)
        with open(conf_file, "w", encoding="utf-8") as f2:
            f2.writelines(lines)
    else:
        insert_line_num = exec_shell_command(f"sed -n \"/{keyword}/=\" {conf_file}")
        with open(conf_file, "r", encoding="utf-8") as f1:
            lines = f1.readlines()
            lines.insert(int(insert_line_num), config_content)
        with open(conf_file, "w", encoding="utf-8") as f2:
            f2.writelines(lines)
    print(f"The configuration file {conf_file} has been generated ")


def get_download_dir():
    root_dir = get_root_dir()
    package_dir = os.path.join(root_dir, "package", filename)
    if not os.path.exists(package_dir):
        print(
            f"installation package download path  {package_dir}  is not exist. Please upload the installation package to {package_dir}")
        sys.exit(1)
    return package_dir


def check_service(service_port, service_name, ip_list=install_ip):
    for ip in ip_list:
        while True:
            result = subprocess.run(
                f"nc -zv {ip} {service_port}", shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"wait host  {ip} {service_name}  start")
                time.sleep(3)
            else:
                break


def kill_service(service_class):
    for class_name in service_class:
        stdout = exec_shell_command(f"ps -ef | grep {class_name} | grep -v grep | awk '{{print $2}}'")
        if stdout != "":
            exec_shell_command(f"kill -9 {stdout}", f"kill process {class_name}", output=True)


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
    print(f"From hdfs path  {hdfs_path} download file to local path {local_dir}  success")


def upload_from_local(hdfs_host, hdfs_path, local_dir, hdfs_port=50070, recursive=False):
    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')
    if not os.path.exists(local_dir):
        print(f"Local path {local_dir} is not exist")
        return
    for item in os.listdir(local_dir):
        local_item_path = os.path.join(local_dir, item)
        hdfs_item_path = f"{hdfs_path}/{item}"
        if os.path.isfile(local_item_path):
            client.upload(hdfs_item_path, local_item_path, overwrite=True)
        elif os.path.isdir(local_item_path):
            if recursive:
                upload_from_local(hdfs_host, hdfs_item_path, local_item_path)
    print(f"From local path {local_dir} upload file to hdfs path {hdfs_path}  success")


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


def generate_uuid(key):
    hash_object = hashlib.sha256(key.encode())
    hash_hex = hash_object.hexdigest()
    return uuid.UUID(hash_hex[:32])


def configure_environment(app, app_home):
    env_file = get_user_env_filename()

    with open(env_file, 'r+') as file:
        lines = file.readlines()
        path_found = False
        app_found = False
        for line in lines:
            if line.startswith("export PATH="):
                path_found = True
                if line.startswith(f"export {app}"):
                    app_found = True
                    break

        # 如果有 PATH 设置，且不存在app-home 则插入 export app-home 并在 PATH 后添加应用路径
        if path_found:
            if not app_found:
                pass
            else:
                with open(env_file, 'w') as f2:
                    new_lines = []
                    for line in lines:
                        if line.startswith("export PATH="):
                            new_lines.append(f"export {app}={app_home}\n")
                            new_lines.append(line.strip() + f":${app}/bin\n")
                        else:
                            new_lines.append(line)
                    f2.writelines(new_lines)
        else:
            # 如果没有设置 PATH，则直接添加 export app-home 和 export PATH
            with open(env_file, 'a') as f3:
                f3.write(f"export {app}={app_home}\n")
                f3.write(f"export PATH=$PATH:${app}/bin\n")
    exec_shell_command(f"source {env_file}")
    print(f"please run this command to effective environment variables  ->   source {env_file}")


def delete_dir(path):
    if os.path.isdir(path):
        is_empty_dir = len(os.listdir(path)) == 0
        if not is_empty_dir:
            removed = input(f"this path is not empty, do you want remove this {path}? [y/N] ")
            if removed == "y" or removed == "Y" or removed == "yes":
                exec_shell_command(f"rm -rf {path}", f"remove {path}", output=True)


app_home_dir = os.path.join(get_app_home_dir(), module_name)
delete_dir(app_home_dir)
