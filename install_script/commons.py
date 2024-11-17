# -*- coding: utf-8 -*-
import ipaddress
import json
import math
import os
import shutil
import subprocess
import sys
import tarfile
import zipfile

from jinja2 import Template

def is_valid_ip_nums(*args):
    for arg in args:
        if isinstance(arg, str):
            is_valid_ip(arg)
        else:
            ip_nums = len(arg)
            if ip_nums >= 1 and ip_nums % 2 != 0:
                for ip in arg:
                    is_valid_ip(ip)
                return True
            else:
                print("ip地址格式错误或ip地址个数不正确,请检查ip配置参数")
                sys.exit(1)

def is_valid_ip(ip):
    try:
        ipaddress.ip_address(ip)
        return True
    except ValueError:
        print("ip 地址不合法")
        sys.exit(1)

def get_user_env_filename():
    os_name = get_os_name()
    current_user = os.getlogin()

    if os_name == "ubuntu":
        return f"/home/{current_user}/.profile"
    else:
        return f"/home/{current_user}/.bash_profile"

def get_os_name():
    command = "cat /etc/*-release | grep -wi 'id' | awk -F '=' '{print $2}' | sed 's/\"//g' | tr 'A-Z' 'a-z' "
    result = str(subprocess.run(command, shell=True, capture_output=True, text=True).stdout).strip()
    return result

def get_install_config():
    script_path = os.path.dirname(os.path.abspath(__file__))
    with open(f'{script_path}/conf.json', "r", encoding="utf-8") as f:
        params_dit = json.load(f)
    return params_dit

def get_root_dir():
    current_user = os.getlogin()
    if current_user == "root":
        root_dir = "/opt/app"
    else:
        root_dir = "/home/" + current_user + "/app"
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    return root_dir

def exec_shell_command( command):
    try:
        result = subprocess.run(command, shell=True, capture_output= True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(e)
        sys.exit(1)

def set_permissions( path):
    current_user = os.getlogin()
    for dirpath, dirnames, filenames in os.walk(path):
        for dirname in dirnames:
            os.chmod(os.path.join(dirpath, dirname), 0o750)
        for filename in filenames:
            os.chmod(os.path.join(dirpath, filename), 0o750)
    exec_shell_command(f"chown -R {current_user}:{current_user} {path}")
    print("设置目录权限完成")

def unzip_package():
    # 解析参数
    args = get_install_config()
    script_path = os.path.dirname(os.path.abspath(__file__))
    filename =  args["file"]
    module_name = args["module"]
    
    root_dir = get_root_dir()
    print(f"root_dir is {root_dir}")

    # 获取文件后缀
    if filename.endswith('.tar.gz'):
        filename_suffix = ".tar.gz"
    elif filename.endswith('.zip'):
        filename_suffix = ".zip"
    else:
        print("不支持解压的类型！！")
        sys.exit(1)
    print(f"文件为{filename_suffix}压缩类型")

    if filename_suffix == ".tar.gz" or filename_suffix == ".tgz":
        with tarfile.open(f"{script_path}/{filename}", 'r') as tar_ref:
            tar_ref.extractall(root_dir)
    elif filename_suffix == ".zip":
        with zipfile.ZipFile(f"{script_path}/{filename}", 'r') as zip_ref:
            zip_ref.extractall(root_dir)
    else:
        print("不支持的压缩包类型")
    print(f"文件解压完成")
    
    unpack_name = exec_shell_command(f"tar -tzf {script_path}/{filename}  | head -1 | cut -d'/' -f1")
    old_path = os.path.join(root_dir, unpack_name)
    new_path = os.path.join(root_dir, module_name)
    shutil.move(old_path, new_path)
    print(f"目录移动完成，{old_path} -> {new_path}")

def generate_config_file(template_str, conf_file, keyword, **kwargs):
    template = Template(template_str)
    config_content = template.render(kwargs)
    if keyword == "":
        insert_line_num = 1
    else:
        insert_line_num = exec_shell_command(f"sed -n \"/{keyword}/=\" {conf_file}")
        with open(conf_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            lines.insert(int(insert_line_num),config_content)
        with open(conf_file, "w", encoding="utf-8") as f:
            f.writelines(lines)
    print(f"生成{conf_file}文件完成")
