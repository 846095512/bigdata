本项目用于自动部署一些常见应用脚本

脚本约定：

1、默认部署目录 root 用户为 /opt/app 普通用户为 /home/$user/app

2、安装包存放目录  root 用户为 /opt/package 普通用户为 /home/$user/package



部署 jdk 参数

```json
{
    "file":"jdk安装包名",
    "module":"jdk"
}
```

部署 zk 参数
```json
{
    "file":"zk安装包名",
    "module":"zookeeper",
    "jvm.heapsize":"1g",
    "install.role":"standalone/cluster",
    "local.ip":"192.168.0.1",
    "install.ip":["192.168.0.1"]
}
```

部署 hadoop 参数

```json
{
    "file":"hadoop安装包名",
    "module":"hadoop",
    "local.ip":"192.168.0.1",
    "install.role":"standalone/cluster",
    "dfs.nameservice":"192.168.0.1:9000",
    "install.ip":["192.168.0.1","192.168.0.2","192.168.0.3"],
    "namenode.list":["192.168.0.1","192.168.0.2","192.168.0.3"],
    "resourcemanager.list":["192.168.0.1","192.168.0.2","192.168.0.3"],
    "journalnode.list":["192.168.0.1","192.168.0.2","192.168.0.3"],
    "zk.addr":"192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181",
    "jvm.heapsize":"1g",
    "yarn.cluster.id":"yarncluster",
    "yarn.mem":"1024",
    "yarn.cpu":"1"
}
```

 部署 spark 参数

```json
{
    "file":"spark安装包名",
    "module":"spark",
    "local.ip":"192.168.0.1",
    "install.role":"standalone/cluster/yarn",
    "dfs.nameservice":"192.168.0.1:9000",
    "spark.master.ip":["192.168.176.134"],
    "spark.cluster.id":"sparkcluster",
    "jvm.heapsize":"1g",
    "zk.addr":"192.168.0.1"
}
```

 部署 mysql8 参数
非root用户运行时
ubuntu 系统
sudo apt remove mariadb* -y

sudo apt install libncurses5 -y

centos 系统
sudo yum remove mariadb* -y

sudo yum install ncurses-compat-libs -y
```json
{
    "file":"mysql8安装包名",
    "module":"mysql",
    "local.ip":"192.168.0.1",
    "install.role":"standalone/cluster",
    "install.ip":["192.168.0.1"]
}
```


