Golang开发DB2应用程序

## 1.简介

出于项目需要，并行运维组的监控系统要支持Oracle、DB2等商业数据库。

而Golang语言发展的正是如火如荼，所以Telegraf的采集端使用Golang开发。而Golang访问Oracle数据库的驱动目前只有go-oci8

> https://github.com/wendal/go-oci8

所以最终采集端使用Golang+go-oci8进行开发。

## 2.环境准备

本文档默认你已经在开发机上安装了DB2 V9.7版本，并且已经创建了数据库实例db2inst1，用户名为db2inst1密码为12345678

以下所有操作都在数据库实例用户db2inst1环境下进行

### 2.1.操作系统选择

DB2客户端开发环境支持RHEL5.x/6.x/7.x系统

### 2.2.下载go1.8

本次开发使用最新的go1.8版本。下载到/opt/目录下解压可以得到/opt/go目录.

配置go运行环境变量

修改/home/db2inst1/.bashrc文件，追加如下信息到此文件

	export GOROOT=/opt/go

	export GOPATH=/home/db2inst1/go

	export GOBIN=$GOPATH/bin

	export PATH=$PATH:$GOBIN:$GOROOT/bin
	
	export DB2HOME=$HOME/sqllib

	export CGO_LDFLAGS=-L$DB2HOME/lib

	export CGO_CFLAGS=-I$DB2HOME/include


### 2.3.创建工作目录

在db2inst1用户的家目录/home/db2inst1下创建开发工作目录

> $ mkdir /home/db2inst1/go

> $ cd /home/db2inst1/go; mkdir src pkg bin

### 2.4.下载开发库文件

> $ go get bitbucket.org/phiggins/go-db2-cli


## 3.编译及运行程序

### 3.1.初始化最小程序

下载最小程序到/home/db2inst1/go/src/main.go文件，最小程序下载链接

> https://bitbucket.org/phiggins/go-db2-cli

### 3.2.编译程序

> $ go build main.go

### 3.3.运行程序

$ main  -conn 'DATABASE=testdb;HOSTNAME=db2;PORT=50002;PROTOCOL=TCPIP;UID=db2inst1;PWD=12345678'



