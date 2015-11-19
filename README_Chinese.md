### Mycat介绍

### 官网:[http://www.mycat.org.cn](http://www.mycat.org.cn)

### github:[https://github.com/MyCATApache](https://github.com/MyCATApache)
##### 入门: [zh-CN: https://github.com/MyCATApache/Mycat-doc/blob/master/MyCat_In_Action_%E4%B8%AD%E6%96%87%E7%89%88.doc] [English:https://github.com/MyCATApache/Mycat-doc/tree/master/en]

什么是Mycat？简单的说，Mycat就是：

*	一个彻底开源的，面向企业应用开发的“大数据库集群”
*	支持事务、ACID、可以替代MySQL的加强版数据库
*	一个可以视为“MySQL”集群的企业级数据库，用来替代昂贵的Oracle集群
*	一个融合内存缓存技术、Nosql技术、HDFS大数据的新型SQL Server
*	结合传统数据库和新型分布式数据仓库的新一代企业级数据库产品
*	一个新颖的数据库中间件产品



##### Mycat的目标是：

低成本的将现有的单机数据库和应用平滑迁移到“云”端，解决数据存储和业务规模迅速增长情况下的数据瓶颈问题。


##### Mycat的关键特性：

*	支持 SQL 92标准
*	支持MySQL集群，可以作为Proxy使用
*	支持JDBC连接ORACLE、DB2、SQL Server，将其模拟为MySQL  Server使用
*	支持galera for MySQL集群，percona-cluster或者mariadb cluster，提供高可用性数据分片集群
*	自动故障切换，高可用性
*	支持读写分离，支持MySQL双主多从，以及一主多从的模式
*	支持全局表，数据自动分片到多个节点，用于高效表关联查询
*	支持独有的基于E-R 关系的分片策略，实现了高效的表关联查询
*	多平台支持，部署和实施简单


##### Mycat的优势：

*	基于阿里开源的Cobar产品而研发，Cobar的稳定性、可靠性、优秀的架构和性能，以及众多成熟的使用案例使得Mycat一开始就拥有一个很好的起点，站在巨人的肩膀上，我们能看到更远。
*	广泛吸取业界优秀的开源项目和创新思路，将其融入到Mycat的基因中，使得Mycat在很多方面都领先于目前其他一些同类的开源项目，甚至超越某些商业产品。
*	Mycat背后有一只强大的技术团队，其参与者都是5年以上资深软件工程师、架构师、DBA等，优秀的技术团队保证了Mycat的产品质量。
*	Mycat并不依托于任何一个商业公司，因此不像某些开源项目，将一些重要的特性封闭在其商业产品中，使得开源项目成了一个摆设。


##### Mycat的长期路线规划：

*	在支持MySQL的基础上，后端增加更多的开源数据库和商业数据库的支持，包括原生支持PosteSQL、FireBird等开源数据库，以及通过JDBC等方式间接支持其他非开源的数据库如Oracle、DB2、SQL Server等
*	实现更为智能的自我调节特性，如自动统计分析SQL，自动创建和调整索引，根据数据表的读写频率，自动优化缓存和备份策略等
*	实现更全面的监控管理功能
*	与HDFS集成，提供SQL命令，将数据库装入HDFS中并能够快速分析
*	集成优秀的开源报表工具，使之具备一定的数据分析的能力

##### 下载：

github上面的Mycat-download项目是编译好的二进制安装包 [https://github.com/MyCATApache/Mycat-download](https://github.com/MyCATApache/Mycat-download)

##### 文档：

github上面的Mycat-doc项目是相关文档 [https://github.com/MyCATApache/Mycat-doc](https://github.com/MyCATApache/Mycat-doc)

##### 尝试 Mycat -- Mycat All in One
在这里我们为您提供了集 mycat-server,mycat-web,mysql，zookeeper 于一身的测试开发环境，是您开发测试必备良器，
您只需要执行如下几个步骤便可开启属于您的 mycat 之旅 ：

> <b>导入 OVA </b> 
>* 安装Oracle VM VirtualBox
>* 启动Oracle VM VirtualBox
>* 下载 mycat-all-in-one 镜像文件，[戳这里下载all-in-one镜像](http://pan.baidu.com/s/1qWMkJPM),密码：v63y  
>* File（管理） -> Import Appliances（导入虚拟电脑）<网络模式首选桥接模式>
>* 选择CentOS 7.ova
>* 一路Next

><b> 启动虚拟机 </b>
>* 登录虚拟机 root/123456
>* 启动多实例Mysql

   ```
      mysqld_multi start
   ```

> <b>启动 Mycat</b>
 ```
    cd /opt/mycat/
    ./bin/mycat start
 ```
 > <b>  ZK启动 </b>
    
```
    cd /opt/zookeeper-3.4.6
	bin/zkServer.sh start
	bin/zkCli.sh
```
> <b>体验 Mycat</b> 
   >* 启动Navicat Premium
   >* 连接Mycat，IP:8066 test/test
   >* 连接TESTDB
   >* 测试

```
      select * from t_user;
```
<b>请留意 '体验 Mycat'该步骤中的 IP 地址的设定，虚拟机中 IP 地址若与主机地址不匹配会引发连接失败的情况，
此时可以将 虚拟机IP 地址修改静态IP地址来解决，修改位于路径
````
/etc/sysconfig/network-scripts/ifcfg-enp0s3 
````
下面的文件，然后运行命令 
````
service network restart
````
来让刚刚修改过的文件生效即可</b>