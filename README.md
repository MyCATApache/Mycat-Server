# 个人博客

[http://www.iocoder.cn](http://www.iocoder.cn/?github)

-------

![](http://www.iocoder.cn/images/common/wechat_mp.jpeg)

> 🙂🙂🙂关注**微信公众号：【芋艿的后端小屋】**有福利：  
> 1. RocketMQ / MyCAT / Sharding-JDBC **所有**源码分析文章列表  
> 2. RocketMQ / MyCAT / Sharding-JDBC **中文注释源码 GitHub 地址**  
> 3. 您对于源码的疑问每条留言**都**将得到**认真**回复。**甚至不知道如何读源码也可以请教噢**。  
> 4. **新的**源码解析文章**实时**收到通知。**每周更新一篇左右**。

-------

* 知识星球：![知识星球](http://www.iocoder.cn/images/Architecture/2017_12_29/01.png)

* 数据库中间件 **MyCAT**
    * [《MyCAT 源码分析 —— 为什么阅读 MyCAT 源码？》](http://www.iocoder.cn/MyCAT/why-read-MyCAT-source-code?github&1603)
    * [《MyCAT 源码分析 —— 调试环境搭建》](http://www.iocoder.cn/MyCAT/build-debugging-environment?github&1603)
    * [《MyCAT 源码分析 —— 【单库单表】插入》](http://www.iocoder.cn/MyCAT/single-db-single-table-insert?github&1603)
    * [《MyCAT 源码分析 —— 【单库单表】查询》](http://www.iocoder.cn/MyCAT/single-db-single-table-select?github&1603)
    * [《MyCAT 源码解析 —— 分片结果合并（一）》](http://www.iocoder.cn/MyCAT/sharding-result-merge-first?github&1603)
    * [《MyCAT 源码分析  —— 跨库两表Join》](http://www.iocoder.cn/MyCAT/two-table-share-join?github&1603)
    * [《MyCAT 源码分析  —— XA分布式事务》](http://www.iocoder.cn/MyCAT/xa-distributed-transaction?github&1603)
    * [《MyCAT 源码分析  —— PreparedStatement 重新入门》](http://www.iocoder.cn/MyCAT/what-is-PreparedStatement?github&1603)
    * [《MyCAT 源码分析  —— SQL ON MongoDB》](http://www.iocoder.cn/MyCAT/connect-mongodb?github&1603)

# [MyCAT](http://mycat.io/)
[![GitHub issues](https://img.shields.io/github/issues/MyCATApache/Mycat-Server.svg)](https://github.com/MyCATApache/Mycat-Server/issues)
[![GitHub forks](https://img.shields.io/github/forks/MyCATApache/Mycat-Server.svg)](https://github.com/MyCATApache/Mycat-Server/network)
[![GitHub stars](https://img.shields.io/github/stars/MyCATApache/Mycat-Server.svg)](https://github.com/MyCATApache/Mycat-Server/stargazers)
[![MyCAT](https://img.shields.io/badge/MyCAT-%E2%9D%A4%EF%B8%8F-%23ff69b4.svg)](http://mycat.io/)

MyCAT is an Open-Source software, “a large database cluster” oriented to enterprises. MyCAT is an enforced database which is a replacement for MySQL and supports transaction and ACID. Regarded as MySQL cluster of enterprise database, MyCAT can take the place of expensive Oracle cluster. MyCAT is also a new type of database, which seems like a SQL Server integrated with the memory cache technology, NoSQL technology and HDFS big data. And as a new modern enterprise database product, MyCAT is combined with the traditional database and new distributed data warehouse. In a word, MyCAT is a fresh new middleware of database.

Mycat’s target is to smoothly migrate the current stand-alone database and applications to cloud side with low cost and to solve the bottleneck problem caused by the rapid growth of data storage and business scale.

* [Getting Started](https://github.com/MyCATApache/Mycat-doc/tree/master/en)
* [尝试 MyCAT](https://github.com/MyCATApache/Mycat-doc/blob/master/MyCat_In_Action_%E4%B8%AD%E6%96%87%E7%89%88.doc)

## Features

* Supports SQL 92 standard
* Supports MySQL cluster, used as a Proxy
* Supports JDBC connection with ORACLE, DB2, SQL Server, simulated as normal MySQL Server connection
* Supports MySQL cluster, percona cluster or mariadb cluster, providing high availability of data fragmentation clusters
* Supports automatic failover and high availability
* Supports separation of read and write, dual-master with multi-slave, single-master with multi-master of MySQL model
* Supports global table, automatically fragment data into multiple nodes for efficient relational query
* Supports the unique fragmentation strategy based on ER-relation for efficient relational query
* Supports multiple platforms, easy deployment and implementation

## Advantage

* Based on Alibaba's open-source project [Cobar](https://github.com/alibaba/cobar), whose stability, reliability, excellent architecture and performance, as well as many mature use-cases make MyCAT have a good starting. Standing on the shoulders of giants, MyCAT feels confident enough to go farther.
* Extensively drawing on the best open-source projects and innovative ideas, which are integrated into the Mycat’s gene, make MyCAT be ahead of the other current similar open-source projects, even beyond some commercial products.
* MyCAT behind a strong technical team whose participants are experienced more than five years including some senior software engineer, architect, DBA, etc. Excellent technical team to ensure the product quality of Mycat.
* MyCAT does not rely on any commercial company. It’s unlike some open-source projects whose important features is enclosed in its commercial products and making open-source projects like a decoration.

## Roadmap

* On the basis of MySQL’s support, MyCAT add more support of commercial open-source database, including native support of PostgreSQL, FireBird and other open-source databases, as well as indirect support via JDBC of other non-open-source databases such as Oracle, DB2, SQL Server etc.
* More intelligent self-regulating properties, such as automatic statistical analysis of SQL, automatic creating and adjusting indexes. Based on the frequency of read and write, MyCAT automatically optimizes caching and backup strategies
* Achieve a more comprehensive monitoring and management
* Integrated with HDFS, provide SQL commands, load databases into HDFS for rapid analysis
* Integrated excellent open-source reporting tools to make MyCAT have data analysis capability 

## Download

There are some compiled binary installation packages in Mycat-download project on github at  [Mycat-download](https://github.com/MyCATApache/Mycat-download).

## Document

There are some documents in Mycat-doc project on github at [Mycat-doc](https://github.com/MyCATApache/Mycat-doc).


Mycat 简单demo，具体参考Mycat权威指南

官网 : mycat.io
qq官方群：106088787
Mycat权威指南官方下载：http://songwie.com/attached/file/mycat_1.5.2.pdf
wiki：<a href="https://github.com/MyCATApache/Mycat-Server/wiki"> wiki</a>

# Mycat前世今生

2013年阿里的Cobar在社区使用过程中发现存在一些比较严重的问题，及其使用限制，经过Mycat发起人第一次改良，第一代改良版——Mycat诞生。 Mycat开源以后，一些Cobar的用户参与了Mycat的开发，最终Mycat发展成为一个由众多软件公司的实力派架构师和资深开发人员维护的社区型开源软件。

2014年Mycat首次在上海的《中华架构师》大会上对外宣讲，更多的人参与进来，随后越来越多的项目采用了Mycat。

2015年5月，由核心参与者们一起编写的第一本官方权威指南《Mycat权威指南》电子版发布，累计超过500本，成为开源项目中的首创。

2015年10月为止，Mycat项目总共有16个Committer。

截至2015年11月，超过300个项目采用Mycat，涵盖银行、电信、电子商务、物流、移动应用、O2O的众多领域和公司。

截至2015年12月，超过4000名用户加群或研究讨论或测试或使用Mycat。

Mycat是基于开源cobar演变而来，我们对cobar的代码进行了彻底的重构，使用NIO重构了网络模块，并且优化了Buffer内核，增强了聚合，Join等基本特性，同时兼容绝大多数数据库成为通用的数据库中间件。1.4 版本以后 完全的脱离基本cobar内核，结合Mycat集群管理、自动扩容、智能优化，成为高性能的中间件。我们致力于开发高性能数据库中间而努力。永不收费，永不闭源，持续推动开源社区的发展。

Mycat吸引和聚集了一大批业内大数据和云计算方面的资深工程师，Mycat的发展壮大基于开源社区志愿者的持续努力，感谢社区志愿者的努力让Mycat更加强大，同时我们也欢迎社区更多的志愿者，特别是公司能够参与进来，参与Mycat的开发，一起推动社区的发展，为社区提供更好的开源中间件。

Mycat还不够强大，Mycat还有很多不足，欢迎社区志愿者的持续优化改进。

#  关键特性
支持SQL92标准

遵守Mysql原生协议，跨语言，跨平台，跨数据库的通用中间件代理。

基于心跳的自动故障切换，支持读写分离，支持MySQL主从，以及galera cluster集群。

支持Galera for MySQL集群，Percona Cluster或者MariaDB cluster

基于Nio实现，有效管理线程，高并发问题。

支持数据的多片自动路由与聚合，支持sum,count,max等常用的聚合函数。

支持单库内部任意join，支持跨库2表join，甚至基于caltlet的多表join。

支持通过全局表，ER关系的分片策略，实现了高效的多表join查询。

支持多租户方案。

支持分布式事务（弱xa）。

支持全局序列号，解决分布式下的主键生成问题。

分片规则丰富，插件化开发，易于扩展。

强大的web，命令行监控。

支持前端作为mysq通用代理，后端JDBC方式支持Oracle、DB2、SQL Server 、 mongodb 、巨杉。

支持密码加密

支持服务降级

支持IP白名单

支持SQL黑名单、sql注入攻击拦截

支持分表（1.6）

集群基于ZooKeeper管理，在线升级，扩容，智能优化，大数据处理（2.0开发版）。


# Mycat安装与使用

## 下载：
[https://github.com/MyCATApache/Mycat-download](https://github.com/MyCATApache/Mycat-download)
具体下载哪个版本以发布为准，推荐1.4,1.5.

## 安装：
下载的文件直接解压即可。

## 运行：
### linux：
   ./mycat start 启动

   ./mycat stop 停止

   ./mycat console 前台运行

   ./mycat install 添加到系统自动启动（暂未实现）

   ./mycat remove 取消随系统自动启动（暂未实现）

   ./mycat restart 重启服务

   ./mycat pause 暂停

   ./mycat status 查看启动状态

### win：
直接运行startup_nowrap.bat，如果出现闪退，在cmd 命令行运行，查看出错原因。 

## 内存配置：
启动前，一般需要修改JVM配置参数，打开conf/wrapper.conf文件，如下行的内容为2G和2048，可根据本机配置情况修改为512M或其它值。
以下配置跟jvm参数完全一致，可以根据自己的jvm参数调整。

Java Additional Parameters

wrapper.java.additional.1=

wrapper.java.additional.1=-DMYCAT_HOME=.

wrapper.java.additional.2=-server

wrapper.java.additional.3=-XX:MaxPermSize=64M

wrapper.java.additional.4=-XX:+AggressiveOpts

wrapper.java.additional.5=-XX:MaxDirectMemorySize=100m

wrapper.java.additional.6=-Dcom.sun.management.jmxremote

wrapper.java.additional.7=-Dcom.sun.management.jmxremote.port=1984

wrapper.java.additional.8=-Dcom.sun.management.jmxremote.authenticate=false

wrapper.java.additional.9=-Dcom.sun.management.jmxremote.ssl=false

wrapper.java.additional.10=-Xmx100m

wrapper.java.additional.11=-Xms100m

wrapper.java.additional.12=-XX:+UseParNewGC

wrapper.java.additional.13=-XX:+UseConcMarkSweepGC

wrapper.java.additional.14=-XX:+UseCMSCompactAtFullCollection

wrapper.java.additional.15=-XX:CMSFullGCsBeforeCompaction=0

wrapper.java.additional.16=-XX:CMSInitiatingOccupancyFraction=70


以下配置作废：

wrapper.java.initmemory=3

wrapper.java.maxmemory=64

### Mycat连接测试：
测试mycat与测试mysql完全一致，mysql怎么连接，mycat就怎么连接。

推荐先采用命令行测试：

mysql -uroot -proot -P8066 -h127.0.0.1

如果采用工具连接，1.4,1.3目前部分工具无法连接，会提示database not selected，建议采用高版本，navicat测试。1.5已经修复了部分工具连接。


# Mycat配置入门

## 配置：
--bin  启动目录

--conf 配置文件存放配置文件：

      --server.xml：是Mycat服务器参数调整和用户授权的配置文件。

      --schema.xml：是逻辑库定义和表以及分片定义的配置文件。

      --rule.xml：  是分片规则的配置文件，分片规则的具体一些参数信息单独存放为文件，也在这个目录下，配置文件修改需要重启MyCAT。

      --log4j.xml： 日志存放在logs/log中，每天一个文件，日志的配置是在conf/log4j.xml中，根据自己的需要可以调整输出级别为debug                           debug级别下，会输出更多的信息，方便排查问题。

      --autopartition-long.txt,partition-hash-int.txt,sequence_conf.properties， sequence_db_conf.properties 分片相关的id分片规则配置文件

      --lib	    MyCAT自身的jar包或依赖的jar包的存放目录。

      --logs        MyCAT日志的存放目录。日志存放在logs/log中，每天一个文件

下面图片描述了Mycat最重要的3大配置文件：
<p>
	<img src="http://songwie.com/attached/image/20160205/20160205164558_154.png" alt="">
</p>

## 逻辑库配置：
### 配置server.xml
添加两个mycat逻辑库：user,pay:
system 参数是所有的mycat参数配置，比如添加解析器：defaultSqlParser，其他类推
user 是用户参数。

	<system>

		<property name="defaultSqlParser">druidparser</property>

	</system>

	<user name="mycat">

		<property name="password">mycat</property>

		<property name="schemas">user,pay</property>

	</user>

### 编辑schema.xml
修改dataHost和schema对应的连接信息，user,pay 垂直切分后的配置如下所示：

schema 是实际逻辑库的配置，user，pay分别对应两个逻辑库，多个schema代表多个逻辑库。

dataNode是逻辑库对应的分片，如果配置多个分片只需要多个dataNode即可。

dataHost是实际的物理库配置地址，可以配置多主主从等其他配置，多个dataHost代表分片对应的物理库地址，下面的writeHost、readHost代表该分片是否配置多写，主从，读写分离等高级特性。

以下例子配置了两个writeHost为主从。

    <schema name="user" checkSQLschema="false" sqlMaxLimit="100" dataNode="user" />
    <schema name="pay"  checkSQLschema="false" sqlMaxLimit="100" dataNode="pay" >
       <table name="order" dataNode="pay1,pay2" rule="rule1"/>
    </schema>

    <dataNode name="user" dataHost="host" database="user" />
    <dataNode name="pay1" dataHost="host" database="pay1" />
    <dataNode name="pay2" dataHost="host" database="pay2" />

    <dataHost name="host" maxCon="1000" minCon="10" balance="0"
       writeType="0" dbType="mysql" dbDriver="native">
       <heartbeat>select 1</heartbeat>
       <!-- can have multi write hosts -->
       <writeHost host="hostM1" url="192.168.0.2:3306" user="root" password="root" />
       <writeHost host="hostM2" url="192.168.0.3:3306" user="root" password="root" />
    </dataHost>
    
    

# Mycat逻辑库、系统参数配置

## 配置Mycat环境参数
    <?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <!DOCTYPE mycat:server SYSTEM "server.dtd">
    <mycat:server xmlns:mycat="http://org.opencloudb/">
       <system>
    	  <property name="defaultSqlParser">druidparser</property>
        </system> 
     </mycat:server>

如例子中配置的所有的Mycat参数变量都是配置在server.xml 文件中，system标签下配置所有的参数，如果需要配置某个变量添加相应的配置即可，例如添加启动端口8066，默认为8066：

       <property name="serverPort">8066</property>

其他所有变量类似。

## 配置Mycat逻辑库与用户

    <?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <!DOCTYPE mycat:server SYSTEM "server.dtd">
    <mycat:server xmlns:mycat="http://org.opencloudb/">
	<user name="mycat">
		<property name="password">mycat</property>
		<property name="schemas">TESTDB</property>
	</user>
     </mycat:server>


如例子中配置的所有的Mycat连接的用户与逻辑库映射都是配置在server.xml 文件中，user标签下配置所有的参数，例如例子中配置了一个mycat用户供应用连接到mycat，同时mycat 在schema.xml中配置后了一个逻辑库TESTDB，配置好逻辑库与用户的映射关系。


# 逻辑库、表分片配置

## 配置逻辑库（schema）

Mycat作为一个中间件，实现mysql协议那么对前端应用连接来说就是一个数据库，也就有数据库的配置，mycat的数据库配置是在schema.xml中配置，配置好后映射到server.xml里面的用户就可以了。

	<?xml version="1.0" encoding="UTF-8"?>
	<!DOCTYPE mycat:schema SYSTEM "schema.dtd">
	
	<mycat:schema  xmlns:mycat="http://org.opencloudb/">
	  <schema name="TESTDB" checkSQLschema="true" sqlMaxLimit="100" dataNode="dn1">
	      <table name="t_user" dataNode="dn1,dn2" rule="sharding-by-mod2"/>
	      <table name="ht_jy_login_log" primaryKey="ID" dataNode="dn1,dn2" rule="sharding-by-date_jylog"/>
	  </schema>
	  <dataNode name="dn1" dataHost="localhost1" database="mycat_node1"/>
	  <dataNode name="dn2" dataHost="localhost1" database="mycat_node2"/>
	  
	  <dataHost name="localhost1" writeType="0" switchType="1" slaveThreshold="100" balance="1" dbType="mysql" maxCon="10" minCon="1" dbDriver="native">
	    <heartbeat>show status like 'wsrep%'</heartbeat>
	    <writeHost host="hostM1" url="127.0.0.1:3306" user="root" password="root" >
	    </writeHost>  
	  </dataHost>
	</mycat:schema >

上面例子配置了一个逻辑库TESTDB，同时配置了t_user，ht_jy_login_log两个分片表。

### 逻辑表配置
	      <table name="t_user" dataNode="dn1,dn2" rule="sharding-by-mod2"/>

table 标签 是逻辑表的配置 其中 

name代表表名，

dataNode代表表对应的分片，

Mycat默认采用分库方式，也就是一个表映射到不同的库上，

rule代表表要采用的数据切分方式，名称对应到rule.xml中的对应配置，如果要分片必须配置。


## 配置分片（dataNode）

	  <dataNode name="dn1" dataHost="localhost1" database="mycat_node1"/>
	  <dataNode name="dn2" dataHost="localhost1" database="mycat_node2"/>

表切分后需要配置映射到哪几个数据库中，Mycat的分片实际上就是库的别名，例如上面例子配置了两个分片dn1，dn2 分别对应到物理机映射dataHost
localhost1 的两个库上。

## 配置物理库分片映射（dataHost）

	  <dataHost name="localhost1" writeType="0" switchType="1" slaveThreshold="100" balance="1" dbType="mysql" maxCon="10" minCon="1" dbDriver="native">
	    <heartbeat>show status like 'wsrep%'</heartbeat>
	    <writeHost host="hostM1" url="127.0.0.1:3306" user="root" password="root" >
	    </writeHost>  
	  </dataHost>

Mycat作为数据库代理需要逻辑库，逻辑用户，表切分后需要配置分片，分片也就需要映射到真实的物理主机上，至于是映射到一台还是一台的多个实例上，Mycat并不关心，只需要配置好映射即可，例如例子中：

配置了一个名为localhost1的物理主机（dataHost）映射。

heartbeat 标签代表Mycat需要对物理库心跳检测的语句，正常情况下生产案例可能配置主从，或者多写 或者单库，无论哪种情况Mycat都需要维持到数据库的数据源连接，因此需要定时检查后端连接可以性，心跳语句就是来作为心跳检测。

writeHost 此标签代表 一个逻辑主机（dataHost）对应的后端的物理主机映射，例如例子中写库hostM1 映射到127.0.0.1:3306。如果后端需要做读写分离或者多写 或者主从则通过配置 多个writeHost 或者readHost即可。

dataHost 标签中的 writeType balance 等标签则是不同的策略，具体参考指南。

# Mycat 表切分规则配置

## 表切分规则

	<?xml version="1.0" encoding="UTF-8"?>
	<!DOCTYPE mycat:rule SYSTEM "rule.dtd">
	
	<mycat:rule  xmlns:mycat="http://org.opencloudb/">
	  <tableRule name="sharding-by-hour">
	    <rule>
	      <columns>createTime</columns>
	      <algorithm>sharding-by-hour</algorithm>
	    </rule>
	  </tableRule>
	  
	  <function name="sharding-by-hour" class="org.opencloudb.route.function.LatestMonthPartion">
	    <property name="splitOneDay">24</property>
	  </function>
	   
	</mycat:rule >

数据切分中作为表切分规则中最重要的配置，表的切分方式决定了数据切分后的性能好坏，因此也是最重要的配置。

如上面例子配置了一个切分规则，名为sharding-by-hour 对应的切分方式（function ）是按日期切分，该配置中：

### tableRule

name 为schema.xml 中table 标签中对应的 rule="sharding-by-hour" ,也就是配置表的分片规则，

columns 是表的切分字段： createTime 创建日期。

algorithm 是规则对应的切分规则：映射到function 的name。


### function 

function 配置是分片规则的配置。

name 为切分规则的名称，名字人员取，但是需要与tableRule 中匹配。

class 是切分规则对应的切分类，写死，需要哪种规则则配置哪种，例如本例子是按小时分片：org.opencloudb.route.function.LatestMonthPartion

property 标签是切分规则对应的不同属性，不同的切分规则配置不同。


