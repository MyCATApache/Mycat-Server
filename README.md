
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
