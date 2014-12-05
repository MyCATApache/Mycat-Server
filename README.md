### An Introduction to Mycat

Mycat is an Open-Source software, “a big database cluster” oriented to the enterprises. Mycat is an enforced database which is a replacement for MySQL and support transaction and ACID. Regarded as MySQL cluster of enterprise database, Mycat can take the place of expensive Oracle cluster. Mycat is also a new type of SQL Server integrated with the memory cache technology, Nosql technology and HDFS big data. And as a new modern enterprise database product, Mycat is combined with the traditional database and new distributed data warehouse. In a word, Mycat is a fresh new middleware of database..

Mycat’s target is to smoothly migrate the current stand-alone database and applications to cloud side with low cost and to solve the bottleneck problem caused by the rapid growth of data storage and business scale.

##### Mycat Key Features:

* Supports SQL 92 standard
* Supports MySQL cluster, used as a Proxy
* Supports JDBC connection with ORACLE, DB2, SQL Server, simulated as normal MySQL Server connection
* Supports MySQL cluster, percona cluster or mariadb cluster, providing high availability of data fragmentation clusters
* Supports automatic failover and high availability
* Supports separation of read and write, dual-master with multi-slave, single-master with multi-master of MySQL model
* Supports global table, automatically fragment data into multiple nodes for efficient relational query
* Supports the unique fragmentation strategy based on ER-relation for efficient relational query
* Supports multi-platform, easy deployment and implementation

##### Mycat adventage:

* Based on Ali open-source product Cobar, whose stability, reliability, excellent architecture and performance, as well as many mature use-case make Mycat have a good starting. Standing on the shoulders of giants, Mycat can be able to go farther.
* Extensively drawing on the best open-source projects and innovative ideas, which are integrated into the Mycat’s gene, make Mycat be ahead of the other current similar open-source projects, even beyond some commercial products.
* Mycat behind a strong technical team whose participants are experienced more than five years including some senior software engineer, architect, DBA, etc. Excellent technical team to ensure the product quality of Mycat.
* Mycat does not rely on any commercial company. It’s unlike some open-source projects whose important features is enclosed in its commercial products and making open-source projects like a decoration.

##### Mycat long-term plan:

* On the basis of MySQL’s support, Mycat add more support of commercial open-source database, including native support of PosteSQL, FireBird and other open-source databases, as well as indirect support via JDBC of other non-open-source databases such as Oracle, DB2, SQL Server etc.
* More intelligent self-regulating properties, such as automatic statistical analysis of SQL, automatic creating and adjusting indexes. Based on the frequency of reading and writing, Mycat automatically optimize caching and backup strategies
* Achieve a more comprehensive monitoring and management
* Integrated with HDFS, provide SQL commands, load databases into HDFS for rapid analysis
* Integrated excellent open-source reporting tools to make Mycat have data analysis capability 