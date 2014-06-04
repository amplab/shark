# Shark (Hive on Spark) 1.0.0-PREVIEW

**This a developer preview of Shark using Spark SQL instead of the Hive optimizer.  Since this is a nearly from scratch rewrite some features are currently missing.**

Shark is a large-scale data warehouse system for Spark designed to be compatible with
Apache Hive. Shark supports Hive's query language, metastore, serialization formats, and 
user-defined functions.

Currently this preview release only supports the SharkServer2 JDBC server.  Users who would like to run [HQL](https://cwiki.apache.org/confluence/display/Hive/LanguageManual) queries alongside their Spark jobs should check out the interfaces provided by [Spark SQL](http://people.apache.org/~pwendell/catalyst-docs/sql-programming-guide.html)

## Running SharkServer2

SharkSever2 is a port of the [HiveSever2 JDBC server](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2).  It can be started by running `bin/shark --service HiveServer2`.  By default, the server will create a local metastore and warehouse.  To connect to an existing Hive instalation, simply place your `hive-site.xml` file in the `conf/` directory before starting the server.  More information about Hive configuration can be found on the [Hive wiki](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Configuration/)

## Connecting to a SharkServer2 Instance

The easiest way to connect to a SharkServer2 instance is through the beeline commandline client, which is included in the Shark distribution at `bin/beeline`.  For example:

```
$ bin/beeline
Beeline version 0.12.0 by Apache Hive
beeline> !connect jdbc:hive2://localhost:10000/
scan complete in 4ms
Connecting to jdbc:hive2://localhost:10000/
Enter username for jdbc:hive2://localhost:10000/: 
Enter password for jdbc:hive2://localhost:10000/: 
Connected to: Hive (version 0.12.0)
Driver: Hive (version 0.12.0)
Transaction isolation: TRANSACTION_REPEATABLE_READ
0: jdbc:hive2://localhost:10000/> CREATE TABLE src (key INT, value STRING);                  
No rows affected (1.296 seconds)
0: jdbc:hive2://localhost:10000/> LOAD DATA LOCAL INPATH 'data/files/kv1.txt' INTO TABLE src;
No rows affected (0.531 seconds)
0: jdbc:hive2://localhost:10000/> SELECT * FROM src;                                         
+------+----------+
| key  |  value   |
+------+----------+
| 238  | val_238  |
| 86   | val_86   |
| 311  | val_311  |
| 27   | val_27   |
| 165  | val_165  |
| 409  | val_409  |
...
```

You can also connect to a SharkServer2 instance using the Hive JDBC driver.  For example:

```scala
import java.sql.DriverManager

val connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/", "", "")
val statement = connection.createStatement()

statement.execute("CREATE TABLE src (key INT, value STRING)")
statement.execute("LOAD DATA LOCAL INPATH 'data/files/kv1.txt' INTO TABLE src");
val resultSet = statement.executeQuery("SELECT COUNT(*) FROM src")
resultSet.next()
println(resultSet.getInt(1))
```

## Running Shark CLI
* Configure the shark_home/conf/shark-env.sh
* Configure the shark_home/conf/hive-site.xml
* Start the Shark CLI
```
$ bin/shark
catalyst> show tables;
catalyst> set shark.exec.mode=hive;
hive>show tables;
```
But there is a bug, which require show tables before doing anything else.

## Known Missing Features
* Invalidation of cached tables when data is INSERTed
* Off-heap storage using Tachyon
* TGFs
* ...
