# Shark (Hive on Spark)

Shark is a large-scale data warehouse system for Spark designed to be compatible with
Apache Hive. It can answer Hive QL queries up to 30 times faster than Hive without
modification to the existing data nor queries. Shark supports Hive's query language,
metastore, serialization formats, and user-defined functions.


## Build

Shark requires Hive 0.7.0 and Spark (0.4-SNAPSHOT).

Get Hive from Apache:

    $ export HIVE_HOME=/path/to/hive
    $ wget http://archive.apache.org/dist/hive/hive-0.7.0/hive-0.7.0-bin.tar.gz
    $ tar xvzf hive-0.7.0-bin.tar.gz
    $ mv hive-0.7.0-bin $HIVE_HOME

Get Spark from Github, compile, and publish to local ivy:

    $ git clone git://github.com/mesos/spark.git spark 
    $ cd spark 
    $ sbt/sbt publish-local

Get Shark from Github:

    $ git clone git://github.com/amplab/shark.git shark
    $ cd shark

Before building Shark, first modify the config file:

    $ conf/shark-env.sh 

Compile Shark (make sure `$HIVE_HOME` is either set in config file or environmental variable):

    $ sbt/sbt compile


## Execution

There are several executables in /bin:

* `shark`: Runs Shark CLI.
* `shark-withinfo`: Runs Shark with INFO level logs printed to the console.
* `shark-withdebug`: Runs Shark with DEBUG level logs printed to the console.
* `shark-shell`: Runs Shark scala console. This provides an experimental feature
to convert Hive QL queries into `TableRDD`.
* `clear-buffer-cache.py`: Automatically clears OS buffer caches on Mesos EC2
clusters. This is handy for performance studies.


## Runtime Configuration

Shark reuses Hive's configuration files, which are loaded from `$HIVE_HOME/conf`.

We also include a few Shark-specific configuration parameters that can be set
in the same way as you would set configuration parameters in Hive (e.g. from the 
Shark CLI):

    shark> shark.exec.mode = [hive | shark (default)]
    shark> shark.explain.mode = [hive | shark (default)]


## Caching

Shark caches tables in memory as long as their name ends in "`_cached`". For example, 
if you have a table named "test", you can create a cached version of it as follows:

    shark> CREATE TABLE test_cached AS SELECT * FROM test;


References
----------
For information on setting up Hive or HiveQl, please read:
https://cwiki.apache.org/confluence/display/Hive/GettingStarted

For information on Spark, please read:
https://github.com/mesos/spark


