#! /usr/bin/env sh

# Download and build Spark.
git clone https://github.com/mesos/spark.git
pushd spark
sbt/sbt clean compile
popd

# Do we need to checkout and setup an HDFS?
#export HADOOP_HOME="$WORKSPACE/hadoop-0.20.205.0"

# Instead of making and editing a copy of conf/shark-env.sh.template
# just set env variables Shark needs here.
export SPARK_MEM=8g
export SHARK_MASTER_MEM=8g

export HIVE_DEV_HOME="$WORKSPACE/hive"
export HIVE_HOME="$HIVE_DEV_HOME/build/dist"

SPARK_JAVA_OPTS="-Dspark.local.dir=/tmp "
SPARK_JAVA_OPTS+="-Dspark.kryoserializer.buffer.mb=10 "
SPARK_JAVA_OPTS+="-verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps "
export SPARK_JAVA_OPTS

export SCALA_VERSION=2.9.3
export SPARK_HOME="$WORKSPACE/spark"

# Do we need to setup the Hive warehouse directory.
#HIVE_WAREHOUSE=./hive-warehouse
#mkdir -p $HIVE_WAREHOUSE
#chmod chmod 0777 $HIVE_WAREHOUSE

echo JAVA_HOME is $JAVA_HOME

# Download and build Hive.
git clone https://github.com/amplab/hive.git -b shark-0.9
pushd hive
ant package
ant test -Dtestcase=TestCliDriver
popd
