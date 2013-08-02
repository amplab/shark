#! /usr/bin/env sh
set -x

export SCALA_VERSION=2.9.3

if [ "x$JAVA_HOME" == "x" ] ; then
  exit "ERROR: You must set JAVA_HOME."
fi

if [ "x$SCALA_HOME" == "x" ] ; then
  exit "ERROR: You must set SCALA_HOME env var."
fi

if [ "x$WORKSPACE" == "x" ] ; then
  pushd `dirname $0` > /dev/null
  WORKSPACE=`pwd`
  popd > /dev/null
fi

# Clean up past Spark artifacts published locally.
rm -rf ~/.ivy2/local/org.spark*
rm -rf ~/.ivy2/cache/org.spark*

# Download and build Spark.
git clone https://github.com/mesos/spark.git
pushd spark
sbt/sbt clean publish-local
popd
export SPARK_HOME="$WORKSPACE/spark"

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

# Do we need to setup the Hive warehouse directory.
#HIVE_WAREHOUSE=./hive-warehouse
#mkdir -p $HIVE_WAREHOUSE
#chmod chmod 0777 $HIVE_WAREHOUSE

# Download and build Hive.
git clone https://github.com/amplab/hive.git -b shark-0.9
pushd hive
ant package
# Run ant test in the background
ant test -Dtestcase=TestCliDriver &> output &
# Watch the output of ant test and kill it when the unit tests start running.
still_searching=true
while $still_searching; do
  grep "^\s*\[junit\]" output
  if [[ $? == 0 ]]; then
    kill -s SIGINT %ant
    still_searching=false
  fi
  sleep 1
done
popd

# Run the Shark tests.
sbt/sbt test:compile
