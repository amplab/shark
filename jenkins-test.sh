#! /usr/bin/env sh
set -x


if [ "x$SPARK_PROJ_DIR" == "x" ] ; then
  pushd `dirname $0` > /dev/null
  SPARK_PROJ_DIR=`pwd`
  popd > /dev/null
fi

# Since this script downloads a lot of stuff, keep it in a subdir workspace.
WORKSPACE="$SPARK_PROJ_DIR/jenkins-test-workspace"
if [ "x$WORKSPACE" == "x/" ] ; then
  exit '$WORKSPACE cannot be / since we delete and recreate it.'
fi
rm -rf $WORKSPACE
mkdir $WORKSPACE
pushd $WORKSPACE

export SCALA_VERSION=2.9.3

if [ "x$JAVA_HOME" == "x" ] ; then
  exit "ERROR: You must set JAVA_HOME."
fi

if [ "x$SCALA_HOME" == "x" ] ; then
  exit "ERROR: You must set SCALA_HOME env var."
fi

if [ "x$HADOOP_VERSION" == "x" ] ; then
  HADOOP_VERSION="0.20.205.0"
fi

if [ "x$HADOOP_MAJOR_VERSION" == "x" ] ; then
  HADOOP_MAJOR_VERSION=1
fi

# Clean up past Spark artifacts published locally.
rm -rf ~/.ivy2/local/org.spark*
rm -rf ~/.ivy2/cache/org.spark*

# Download and build Spark.
git clone https://github.com/mesos/spark.git
pushd spark
# Replace Hadoop1 settings in build file, which are the default, with Hadoop2 settings.
sed -i.backup "s/val HADOOP_VERSION = \"1\.0\.4\"/val HADOOP_VERSION = \"$HADOOP_VERSION\"/" project/SparkBuild.scala
sed -i.backup "s/val HADOOP_MAJOR_VERSION = \"1\"/val HADOOP_MAJOR_VERSION = \"$HADOOP_MAJOR_VERSION\"/" project/SparkBuild.scala
# Build spark and push the jars to local Ivy/Maven caches.
sbt/sbt clean publish-local
popd
export SPARK_HOME="$WORKSPACE/spark"

# Download and unpack Hadoop and set env variable.
wget http://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
tar xvfz hadoop-${HADOOP_VERSION}.tar.gz
export HADOOP_HOME="$WORKSPACE/hadoop-${HADOOP_VERSION}"

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

# Compile and run the Shark tests.
sbt/sbt test

# Hive CLI Tests
bin/dev/test
