#! /usr/bin/env bash

# This script can be used as an automated way to set up Shark scratch and then run
# the Shark unit tests. It is primarily intended to be called by a continuous
# integration testing framework such as Jenkins to make sure all of the unit tests
# are passing with each new commit, but is also helpful as a reference document
# for new users of Shark to see all of the necessary steps to get up and running.

usage()
{
cat << EOF
usage: $0 options

This script will setup and run the Shark unit tests, setting up the
necessary environment from scratch.

OPTIONS:
   -h      Show this message
   -s      Skip Spark (downloading and building)
   -e      Skip Hive (downloading and building)
   -d      Skip Hadoop (downloading and buildling)
EOF
exit
}

SKIP_SPARK=false
SKIP_HIVE=false
SKIP_HADOOP=false

while getopts "hsed" opt; do
  case $opt in
    h)
      usage
      ;;
    s)
      echo SKIP SPARK
      SKIP_SPARK=true
      ;;
    e)
      echo SKIP HIVE
      SKIP_HIVE=true
      ;;
    d)
      echo SKIP HADOOP
      SKIP_HADOOP=true
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done


set -xe

if [ "x$SHARK_PROJ_DIR" == "x" ] ; then
  pushd `dirname $0` > /dev/null
  SHARK_PROJ_DIR=`pwd`
  popd > /dev/null
fi

# Since this script downloads a lot of stuff, keep it in a subdir workspace.
WORKSPACE="$SHARK_PROJ_DIR/jenkins-test-workspace"
pushd $WORKSPACE

export SCALA_VERSION=2.9.3

if [ "x$JAVA_HOME" == "x" ] ; then
  echo "ERROR: You must set JAVA_HOME."
  exit -1
fi
# Make sure they are running a new enough JDK.
JAVA_VERSION=`${JAVA_HOME}/bin/java -version 2>&1 |awk -F\" '/version/ { print $2 }'`
JAVA_MAJOR_VERSION=`echo $JAVA_VERSION | awk -F\_ '{print $1}' | awk -F\. '{print $2}'`
JAVA_MINOR_VERSION=`echo $JAVA_VERSION | awk -F\_ '{print $1}' | awk -F\. '{print $3}'`
JAVA_SUB_VERSION=`echo $JAVA_VERSION | awk -F\_ '{print $2}'`
if (($JAVA_MAJOR_VERSION < 7)) ||
   ((($JAVA_MAJOR_VERSION == 7)) && (($JAVA_MINOR_VERSION == 0)) && (($JAVA_SUB_VERSION < 21))); then
  echo "You are running Java version ${JAVA_VERSION}, please run Java 1.7.0_21 or newer."
  exit -1
fi

if [ "x$SCALA_HOME" == "x" ] ; then
  echo "ERROR: You must set SCALA_HOME env var."
  exit -1
fi

if [ "x$HADOOP_VERSION" == "x" ] ; then
  HADOOP_VERSION="0.20.205.0"
fi

if [ "x$HADOOP_MAJOR_VERSION" == "x" ] ; then
  HADOOP_MAJOR_VERSION=1
fi

if $SKIP_SPARK ; then
  if [ ! -e "spark" ] ; then
    echo "spark dir must exist when skipping Spark download and build stage."
    exit -1
  fi
else
  # Clean up past Spark artifacts published locally.
  rm -rf ./spark
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
fi
export SPARK_HOME="$WORKSPACE/spark"

if $SKIP_HADOOP ; then
  if [ ! -e "hadoop-${HADOOP_VERSION}" ] ; then
    echo "hadoop-${HADOOP_VERSION} must exist when skipping Hadoop download and build stage."
    exit -1
  fi
else
  rm -rf hadoop-${HADOOP_VERSION}.tar.gz
  # Download and unpack Hadoop and set env variable.
  wget http://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
  tar xvfz hadoop-${HADOOP_VERSION}.tar.gz
fi
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

# Download and build Hive.
if $SKIP_HIVE ; then
  if [ ! -e "hive" -o ! -e "hive-warehouse" ] ; then
    echo "hive and hive-warehouse dirs must exist when skipping Hive download and build stage."
    exit -1
  fi
else
  # Do we need to setup the Hive warehouse directory.
  HIVE_WAREHOUSE=./hive-warehouse
  mkdir -p $HIVE_WAREHOUSE
  chmod 0777 $HIVE_WAREHOUSE

  rm -rf hive
  git clone https://github.com/amplab/hive.git -b shark-0.9
  pushd hive
  ant package
  popd
fi

# Run Hive tests in the background using ant as a background process.
pushd hive
ant test -Dtestcase=TestCliDriver &> output &
# Watch the output of ant test and kill it when the unit tests start running.
still_searching=true
set +e # We expect grep to return non-0 for a while here so disable -e sh flag.
while $still_searching; do
  grep "^\s*\[junit\]" output
  if [[ $? == 0 ]]; then
    kill -s SIGINT %ant
    still_searching=false
  fi
  sleep 1
done
set -e
popd

rm -rf $WORKSPACE/hive/build/test/junit_metastore_db

pushd $SHARK_PROJ_DIR

# Compile and run the Shark tests.
sbt/sbt test

# Hive CLI Tests
bin/dev/test

popd
