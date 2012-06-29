#!/usr/bin/env bash

# Set Shark environment variables for your site in this file. Some useful
# variables to set are:
# - MESOS_NATIVE_LIBRARY, to point to your Mesos native library (libmesos.so)
# - SCALA_HOME, to point to your Scala installation
# - SPARK_CLASSPATH, to add elements to Spark's classpath
# - SPARK_JAVA_OPTS, to add JVM options
# - SPARK_MEM, to change the amount of memory used per node (this should
#   be in the same format as the JVM's -Xmx option, e.g. 300m or 1g).
# - SPARK_LIBRARY_PATH, to add extra search paths for native libraries.
# - HIVE_HOME, to point to the Hive binary distribution.
# - HIVE_CONF_DIR, optional, to specify the path of Hive configuration files
#   (default HIVE_HOME/conf)

export SCALA_VERSION=2.9.1

# Set Spark's memory per machine -- you might want to increase this
export SPARK_MEM=3g

# On EC2, change the local.dir to /mnt/tmp
export SPARK_JAVA_OPTS="-Dspark.local.dir=/tmp -Dspark.kryoserializer.buffer.mb=10  -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps"

# This is only needed for development (SBT test uses this).
#export HIVE_DEV_HOME="/root/hive"
#export HIVE_HOME=$HIVE_DEV_HOME/build/dist

# Set these options when running through spark-ec2 scripts
#export MASTER=`cat /root/mesos-ec2/cluster-url`
#export HADOOP_HOME=/root/ephemeral-hdfs
#export MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so
# export SCALA_HOME=/root/scala-$SCALA_VERSION.final
