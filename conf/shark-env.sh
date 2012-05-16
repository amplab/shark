#!/usr/bin/env bash

# Set Spark environment variables for your site in this file. Some useful
# variables to set are:
# - MESOS_HOME, to point to your Mesos installation.
# - SCALA_HOME, to point to your Scala installation.
# - SPARK_CLASSPATH, to add elements to Spark's classpath.
# - SPARK_JAVA_OPTS, to add JVM options.
# - SPARK_MEM, to change the amount of memory used per node (this should
#   be in the same format as the JVM's -Xmx option, e.g. 300m or 1g).
# - SPARK_LIBRARY_PATH, to add extra search paths for native libraries.
# - HIVE_CONF_DIR, to specify the path of Hive configuration files (default HIVE_HOME/conf)

export SCALA_VERSION=2.9.1

# Default EC2 settings
# export SCALA_HOME=/root/scala-$SCALA_VERSION.final
# export MESOS_HOME=/root/mesos
# export HIVE_HOME=/root/hive-0.7.0-bin
# export MASTER=`cat /root/mesos-ec2/cluster-url`

# Set Spark's memory per machine -- you might want to increase this
export SPARK_MEM=3g

export SPARK_JAVA_OPTS="-Dspark.kryoserializer.buffer.mb=10  -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps"

# This is only needed for development (SBT test uses this).
#export HIVE_DEV_HOME=""

