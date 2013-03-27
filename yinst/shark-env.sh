#!/usr/bin/env bash

# Copyright (C) 2012 The Regents of The University California.
# All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# (Required) Amount of memory used per slave node. This should be in the same
# format as the JVM's -Xmx option, e.g. 300m or 1g.
export SPARK_MEM=$(miners_shark__SPARK_MEM)

# (Required) Set the master program's memory
export SHARK_MASTER_MEM=$(miners_shark__SHARK_MASTER_MEM)

# (Required) Point to your Scala installation.
export SCALA_VERSION=2.9.2
export SCALA_HOME=$(miners_shark__SCALA_HOME)

export JAVA_HOME=/home/y/libexec64/jdk1.7.0/

# (Required) Point to the patched Hive binary distribution
export HIVE_HOME=$(miners_shark__HIVE_HOME)

# (Optional) Specify the location of Hive's configuration directory. By default,
# it points to $HIVE_HOME/conf
#export HIVE_CONF_DIR="$HIVE_HOME/conf"

# For running Shark in distributed mode, set the following:
export HADOOP_HOME=$(miners_shark__HADOOP_HOME)
export HADOOP_PREFIX=$(miners_shark__HADOOP_PREFIX)
export SPARK_HOME=$(miners_shark__SPARK_HOME)

export MASTER=$(miners_shark__MASTER)
export SPARK_MASTER_IP=$(miners_shark__SPARK_MASTER_IP)
export SPARK_MASTER_PORT=$(miners_shark__SPARK_MASTER_PORT)
#export MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so

# (Optional) Extra classpath
#export SPARK_LIBRARY_PATH=""

# Java options
# On EC2, change the local.dir to /mnt/tmp
export GC_OPTS="-verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps"
export DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8787"

SPARK_JAVA_OPTS="$DEBUG_OPTS -XX:-UseSplitVerifier -XX:ReservedCodeCacheSize=256m -XX:MaxPermSize=1g "
SPARK_JAVA_OPTS+="-Dspark.local.dir=/tmp "
SPARK_JAVA_OPTS+="-Dspark.kryoserializer.buffer.mb=10 "
SPARK_JAVA_OPTS+="-verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps "
export SPARK_JAVA_OPTS
