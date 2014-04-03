#
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
#


import os
import stat


from py4j.java_gateway import java_import
from pyspark.java_gateway import launch_gateway


SPARK_HOME = os.environ['SPARK_HOME']


SHARK_HOME = os.environ["SHARK_HOME"]
HIVE_CONF_DIR = os.environ.get("HIVE_CONF_DIR")
if not HIVE_CONF_DIR:
    os.environ["HIVE_CONF_DIR"] = os.path.join(SHARK_HOME, 'conf')

COMPUTE_CLASSPATH_INJECTION = """
if [ "x" != "x$HIVE_CONF_DIR" ]; then
    CLASSPATH="$CLASSPATH:$HIVE_CONF_DIR"
else
    CLASSPATH="$CLASSPATH:$SHARK_HOME/conf"
fi

if [ "x" != "x$SHARK_HOME" ]; then
    SHARKJAR=`ls "$SHARK_HOME"/target/scala-"$SCALA_VERSION"/shark-assembly*hadoop*.jar`
    CLASSPATH="$CLASSPATH:$SHARKJAR"
fi

"""


def launch_shark_gateway():
    try:
        # The shark jar needs to be added at the end of the classpath
        # for the gateway otherwise it causes problems. This is a 
        # terrible hack and should be fixed some other way. Also,
        # this currently is untested on windows.
        computeClasspath = os.path.join(SPARK_HOME, 'bin/compute-classpath.sh')
        computeClasspathOrig = os.path.join(SPARK_HOME, 'bin/compute-classpath.sh.orig')
        os.rename(computeClasspath, computeClasspathOrig)
        with open(computeClasspath, 'w') as compCp:
            with open(computeClasspathOrig, 'rb') as compCpOrig:
                lines = compCpOrig.readlines()
                for line in lines[:-1]:
                    compCp.write("%s" % line)
                compCp.write(COMPUTE_CLASSPATH_INJECTION)
                compCp.write('%s\n' % lines[-1])
        st = os.stat(computeClasspath)
        os.chmod(computeClasspath, st.st_mode | stat.S_IEXEC)
        # Launch the standard Py4j spark gateway
        gateway = launch_gateway()
        # Clean up after the compute-classpath hack
        os.rename(computeClasspathOrig, computeClasspath)
        # Import additional classes used by PyShark
        java_import(gateway.jvm, "shark.api.*")
        java_import(gateway.jvm, "shark.SharkEnv")
        return gateway
    except:
        # Clean up after the compute-classpath hack
        os.rename(computeClasspathOrig, computeClasspath)
        raise
