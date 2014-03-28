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


import sys
import os
sys.path.insert(0, os.path.join(os.path.abspath(os.environ["SPARK_HOME"]), "python"))


from pyspark import SparkContext, RDD
from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer


from pyshark.java_gateway import launch_shark_gateway


class SharkContext(SparkContext):
    """
    Main entry point for Shark functionality. A SharkContext represents the
    connection to a Spark cluster and the connection to a hive metastore. It
    can be used to run SQL queries, create RDDs from SQL queries, and perform
    all functionality of the base SharkContext.
    """

    @classmethod
    def _ensure_initialized(cls, instance=None, gateway=None):
        with SharkContext._lock:
            if not SharkContext._gateway:
                SharkContext._gateway = gateway or launch_shark_gateway()
                SharkContext._jvm = SharkContext._gateway.jvm
                SharkContext._writeToFile = SharkContext._jvm.PythonRDD.writeToFile
            if instance:
                if SharkContext._active_spark_context and SharkContext._active_spark_context != instance:
                    currentMaster = SharkContext._active_spark_context.master
                    currentAppName = SharkContext._active_spark_context.appName
                    callsite = SharkContext._active_spark_context._callsite
                    # Raise error if there is already a running Spark context
                    raise ValueError("Cannot run multiple SharkContexts at once; existing SharkContext(app=%s, master=%s)" \
                        " created by %s at %s:%s " \
                        % (currentAppName, currentMaster, callsite.function, callsite.file, callsite.linenum))
                else:
                    SharkContext._active_spark_context = instance

    # Initialize SharkContext in function to allow subclass specific initialization
    def _initialize_context(self, jconf):
        sharkContext = self._jvm.JavaSharkContext(jconf)
        return self._jvm.SharkEnv.initWithJavaSharkContext(sharkContext)

    def sql2rdd(self, query):
        jrdd = self._jvm.PythonTableRDD.sql2rdd(self._jsc, query)
        return RDD(jrdd, self, PickleSerializer())

    def sql(self, query):
        return self._jsc.sql(query)

    def runSql(self, query, maxRows=None):
        if maxRows:
            return self._jsc.runSql(query, maxRows)
        else:
            return self._jsc.runSql(query)

    def sql2console(self, query):
        return self._jsc.sql2console(query)
