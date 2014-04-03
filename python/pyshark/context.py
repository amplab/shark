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


from pyspark import SparkContext, RDD, accumulators, SparkFiles, SparkConf
from pyspark.serializers import PickleSerializer, BatchedSerializer


from pyshark.java_gateway import launch_shark_gateway


class SharkContext(SparkContext):
    """
    Main entry point for Shark functionality. A SharkContext represents the
    connection to a Spark cluster and the connection to a hive metastore. It
    can be used to run SQL queries, create RDDs from SQL queries, and perform
    all functionality of the base SharkContext.
    """

    def __init__(self, master=None, appName=None, sparkHome=None, pyFiles=None,
        environment=None, batchSize=1024, serializer=PickleSerializer(), conf=None,
        gateway=None):
        """
        Create a new SharkContext. At least the master and app name should be set,
        either through the named parameters here or through C{conf}. This method
        should be removed in future versions of pyspark, but because of the way
        the SparkContext is decomposed in pyspark 0.9.0, we have to overwrite the
        whole thing.

        @param master: Cluster URL to connect to
               (e.g. mesos://host:port, spark://host:port, local[4]).
        @param appName: A name for your job, to display on the cluster web UI.
        @param sparkHome: Location where Spark is installed on cluster nodes.
        @param pyFiles: Collection of .zip or .py files to send to the cluster
               and add to PYTHONPATH.  These can be paths on the local file
               system or HDFS, HTTP, HTTPS, or FTP URLs.
        @param environment: A dictionary of environment variables to set on
               worker nodes.
        @param batchSize: The number of Python objects represented as a single
               Java object.  Set 1 to disable batching or -1 to use an
               unlimited batch size.
        @param serializer: The serializer for RDDs.
        @param conf: A L{SparkConf} object setting Spark properties.
        @param gateway: Use an existing gateway and JVM, otherwise a new JVM
               will be instatiated. 
        """
        SharkContext._ensure_initialized(self, gateway=gateway)

        self.environment = environment or {}
        self._conf = conf or SparkConf(_jvm=self._jvm)
        self._batchSize = batchSize  # -1 represents an unlimited batch size
        self._unbatched_serializer = serializer
        if batchSize == 1:
            self.serializer = self._unbatched_serializer
        else:
            self.serializer = BatchedSerializer(self._unbatched_serializer,
                                                batchSize)

        # Set any parameters passed directly to us on the conf
        if master:
            self._conf.setMaster(master)
        if appName:
            self._conf.setAppName(appName)
        if sparkHome:
            self._conf.setSparkHome(sparkHome)
        if environment:
            for key, value in environment.iteritems():
                self._conf.setExecutorEnv(key, value)

        # Check that we have at least the required parameters
        if not self._conf.contains("spark.master"):
            raise Exception("A master URL must be set in your configuration")
        if not self._conf.contains("spark.app.name"):
            raise Exception("An application name must be set in your configuration")

        # Read back our properties from the conf in case we loaded some of them from
        # the classpath or an external config file
        self.master = self._conf.get("spark.master")
        self.appName = self._conf.get("spark.app.name")
        self.sparkHome = self._conf.get("spark.home", None)
        for (k, v) in self._conf.getAll():
            if k.startswith("spark.executorEnv."):
                varName = k[len("spark.executorEnv."):]
                self.environment[varName] = v

        # Create the Java SparkContext through Py4J
        self._jsc = self._initialize_context(self._conf._jconf)

        # Create a single Accumulator in Java that we'll send all our updates through;
        # they will be passed back to us through a TCP server
        self._accumulatorServer = accumulators._start_update_server()
        (host, port) = self._accumulatorServer.server_address
        self._javaAccumulator = self._jsc.accumulator(
                self._jvm.java.util.ArrayList(),
                self._jvm.PythonAccumulatorParam(host, port))

        self.pythonExec = os.environ.get("PYSPARK_PYTHON", 'python')

        # Broadcast's __reduce__ method stores Broadcast instances here.
        # This allows other code to determine which Broadcast instances have
        # been pickled, so it can determine which Java broadcast objects to
        # send.
        self._pickled_broadcast_vars = set()

        SparkFiles._sc = self
        root_dir = SparkFiles.getRootDirectory()
        sys.path.append(root_dir)

        # Deploy any code dependencies specified in the constructor
        self._python_includes = list()
        for path in (pyFiles or []):
            self.addPyFile(path)

        # Create a temporary directory inside spark.local.dir:
        local_dir = self._jvm.org.apache.spark.util.Utils.getLocalDir(self._jsc.sc().conf())
        self._temp_dir = \
            self._jvm.org.apache.spark.util.Utils.createTempDir(local_dir).getAbsolutePath()

    @classmethod
    def _ensure_initialized(cls, instance=None, gateway=None):
        with SparkContext._lock:
            if not SparkContext._gateway:
                SparkContext._gateway = gateway or launch_shark_gateway()
                SparkContext._jvm = SparkContext._gateway.jvm
                SparkContext._writeToFile = SparkContext._jvm.PythonRDD.writeToFile
            if instance:
                if SparkContext._active_spark_context and SparkContext._active_spark_context != instance:
                    currentMaster = SparkContext._active_spark_context.master
                    currentAppName = SparkContext._active_spark_context.appName
                    callsite = SparkContext._active_spark_context._callsite
                    # Raise error if there is already a running Spark context
                    raise ValueError("Cannot run multiple SharkContexts at once; existing SharkContext(app=%s, master=%s)" \
                        " created by %s at %s:%s " \
                        % (currentAppName, currentMaster, callsite.function, callsite.file, callsite.linenum))
                else:
                    SparkContext._active_spark_context = instance

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

    def saveAsTable(self, rdd, tableName, columnNames):
        cols = self._jvm.java.util.ArrayList()
        for columnName in columnNames:
            cols.append(columnName)
        # Guarantee we have a PythonRDD that contains lists
        rdd = rdd.map(lambda x: list(x))
        self._jvm.PythonRDDTable.apply(rdd._jrdd.rdd(), tableName, cols)
