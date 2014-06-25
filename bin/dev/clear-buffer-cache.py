#!/usr/bin/python

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

# Clear OS buffer cache for mesos clusters on EC2.

import os
import thread
import time
import sys

EC2_MACHINES_FILE = "/root/spark-ec2/slaves"

spark_home = os.getenv("SPARK_HOME")
if spark_home is None:
  machinesFile = EC2_MACHINES_FILE
  if not os.path.exists(machinesFile):
    print "Could not find Spark slaves file.  SPARK_HOME is not set and could not find file at %s" % EC2_MACHINES_FILE
    sys.exit(1)
else:
  machinesFile = spark_home + "/conf/slaves"
  if not os.path.exists(machinesFile):
    print "Could not find Spark slaves file. Based on SPARK_HOME, expected file to be located at %s" % machinesFile
    sys.exit(1)
    
machs = open(machinesFile).readlines()
machs = map(lambda s: s.strip(),machs)
machCount = len(machs)
machID = 0
cmd = "sync; echo 3 > /proc/sys/vm/drop_caches"
done = {}

def dropCachesThread( mach, myID, *args ):
  print "SSH to machine %i" % (myID)
  os.system("ssh %s '%s'" % (mach, cmd))
  done[mach] = "done"

for mach in ( machs ):
  thread.start_new_thread(dropCachesThread, (mach, machID))
  machID = machID + 1
  time.sleep(0.2)

while (len(done.keys()) < machCount):
  print "waiting for %d tasks to finish..." % (machCount - len(done.keys()))
  time.sleep(1)
  
print "Done with %i threads" % (len(done.keys()))

