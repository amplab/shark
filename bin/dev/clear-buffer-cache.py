#!/usr/bin/python

# Clear OS buffer cache for mesos clusters on EC2.

import os
import thread
import time

machinesFile = "/root/ephemeral-hdfs/conf/slaves"
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

