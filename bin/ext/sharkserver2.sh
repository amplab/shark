#!/bin/sh

THISSERVICE=sharkserver2
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

# Use Java to launch Shark otherwise the unit tests cannot properly kill
# the server process.
export SHARK_LAUNCH_WITH_JAVA=1

sharkserver2() {
  echo "Starting the Shark Server"
  exec $FWDIR/run shark.SharkServer2 "$@"
}

sharkserver2_help() {
 echo "usage SHARK_PORT=xxxx ./shark --service sharkserver2"
 echo "SHARK_PORT : Specify the server port"
}
