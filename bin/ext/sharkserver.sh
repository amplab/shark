#!/bin/sh
THISSERVICE=sharkserver
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

# Use Java to launch Shark otherwise the unit tests cannot properly kill
# the server process.
export SHARK_LAUNCH_WITH_JAVA=1

sharkserver() {
  echo "Starting the Shark Server"
  exec $FWDIR/run shark.SharkServer "$@"
}

sharkserver_help() {
 echo "usage SHARK_PORT=xxxx ./shark --service sharkserver"
 echo "SHARK_PORT : Specify the server port"
}
