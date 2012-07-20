#!/bin/sh
bin="`dirname $0`"
FWDIR="`dirname $bin`"

THISSERVICE=sharkserver
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

sharkserver() {
  echo "Starting the Shark Server" 
  $FWDIR/run shark.SharkServer  "$@"
}

sharkserver_help() {
 echo "usage SHARK_PORT=xxxx ./shark --service sharkserver"
 echo "SHARK_PORT : Specify the server port"
}
