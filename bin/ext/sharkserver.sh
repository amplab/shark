#!/bin/sh
THISSERVICE=sharkserver
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

sharkserver() {
  echo "Starting the Shark Server"
  exec $FWDIR/run shark.SharkServer "$@"
}

sharkserver_help() {
 echo "usage SHARK_PORT=xxxx ./shark --service sharkserver"
 echo "SHARK_PORT : Specify the server port"
}
