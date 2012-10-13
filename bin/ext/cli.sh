#!/bin/sh

THISSERVICE=cli
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

cli() {
  echo "Starting the Shark Command Line Client"
  exec $FWDIR/run shark.SharkCliDriver "$@"
}

cli_help() {
 echo "usage  ./shark --service cli"
}
