#!/bin/sh
bin="`dirname $0`"
FWDIR="`dirname $bin`"

THISSERVICE=cli
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

cli() {
  echo "Starting the Shark Command Line Client"
  $FWDIR/run shark.SharkCliDriver "$@"
}

cli_help() {
 echo "usage  ./shark --service cli"
}
