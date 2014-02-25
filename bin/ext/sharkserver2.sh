#!/bin/sh

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
 echo "usage HIVE_SERVER2_THRIFT_PORT=xxxx ./shark --service sharkserver2"
 echo "HIVE_SERVER2_THRIFT_PORT : Specify the server port"
}
