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

DEVDIR="`dirname $0`"
BINDIR="`dirname $DEVDIR`"
FWDIR="`dirname $BINDIR`"

rm -rf $FWDIR/run-tests-from-scratch-workspace
rm -rf $FWDIR/test_warehouses

rm -rf $FWDIR/conf/shark-env.sh

rm -rf $FWDIR/metastore_db
rm -rf $FWDIR/derby.log

rm -rf $FWDIR/project/target $FWDIR/project/project/target

rm -rf $FWDIR/target/resolution-cache
rm -rf $FWDIR/target/streams
rm -rf $FWDIR/target/scala-*/cache
rm -rf $FWDIR/target/scala-*/classes
rm -rf $FWDIR/target/scala-*/test-classes

find $FWDIR -name ".DS_Store" -exec rm {} \;
find $FWDIR -name ".history" -exec rm {} \;

