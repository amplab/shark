include $(ROOT)/share/yahoo_cfg/Make.defs

# to build a package - run 
# $ yinst inst miners_shark_build
#
# build and make spark available first;
# $ yinst inst miners_spark_build miners_spark
# $ git clone spark
# $ sbt/sbt package publish-local
#
# then make hive available
# $ yinst inst miners_hive
#
# then shark can be built
# $ yinst inst miners_shark_build
# $ HIVE_HOME=/home/y/libexec/miners_hive/build/dist sbt/sbt clean package
# $ cd yinst; gmake package
# OR
# $ SRCZIP=gitzip gmake package-release
# to clean up -
# $ gmake package-clean
#
#
# Set this variable to be able to make link packages in the future
IN_PACKAGE=1

include $(ROOT)/share/yahoo_cfg/Make.rules
# Do not place any rules or targets above this line
