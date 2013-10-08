BASE_VERSION:=`grep 'version := ' ../project/SharkBuild.scala | sed -e 's/^[^"]*"//' -e 's/".*$$//'`
BUILD_NUMBER?=0
SCALA_HOME?=/home/y/libexec/miners_scala
FULL_VERSION="$(BASE_VERSION).$(BUILD_NUMBER)"
SBT=env SCALA_HOME=$(SCALA_HOME) ./sbt/sbt

export HIVE_HOME:=/home/y/libexec/miners_hive_devel/build/dist/
export HIVE_DEV_HOME:=/home/y/libexec/miners_hive_devel/

export SPARK_DEV_HOME:=/home/y/libexec/miners_spark_devel/

clean-cache:
	rm -rf ~/.ivy2 ~/.m2 ~/.sbt


sparklocal:
	cd $(SPARK_DEV_HOME) && $(SBT) publish-local

all: build

test: build
	cd .. && $(SBT) test

build: sparklocal
	cd .. && $(SBT) package

install: build
	cd .. && $(SBT) publish-local

#TODO need a good way to see if these values would have changed and update only then
BASE_VERSION: ../project/SharkBuild.scala
	echo $(BASE_VERSION) > BASE_VERSION

RELEASE:
	echo $(FULL_VERSION) > RELEASE

testpkg: RELEASE BASE_VERSION build
	yinst_create --buildtype test *.yicf

package: RELEASE BASE_VERSION build
	yinst_create --buildtype release *.yicf

rhel6_build.state: rhel6_build.yidf  rhel6_build.yml
	rm -f rhel6_build.state rhel6_build.state.tmp rhel6_build.ruleset
	statecc --branch test ./rhel6_build.yml

clean:
	rm -rf *_build.ruleset *.tgz BASE_VERSION RELEASE
	cd .. && $(SBT) clean


#BuildyBlocks targets
BB_RULES=/home/y/share/yahoo_cfg/buildyblocks/Make.rules
include $(BB_RULES)

#COMMIT
COMMIT_BIM=./commit.bim
commit: BB_Provision build install test
commit: BB_BIM_FILE=$(COMMIT_BIM)

commit_clean: clean

#COMPONENT
COMPONENT_BIM=./component.bim
component: BB_Provision build test package
component: BB_BIM_FILE=$(COMPONENT_BIM)
#
component-dist-push:: BB_BIM_FILE=./component.bim
component-dist-push:: component BB_Dist_Push
component-dist-push:: component
	/home/y/bin/dist_install miners_shark-*.tgz -batch -branch quarantine -group shark-devel

component_clean: clean

