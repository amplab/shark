#!/bin/bash

set -e -u -o pipefail

DEFAULT_HIVE_GIT_URL=git@github.com:clearstorydata/hive.git

print_help() {
  cat >&2 <<EOT
Build Hive for Shark testing
Usage: ${0##*/} <options>
Options:
  --env_file <file_name>
    Create a file with lines in key=value format at this location with the following keys:

      hive_version   A custom Hive version built for Shark testing
      hive_dev_home  Hive source tree root

  --hive_branch <branch>
    Use this Hive git branch or commit

  --hive_git_url <git_url>
    Clone Hive from this location, ${DEFAULT_HIVE_GIT_URL} by default

  --nexus_url <url>
    Nexus repository server URL to use

  --no-clean
    Do not clean before building

EOT
  exit 1
}

original_dir=$PWD

if [ $# -eq 0 ]; then
  print_help
fi

shark_dir=$(cd $(dirname $0)/../..; pwd)
if [ ! -d "$shark_dir" ]; then
  echo "Could not determine Shark source tree root from script location $0" >&2
  exit 1
fi

if [ ! -f "${shark_dir}/pom.xml" ] || \
   ! grep -q "<name>shark</name>" "${shark_dir}/pom.xml"; then
  echo "${shark_dir}/pom.xml either does not exist or does not refer to the Shark project" >&2
  exit 1
fi

# Parse command-line options
nexus_url=""
env_file=""
hive_branch=""
clean_opt="clean"
hive_git_url=${DEFAULT_HIVE_GIT_URL}
while [ $# -gt 0 ]; do
  case "$1" in 
    --env_file) env_file=$2; shift ;;
    --hive_branch) hive_branch=$2; shift ;;
    --hive_git_url) hive_git_url=$2; shift ;;
    --nexus_url) nexus_url=$2; shift ;;
    --no-clean) clean_opt="" ;;
    -h|--help) print_help ;;
    *)
      echo "Invalid option: $1" >&2
      exit 1
  esac
  shift
done

if [ -n "${env_file}" ]; then
  # Verify this location is writable
  set -x
  touch "${env_file}"
  set +x
fi

if [ -z "${hive_branch}" ]; then
  echo "--hive_branch is not specified" >&2
  exit 1
fi

cd "${shark_dir}/.."

hive_dir_basename=hive_for_shark
hive_dir="$PWD/${hive_dir_basename}"
echo "Using Hive directory $hive_dir"

if [ ! -d "${hive_dir}" ]; then
  set -x
  git clone "${hive_git_url}" "${hive_dir_basename}"
  set +x
fi

set -x
cd "${hive_dir}"
# Fetch with this refspec to be able to test pull requests
git fetch origin
git fetch origin +refs/pull/*:refs/remotes/origin/pr/*
git checkout "${hive_branch}"
set +x

echo "Recent commits in Hive:"
git log -n 10 

hive_version_base=$( awk -F= '/^version=/ {print $NF}' <build.properties )
if [ -z "${hive_version_base}" ]; then
  echo "Failed to determine Hive version configured in build.properties" >&2
  exit 1
fi

hive_version_extension=$( c=$(git rev-parse HEAD); echo ${c:0:10} )
if [ -z "${hive_version_extension}" ]; then
  echo "Failed to get a git sha1 prefix of HEAD in $PWD" >&2
  exit 1
fi
hive_version="${hive_version_base}_${hive_version_extension}"

nexus_url_opt=""
if [ -n "${nexus_url}" ]; then
  nexus_url_opt="-Dnexus.url=${nexus_url}"
fi

local_maven_repo_dir="$HOME/.m2/repository"

set -x
mkdir -p "${local_maven_repo_dir}"
ant -Dversion="${hive_version}" \
    -Dmvn.publish.repo=local \
    -Dskip.javadoc=true \
    -Dmvn.publish.repoUrl="file://${local_maven_repo_dir}" \
    "${nexus_url_opt}" \
    "${clean_opt}" package hive-exec-test-jar maven-build maven-publish
set +x

if [ -n "$env_file" ]; then
  cd "${original_dir}"
  echo "hive_version=${hive_version}" >"${env_file}"
  echo "hive_dev_home=${hive_dir}" >>"${env_file}"
  echo
  echo "Created an environment file at ${env_file}:"
  echo
  cat "${env_file}"
  echo
fi

