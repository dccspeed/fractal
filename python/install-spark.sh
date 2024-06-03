#! /usr/bin/env bash

# script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# temporary directory
#WORK_DIR=$(mktemp -d -p "$DIR")
WORK_DIR=$(mkdir -p build)

# check if tmp dir was created
if [[ ! "$WORK_DIR" || ! -d "$WORK_DIR" ]]; then
  echo "Could not create temp dir"
  exit 1
fi

# deletes the temp directory
function cleanup {
  rm -rf "$WORK_DIR"
  echo "Deleted temp working directory $WORK_DIR"
}
trap cleanup EXIT

# install spark
url="https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz"
cd "$WORK_DIR" || exit
wget $url
tar xf spark-3.5.0-bin-hadoop3-scala2.13.tgz
mv spark-3.5.0-bin-hadoop3-scala2.13 spark
pip install spark/python

cd "$DIR" && echo "!# /usr/bin/env bash\npip install ." > build.sh




