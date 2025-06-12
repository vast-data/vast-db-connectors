#!/bin/bash
set -ex

cd "$(dirname "$0")"/..  # change to repo root

WORKDIR=$PWD/plugin/ndb-common/target/src/main/java

flatc --java -o $WORKDIR flatbuffers/flatbuf/*.fbs
flatc --java -o $WORKDIR $(find flatbuffers/arrow/flatbuf/experimental/computeir -name '*.fbs')
