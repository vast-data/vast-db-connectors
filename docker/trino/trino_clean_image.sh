#!/bin/bash

export IMAGES_REPOSITORY=dev/orion
BUILD_DIR='src/tabular/trino/docker/trino'
MODULE=trino_clean
VER=A
SALT=1  # change this to force re-build
MODULE_DOCKERFILE=src/tabular/trino/docker/trino/Dockerfile
VERSION_PATCH=1  # because we need to patch the original image with fresh version information

shopt -s globstar


DEPS=(
  $MODULE_DOCKERFILE
  src/tabular/trino/docker/trino
)
# based on cwd = /
source install/ndb_openjdk_version.sh
source scripts/image_making.sh

MODULE_BASE_IMAGE="${DOCKER_REGISTRY}/docker.io/library/ubuntu:jammy-20230301"

DOCKER_BUILD_FLAGS=(
  --build-arg NDB_CONNECTOR_OPENJDK_FOR_UBUNTU="$NDB_CONNECTOR_OPENJDK_LATEST_UBUNTU"
  --build-arg NDB_CONNECTOR_JAVA_HOME_FOR_UBUNTU="$NDB_CONNECTOR_JAVA_HOME_LATEST_UBUNTU"
)

build_it() {
    echo "Nothing to build"
    }


main "$@"
