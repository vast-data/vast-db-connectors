#!/bin/bash

# Usage: acquire_gluten.sh <target directory> [gluten_bundle_jar_rel_url]

set -eux

if [[ $# -lt 1 ]]; then
   echo "Usage: $0 <target directory> [gluten_bundle_jar_rel_url]"
   exit 1
fi

TARGET="$1"
mkdir -p ${TARGET}

ARTIFACTORY_URL="https://artifactory.vastdata.com/artifactory"

STABLE_GLUTEN_VERSION_IDENTIFIER="708183da-5054ce63"
STABLE_GLUTEN_BUNDLE_JAR_REL_URL="gluten/${STABLE_GLUTEN_VERSION_IDENTIFIER}/gluten-velox-bundle-spark3.3_2.13-rocky_8.6-1.0.0-${STABLE_GLUTEN_VERSION_IDENTIFIER}.jar"
GLUTEN_BUNDLE_JAR_REL_URL="${2:-${STABLE_GLUTEN_BUNDLE_JAR_REL_URL}}"

GLUTEN_BUNDLE_JAR_URL="${ARTIFACTORY_URL}/${GLUTEN_BUNDLE_JAR_REL_URL}"
GLUTEN_BUNDLE_JAR_FILENAME="$(basename "${GLUTEN_BUNDLE_JAR_REL_URL}")"

if [[ -z ${GLUTEN_BUNDLE_JAR_FILENAME} ]]; then
  echo "Invalid GLUTEN_BUNDLE_JAR_REL_URL='${GLUTEN_BUNDLE_JAR_REL_URL}'"
  exit 2
fi

FULL_DEST_PATH="${TARGET}/${GLUTEN_BUNDLE_JAR_FILENAME}"

echo "Downloading ${GLUTEN_BUNDLE_JAR_URL} to ${FULL_DEST_PATH}"
wget -nv --no-check-certificate -O ${FULL_DEST_PATH} ${GLUTEN_BUNDLE_JAR_URL}
echo "Download complete"
