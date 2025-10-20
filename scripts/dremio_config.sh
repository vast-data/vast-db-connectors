#!/bin/bash

DIST_TABULAR=/workdir/dist/tabular/dremio  # created by `scons dist/tabular`
if [[ -d "$DIST_TABULAR" ]]; then
    echo "Copying contents of $DIST_TABULAR to /opt/dremio/jars/3rdparty/"
    rm -rf /opt/dremio/jars/3rdparty/dremio-vast-data-connector*
    cp -v "$DIST_TABULAR"/dremio-vast-data-connector* /opt/dremio/jars/3rdparty/
else
    echo "Skipping copy of $DIST_TABULAR contents to /opt/dremio/jars/3rdparty/. CWD=${PWD}"
fi

[[ "x$1" = "x" ]] && echo "Log path was not provided" >&2 && exit 1
log_path="$1"

sudo mkdir -p /opt/dremio/data
sudo mkdir -p "$log_path"
sudo chown -R runner:runner /opt/dremio
sudo chown -R runner:runner "$log_path"
sed "s|VAST_DREMIO_LOG_DIR_VAR|$log_path|g" "$(dirname "$(readlink -e "$0")")"/dremio_env.template | sudo tee /opt/dremio/conf/dremio-env
sed -i "s|VAST_DREMIO_JAVA_EXTRA_OPTS_VAR|-Ddremio.log.level=debug|g" /opt/dremio/conf/dremio-env
sudo sed -i "s|dremio.log.level:-info|dremio.log.level:-debug|g" /opt/dremio/conf/logback.xml
