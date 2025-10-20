#!/bin/bash
source /ndb_openjdk_version.sh

set -eux

echo "Setting java to $NDB_CONNECTOR_JAVA_ALTERNATIVE_LATEST_CENTOS"
cd "$(dirname "$0")/.."
TRINO_VERSION=$(cat version.txt)
DIST_TABULAR=${PWD}/../../../dist/tabular  # created by `scons dist/tabular`

rm -rf $1 && mkdir -p $1 && cd $1  # Create trino test directory (should be under 'data/' to be exported to Crater)
DATA_DIR=$PWD

echo "Configuring Trino $TRINO_VERSION at $DATA_DIR"

# for debugging purposes (e.g. no JDK found / wrong JDK is used)
alternatives --list
ls -l /usr/lib/jvm/

sudo update-alternatives --install /usr/bin/java java /usr/lib/jvm/jre-22-openjdk/bin/java 5
alternatives --list
sudo update-alternatives --set java /usr/lib/jvm/jre-22-openjdk/bin/java

mkdir bin
ln -s /opt/trino/bin/* bin/

ln -s /opt/trino/lib lib
ln -s /opt/trino/cli cli

mkdir plugin
ln -s /opt/trino/plugin/* plugin/

pushd plugin
unzip ${DIST_TABULAR}/trino-vast-${TRINO_VERSION}.zip
mv trino-vast-${TRINO_VERSION} vast
popd

NODE_IP=$2

mkdir etc
echo """
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
""" > etc/config.properties
echo """
node.environment=testing
node.internal-address=${NODE_IP}
node.data-dir=${DATA_DIR}
""" > etc/node.properties
echo """
io.trino=INFO
io.trino.execution.executor.TaskExecutor=DEBUG
com.vastdata=DEBUG
""" > etc/log.properties
echo """
-server
-XX:InitialRAMPercentage=40
-XX:MaxRAMPercentage=50
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-Dfile.encoding=UTF-8
# Allow loading dynamic agent used by JOL
-XX:+EnableDynamicAgentLoading
# Disable Preventive GC for performance reasons (JDK-8293861)
-Xlog:gc*:file=/tmp/gc.log:time,uptime,level,tags:filecount=3,filesize=1000m
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
""" > etc/jvm.config

mkdir etc/catalog
echo "connector.name=jmx" > etc/catalog/jmx.properties
echo "connector.name=memory" > etc/catalog/memory.properties
echo "connector.name=tpcds" > etc/catalog/tpcds.properties
echo "connector.name=tpch" > etc/catalog/tpch.properties
echo "
connector.name=vast
endpoint=${S3_ENDPOINT:-http://localhost:9090}
region=us-east-1
access_key_id=${S3_ACCESS_KEY:-}
secret_access_key=${S3_SECRET_KEY:-}
num_of_splits=10
import_chunk_limit=3
vast.http-client.idle-timeout=10s
" > etc/catalog/vast.properties

echo "Trino $TRINO_VERSION is configured at $PWD"
