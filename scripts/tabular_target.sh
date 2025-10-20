#!/usr/bin/env bash
#
# /* Copyright (C) Vast Data Ltd. */
#
CONNECTOR_VERSION="5.3.0"
if [[ "$(uname -i)" != "x86_64" ]] ; then
    echo "Creating dummy artifacts"
    mkdir -p ./plugin/spark3/resources/
    touch ./plugin/spark3/resources/dummy
    mkdir -p ./plugin/spark3/common/target/dependencies/
    touch ./plugin/spark3/common/target/dependencies/dummy
    mkdir -p ./plugin/spark3/spark33/target/dependencies/
    touch ./plugin/spark3/spark33/target/spark-vast-spark33-$CONNECTOR_VERSION.jar
    touch ./plugin/spark3/spark33/target/dependencies/dummy
    mkdir -p ./plugin/spark3/spark34/target/dependencies/
    touch ./plugin/spark3/spark34/target/spark-vast-spark34-$CONNECTOR_VERSION.jar
    touch ./plugin/spark3/spark34/target/dependencies/dummy
    mkdir -p ./plugin/spark3/spark35/target/dependencies/
    touch ./plugin/spark3/spark35/target/spark-vast-spark35-$CONNECTOR_VERSION.jar
    touch ./plugin/spark3/spark35/target/dependencies/dummy
    mkdir -p ./plugin/spark3/spark-agent/target/
    mkdir -p ./plugin/spark3/spark-agent33/target/
    touch ./plugin/spark3/spark-agent/target/spark-vast-spark-agent-$CONNECTOR_VERSION.jar
    touch ./plugin/spark3/spark-agent33/target/spark-vast-spark-agent33-$CONNECTOR_VERSION.jar

    mkdir -p ./plugin/ndb-common/target/dependencies/
    touch ./plugin/ndb-common/target/dependencies/dummy

    mkdir -p ./plugin/trino-vast/target/
    touch ./plugin/trino-vast/target/trino-vast-$CONNECTOR_VERSION.zip
    exit 0
fi
UNITTEST_CONNECTORS=$1

MAVEN_ARGS="-e -U"
if [[ "$UNITTEST_CONNECTORS" != "1" ]]; then
    echo "Skipping Maven tests"
    MAVEN_ARGS="$MAVEN_ARGS -DskipTests"
fi

echo "CLEANING WORKSPACE"
./mvnw --show-version clean || exit 1
MVNW="./mvnw "$MAVEN_ARGS""
echo "SWITCH TO JAVA HOME to 8"  # compilation of common & spark must be with java 8
source ../../../install/ndb_openjdk_version.sh
export JAVA_HOME="$NDB_CONNECTOR_JAVA_HOME_8_CENTOS"
echo "JAVA_HOME=$JAVA_HOME"
echo "BUILDING SPARK"
$MVNW install || exit 1

if [[ -d ~/.m2/repository/ndb/ndb-root ]] ; then
    echo "REPLACING existing pom.lastUpdated with root pom file"
    pom_last_updated=$(echo ~/.m2/repository/ndb/ndb-root/*/ndb-root-*.pom.lastUpdated)
    echo "pom_last_updated=$pom_last_updated"
    rm -rf ~/.m2/repository/ndb/ndb-root/*/ndb-root-*.pom.lastUpdated
    pom=${pom_last_updated%.lastUpdated}
    echo "pom=$pom"
    cp ./pom.xml "$pom"
else
    echo "CREATING root pom file"
    mkdir -p ~/.m2/repository/ndb/ndb-root/$CONNECTOR_VERSION/
    cp ./pom.xml ~/.m2/repository/ndb/ndb-root/$CONNECTOR_VERSION/ndb-root-$CONNECTOR_VERSION.pom
fi

jar -uvf ./plugin/spark3/spark33/target/spark-vast-spark33-$CONNECTOR_VERSION.jar -C ./plugin/spark3/resources/sparkclasses_scala_2_13 org/apache/spark/sql/execution/datasources/v2/ || exit 1

echo "SWITCH TO latest JAVA"
export JAVA_HOME="$NDB_CONNECTOR_JAVA_HOME_LATEST_CENTOS"
echo "JAVA_HOME=$JAVA_HOME"
export _JAVA_OPTIONS="--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED
                     --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
                     --add-exports=jdk.compiler/com.sun.tools.javac.main=ALL-UNNAMED
                     --add-exports=jdk.compiler/com.sun.tools.javac.model=ALL-UNNAMED
                     --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED
                     --add-exports=jdk.compiler/com.sun.tools.javac.processing=ALL-UNNAMED
                     --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED
                     --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED
                     --add-opens=java.base/java.io=ALL-UNNAMED
                     --add-opens=java.base/java.lang=ALL-UNNAMED
                     --add-opens=java.base/java.nio=ALL-UNNAMED
                     --add-opens=jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED
                     --add-opens=jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED
                     --add-opens=java.base/java.nio=ALL-UNNAMED
                     --add-opens=java.base/java.lang=ALL-UNNAMED
                     --add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
echo "BUILDING TRINO"
$MVNW -pl ndb:trino-vast clean package -f ./plugin/trino-vast/pom.xml
