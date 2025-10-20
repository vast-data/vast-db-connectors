# This Dockerfile prepares an image containing Trino for Release

ARG BASE_IMAGE
FROM $BASE_IMAGE

ARG NDB_CONNECTOR_OPENJDK_FOR_UBUNTU
ARG NDB_CONNECTOR_JAVA_HOME_FOR_UBUNTU

# So the installation won't get stuck on interactive TZ input
# https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# this is required to use our local repo:
RUN apt-get update && apt-get install ca-certificates -y

# add additional vast-repo
RUN rm -rf /etc/apt/sources.list.d/* /etc/apt/sources.list && echo 'deb [trusted=yes] https://artifactory.vastdata.com/artifactory/files/tabular_2024-01-30/ vast main' > /etc/apt/sources.list.d/vast-repo.list

RUN apt-get update && apt-get install -y python2 python3 wget python-is-python3

RUN wget -q https://artifactory.vastdata.com/artifactory/files/java/22/OpenJDK22U-jdk_x64_linux_hotspot_22.0.2_9.tar.gz \
  && tar xzf OpenJDK22U-jdk_x64_linux_hotspot_22.0.2_9.tar.gz \
  && mkdir -p /usr/lib/jvm/jre-22-openjdk \
  && rm -rf OpenJDK22U-jdk_x64_linux_hotspot_22.0.2_9.tar.gz \
  && cp -r jdk-22.0.2+9/* /usr/lib/jvm/jre-22-openjdk/



ENV TRINO_VERSION=462
ENV ARCH=x86_64
ENV TRINO_NAME=${TRINO_VERSION}
ENV TRINO_HOME=/usr/local/trino

ENV JAVA_HOME=/usr/lib/jvm/jre-22-openjdk
RUN echo 'JAVA_HOME path: ${JAVA_HOME}'
ENV PATH $JAVA_HOME/bin:$PATH

WORKDIR /usr/local
RUN wget -q https://artifactory.vastdata.com/artifactory/maven-remote-cache/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz \
 && tar xzf trino-server-${TRINO_NAME}.tar.gz -C /usr/local/ \
 && ln -s /usr/local/trino-server-${TRINO_NAME} $TRINO_HOME

WORKDIR /usr/local/bin
RUN wget -q https://artifactory.vastdata.com/artifactory/maven-remote-cache/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar \
  && chmod +x trino-cli-${TRINO_NAME}-executable.jar \
  && mv trino-cli-${TRINO_NAME}-executable.jar trino-cli

RUN mkdir -p $TRINO_HOME/data
VOLUME ["$TRINO_HOME/data"]
