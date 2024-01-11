# Use the official Python image as the base image
FROM ubuntu:22.04 as linux-base

# Disable interactive installation
ARG DEBIAN_FRONTEND=noninteractive

ARG TRINO_VERSION=435

# Install tools required by the OS | support for development
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      software-properties-common \
      sudo \
      curl \
      iputils-ping \
      vim \
      unzip \
      rsync \
      build-essential \
      dos2unix \
      nano \
      net-tools \
      python3 \
    python3-pip \
    openssh-client \
    && apt-get clean 
    
RUN echo "INSTALING wget & net-tools🛜"
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      wget \
      net-tools \
    && apt-get clean 

# Package for support setup trino
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      uuid-runtime \
    && apt-get clean 

RUN ln -s /usr/bin/python3 /usr/bin/python

# Install jinja for manage config
RUN pip install jinja2
    
# Print a message during the build process
RUN echo "INSTALING JAVA version 17!"
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      openjdk-17-jdk \
      maven \
  && apt-get clean 
    # && rm -rf /var/lib/apt/lists/*

# set java home
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

ENV TRINO_HOME=/opt/trino 
RUN mkdir -p $TRINO_HOME
RUN mkdir -p ${TRINO_HOME}/etc/catalog

# Add trino-cli
WORKDIR /usr/local/bin
RUN curl -L https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar -o trino-cli-${TRINO_VERSION}-executable.jar 
RUN chmod +x trino-cli-${TRINO_VERSION}-executable.jar
RUN mv trino-cli-${TRINO_VERSION}-executable.jar trino-cli

# Trino Server 
WORKDIR $TRINO_HOME 
RUN curl -L https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz -o trino-server-${TRINO_VERSION}.tar.gz \
 && tar xvzf trino-server-${TRINO_VERSION}.tar.gz --directory ${TRINO_HOME} --strip-components 1 \
 && rm -rf trino-server-${TRINO_VERSION}.tar.gz

ENV TRINO_HOME=/opt/trino/
RUN mkdir -p $TRINO_HOME/data

ENV PATH=$PATH:$TRINO_HOME/bin

# How to add config
ADD scripts ${TRINO_HOME}/scripts

# Create data dir
RUN mkdir -p $TRINO_HOME/data
VOLUME ["$TRINO_HOME/data"]

# docker build -t kevinity310/trino-base:435 .
# docker run --rm -it kevinity310/trino-base /bin/bash

# ln -s /usr/bin/python3 /usr/bin/python
# launcher run
# launcher start


