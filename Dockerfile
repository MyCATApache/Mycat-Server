FROM openjdk:8-jre-slim

ARG mycat_version=1.6

MAINTAINER WilliamWei

ENV TINI_VERSION v0.18.0

ENV MYCAT_VERSION=$mycat_version \
    MYCAT_HOME=/opt/mycat


ENV PATH=${PATH}:${MYCAT_HOME}/bin

RUN apt-get update && \
    apt-get install -y wget procps && \
    wget https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -O /tini && \
    chmod +x /tini && \
    wget http://dl.mycat.io/1.6-RELEASE/Mycat-server-1.6-RELEASE-20161028204710-linux.tar.gz  -O mycat-${MYCAT_VERSION}.tar.gz && \
    mkdir -p ${MYCAT_HOME} && \
    tar -zxvf mycat-${MYCAT_VERSION}.tar.gz && \
    cp -r mycat/* ${MYCAT_HOME} && \
    rm mycat-${MYCAT_VERSION}.tar.gz && \
    rm -r mycat && \
    sed 's/MaxDirectMemorySize=4G/MaxDirectMemorySize=2G/g; s/-Xmx4G/-Xmx2G/g; s/-Xmx1G/-Xmx512M/g;' ${MYCAT_HOME}/conf/wrapper.conf

WORKDIR ${MYCAT_HOME}

VOLUME ${MYCAT_HOME}/conf
VOLUME ${MYCAT_HOME}/logs

EXPOSE 3306

ENTRYPOINT ["/tini"]

CMD ["mycat", "console"]

