FROM centos:7

ENV LD_LIBRARY_PATH=/usr/local/lib

ENV NAEMON_DIR /tmp/naemon_repo
ARG NAEMON_BRANCH=origin/master

ENV LIVESTATUS_DIR /tmp/livestatus_repo
ARG LIVESTATUS_BRANCH=origin/master

ENV ZOOKEEPER_DIR=/usr

# Building tools
RUN yum install --assumeyes \
    ant \
    autoconf \
    automake \
    check-devel \
    cppunit-devel \
    gcc \
    gcc-c++ \
    gdb \
    git \
    glib2-devel \
    gperf \
    help2man \
    libdbi \
    libdbi-dbd-mysql \
    libdbi-devel \
    libdbi-drivers \
    libtool \
    make \
    mariadb \
    mariadb-devel \
    mariadb-libs \
    mariadb-server \
    monitor-livestatus \
    ruby-devel \
    rubygems \
    gettext-devel \
    flex \
    vim \
    && yum clean all

RUN yum upgrade --assumeyes && yum clean all

RUN git clone https://github.com/naemon/naemon-core.git $NAEMON_DIR
WORKDIR $NAEMON_DIR
RUN git fetch -a && git checkout $NAEMON_BRANCH
RUN autoreconf -if  && ./configure --prefix=/usr --libdir=/lib64 && make -j install

RUN git clone https://github.com/naemon/naemon-livestatus.git $LIVESTATUS_DIR
WORKDIR $LIVESTATUS_DIR
RUN git fetch -a && git checkout $LIVESTATUS_BRANCH
RUN autoreconf -if  && ./configure --prefix=/usr --libdir=/lib64 && make -j install

# ZooKeeper install
RUN curl http://apache.mirrors.spacedump.net/zookeeper/stable/zookeeper-3.4.10.tar.gz --output /tmp/zookeeper.tar.gz && tar -xf /tmp/zookeeper.tar.gz --directory $ZOOKEEPER_DIR
WORKDIR /usr/zookeeper-3.4.10
COPY zoo.cfg /usr/zookeeper-3.4.10/conf/zoo.cfg
RUN ant compile_jute
WORKDIR /usr/zookeeper-3.4.10/src/c
RUN autoreconf -if && ./configure && make install

CMD /usr/zookeeper-3.4.10/bin/zkServer.sh start && /bin/bash
