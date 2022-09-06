FROM golang:1.18-alpine
 
LABEL maintainer="DKV Developers (dkv-dev@googlegroups.com)"
 
RUN echo "@testing http://nl.alpinelinux.org/alpine/edge/testing" >>/etc/apk/repositories

# Build tools
RUN apk add --update --no-cache linux-headers git make cmake gcc g++ musl musl-dev binutils autoconf automake libtool pkgconfig check-dev file patch bash curl dpkg nano

# Dependencies
RUN apk add --update --no-cache zlib zlib-dev zlib-static bzip2 bzip2-dev bzip2-static snappy snappy-dev snappy-static lz4 lz4-dev lz4-static gflags-dev zstd zstd-dev zstd-static

# Install RocksDB
RUN git clone --branch v6.22.1 --depth 1 https://github.com/facebook/rocksdb.git && \
    cd rocksdb && \
    # Fix 'install -c' flag
    sed -i 's/install -C/install -c/g' Makefile && \
    make -j4 static_lib && make install-static

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

# Install DKV (Skipped for CI Pipelines)
ARG CI
ARG GIT_SHA=master
RUN if [ -z "$CI" ] ; then git clone https://github.com/flipkart-incubator/dkv.git \
    && cd dkv && git checkout $GIT_SHA && GOOS=linux GOARCH=$(dpkg --print-architecture | awk -F'-' '{print $NF}') make build \
    && mv ./bin /usr/local/dkv && chown -R root:root /usr/local/dkv; fi

ENV PATH="/usr/local/dkv:${PATH}"

CMD ["dkvsrv"]
