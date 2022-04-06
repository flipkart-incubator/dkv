ARG BASE=debian:stretch
FROM $BASE
 
LABEL maintainer="DKV Developers (dkv-dev@googlegroups.com)"
 
RUN apt-get update && \
    # Install basic utilities
    apt-get install --yes --allow-unauthenticated adduser vim sudo git curl unzip build-essential \
    # Install Compression libs
    zlib1g-dev libbz2-dev libsnappy-dev && \  
    # Cleanup
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install ZStandard lib
RUN curl -fsSL https://github.com/facebook/zstd/releases/download/v1.4.4/zstd-1.4.4.tar.gz | tar xz \
    && cd zstd-1.4.4 && make install

# Install RocksDB v6.5.3
RUN curl -fsSL https://github.com/facebook/rocksdb/archive/v6.22.1.tar.gz | tar xz \
    && cd rocksdb-6.22.1 && make install

# Install GoLang
RUN curl -fsSL https://dl.google.com/go/go1.16.6.linux-amd64.tar.gz | tar xz \
    && chown -R root:root ./go && mv ./go /usr/local
ENV PATH="/usr/local/go/bin:${PATH}"

# Install Protobuf
RUN curl -fsSL -O https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip \
    && unzip protoc-3.15.8-linux-x86_64.zip -d protoc && chown -R root:root ./protoc && mv ./protoc /usr/local \
    && rm protoc-3.15.8-linux-x86_64.zip
ENV PATH="/usr/local/protoc/bin:${PATH}"

# Install DKV (Skipped for CI Pipelines)
ARG CI
RUN if [ -z "$CI" ] ; then git clone --depth=1 https://github.com/flipkart-incubator/dkv.git \
    && cd dkv && GOOS=linux GOARCH=amd64 make build \
    && mv ./bin /usr/local/dkv && chown -R root:root /usr/local/dkv; fi

ENV PATH="/usr/local/dkv:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

