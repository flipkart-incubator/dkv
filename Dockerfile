ARG BASE=debian:stretch

FROM $BASE
 
# Your team alias or your e-mail id
MAINTAINER DKV Developers (dkv-dev@googlegroups.com)
 
# Install basic utilities
RUN apt-get update && apt-get install --yes --allow-unauthenticated adduser vim sudo git curl unzip build-essential
 
# Install Compression libs
RUN apt-get update && apt-get install --yes --allow-unauthenticated zlib1g-dev libbz2-dev libsnappy-dev

# Install ZStandard lib
RUN curl -fsSL https://github.com/facebook/zstd/releases/download/v1.4.4/zstd-1.4.4.tar.gz | tar xz \
    && cd zstd-1.4.4 && make install

# Install RocksDB v6.5.3
RUN curl -fsSL https://github.com/facebook/rocksdb/archive/v6.5.3.tar.gz | tar xz \
    && cd rocksdb-6.5.3 && make install

# Install GoLang
RUN curl -fsSL https://dl.google.com/go/go1.13.4.linux-amd64.tar.gz | tar xz \
    && chown -R root:root ./go && mv ./go /usr/local
ENV PATH="/usr/local/go/bin:${PATH}"

# Install Protobuf
RUN curl -fsSL -O https://github.com/protocolbuffers/protobuf/releases/download/v3.5.1/protoc-3.5.1-linux-x86_64.zip \
    && unzip protoc-3.5.1-linux-x86_64.zip -d protoc && chown -R root:root ./protoc && mv ./protoc /usr/local \
    && rm protoc-3.5.1-linux-x86_64.zip
ENV PATH="/usr/local/protoc/bin:${PATH}"

# Install GoRocksDB
RUN CGO_CFLAGS="-I/usr/local/include" CGO_LDFLAGS="-lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -lzstd" go get github.com/tecbot/gorocksdb

# Install DKV
RUN git clone https://github.com/flipkart-incubator/dkv.git \
    && cd dkv && GOOS=linux GOARCH=amd64 make build \
    && mv ./bin /usr/local/dkv && chown -R root:root /usr/local/dkv
ENV PATH="/usr/local/dkv:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

# Cleanup
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
 
