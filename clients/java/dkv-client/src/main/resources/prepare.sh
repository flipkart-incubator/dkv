#!/bin/bash

PROTO_DIR=src/main/resources/nexus-proto
rm -f $PROTO_DIR
ln -s `go list -f '{{ .Dir }}' -m github.com/flipkart-incubator/nexus` $PROTO_DIR
