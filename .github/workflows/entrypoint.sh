#!/bin/bash

echo "Running '$1'"

bash -c "$1"

time=$(date)
echo "::set-output name=time::$time"