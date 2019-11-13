#!/bin/bash

dkvSvcHost="localhost"
dkvSvcPort=8080
totalNumKeys=1000000
hotKeysPercent=5
numHotKeys=$(( totalNumKeys * hotKeysPercent / 100 ))
parallelism=50
batchSizes=( 5 10 25 )
valueSizes=( 10 256 1024 4096 8192 16384 )
protoDir="./pkg/serverpb"

for valueSize in "${valueSizes[@]}"
do
  ./bin/dkvbench -name Insert -protoDir $protoDir -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize
  ./bin/dkvbench -name Update -protoDir $protoDir -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize
  ./bin/dkvbench -name Get -protoDir $protoDir -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize
  for batchSize in "${batchSizes[@]}"
  do
    ./bin/dkvbench -name GetAll -protoDir $protoDir -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize -batchSize $batchSize
  done
done
