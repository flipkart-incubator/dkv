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
dkvBench="./bin/dkvBench"

for valueSize in "${valueSizes[@]}"
do
  $dkvBench -name Insert -protoDir $protoDir -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize
  $dkvBench -name Update -protoDir $protoDir -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize
  $dkvBench -name Get -protoDir $protoDir -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize
  for batchSize in "${batchSizes[@]}"
  do
    $dkvBench -name GetAll -protoDir $protoDir -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize -batchSize $batchSize
  done
done
