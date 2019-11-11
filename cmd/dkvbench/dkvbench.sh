#!/bin/bash

dkvSvcHost="localhost"
dkvSvcPort=8080
totalNumKeys=1000000
hotKeysPercent=5
numHotKeys=$(( totalNumKeys * hotKeysPercent / 100 ))
parallelism=50
batchSizes=( 5 10 25 )
valueSizes=( 10 256 1024 4096 8192 16384 )

for valueSize in "${valueSizes[@]}"
do
  ./bin/dkvbench -name Insert -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize
  ./bin/dkvbench -name Update -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize
  ./bin/dkvbench -name Get -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize
  for batchSize in "${batchSizes[@]}"
  do
    ./bin/dkvbench -name GetAll -dkvSvcHost $dkvSvcHost -dkvSvcPort $dkvSvcPort -parallelism $parallelism -totalNumKeys $totalNumKeys -numHotKeys $numHotKeys -valueSizeInBytes $valueSize -batchSize $batchSize
  done
done
