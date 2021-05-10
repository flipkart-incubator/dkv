#!/bin/sh

DKV_EXEC="nohup ./bin/dkvsrv"

$DKV_EXEC -dbRole master -dbListenAddr 127.0.0.1:7080 -dbFolder /tmp/dkvsrv/s0 > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:7091 -replMasterAddr 127.0.0.1:7080 -replPollInterval 100ms > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:7092 -replMasterAddr 127.0.0.1:7080 -replPollInterval 100ms > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:7093 -replMasterAddr 127.0.0.1:7080 -replPollInterval 100ms > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:7094 -replMasterAddr 127.0.0.1:7080 -replPollInterval 100ms > /dev/null 2>&1 &

$DKV_EXEC -dbRole master -dbListenAddr 127.0.0.1:8080 -dbFolder /tmp/dkvsrv/s1 > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:8091 -replMasterAddr 127.0.0.1:8080 -replPollInterval 100ms > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:8092 -replMasterAddr 127.0.0.1:8080 -replPollInterval 100ms > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:8093 -replMasterAddr 127.0.0.1:8080 -replPollInterval 100ms > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:8094 -replMasterAddr 127.0.0.1:8080 -replPollInterval 100ms > /dev/null 2>&1 &

$DKV_EXEC -dbRole master -dbListenAddr 127.0.0.1:9080 -dbFolder /tmp/dkvsrv/s2 > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:9091 -replMasterAddr 127.0.0.1:9080 -replPollInterval 100ms > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:9092 -replMasterAddr 127.0.0.1:9080 -replPollInterval 100ms > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:9093 -replMasterAddr 127.0.0.1:9080 -replPollInterval 100ms > /dev/null 2>&1 &
$DKV_EXEC -dbDiskless -dbEngine badger -dbRole slave -dbListenAddr 127.0.0.1:9094 -replMasterAddr 127.0.0.1:9080 -replPollInterval 100ms > /dev/null 2>&1 &
