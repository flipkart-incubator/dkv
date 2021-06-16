#!/bin/sh

DKV_EXEC="nohup ./bin/dkvsrv"

$DKV_EXEC -role master -listen-addr 127.0.0.1:7080 -db-folder /tmp/dkvsrv/s0 > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:7091 -repl-master-addr 127.0.0.1:7080 -repl-poll-interval 100ms > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:7092 -repl-master-addr 127.0.0.1:7080 -repl-poll-interval 100ms > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:7093 -repl-master-addr 127.0.0.1:7080 -repl-poll-interval 100ms > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:7094 -repl-master-addr 127.0.0.1:7080 -repl-poll-interval 100ms > /dev/null 2>&1 &

$DKV_EXEC -role master -listen-addr 127.0.0.1:8080 -db-folder /tmp/dkvsrv/s1 > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:8091 -repl-master-addr 127.0.0.1:8080 -repl-poll-interval 100ms > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:8092 -repl-master-addr 127.0.0.1:8080 -repl-poll-interval 100ms > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:8093 -repl-master-addr 127.0.0.1:8080 -repl-poll-interval 100ms > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:8094 -repl-master-addr 127.0.0.1:8080 -repl-poll-interval 100ms > /dev/null 2>&1 &

$DKV_EXEC -role master -listen-addr 127.0.0.1:9080 -db-folder /tmp/dkvsrv/s2 > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:9091 -repl-master-addr 127.0.0.1:9080 -repl-poll-interval 100ms > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:9092 -repl-master-addr 127.0.0.1:9080 -repl-poll-interval 100ms > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:9093 -repl-master-addr 127.0.0.1:9080 -repl-poll-interval 100ms > /dev/null 2>&1 &
$DKV_EXEC -diskless -db-engine badger -role slave -listen-addr 127.0.0.1:9094 -repl-master-addr 127.0.0.1:9080 -repl-poll-interval 100ms > /dev/null 2>&1 &
