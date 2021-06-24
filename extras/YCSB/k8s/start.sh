#!/bin/bash

set -e

function load_data
{
    if [[ ! -e /.loaded_data ]]; then
        /opt/ycsb-*/bin/ycsb.sh load "${DBTYPE}" -s -P "${WORKLOAD}" "${DBARGS}"
    fi
    return
}

# exit message
# trap 'echo "ycsb-run has finished"' EXIT

JVM_ARGUMENTS="${JVM_ARGS:-""}"
THREADS="${NUM_THREADS:-"1"}"


# make sure all the params are set and go.
if [[ -z ${DBTYPE} || -z ${WORKLOAD} || -z ${DBARGS} ]]; then
    echo "Missing params! Exiting"
    exit 1
else
    echo "Running WORKLOAD"
    cat "${WORKLOAD}"

    if [[ ! -z "${ACTION}" ]]; then
        eval ./bin/ycsb "${ACTION}" "${DBTYPE}" -s -P "${WORKLOAD}" "${DBARGS}" -jvm-args "${JVM_ARGUMENTS}" -threads "${THREADS}"
    else
        load_data
        eval ./bin/ycsb run "${DBTYPE}" -s -P "${WORKLOAD}" "${DBARGS}" -jvm-args "${JVM_ARGUMENTS}" -threads "${THREADS}"
    fi

    sleep infinity
fi
