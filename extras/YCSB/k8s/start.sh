#!/bin/bash

set -e

function load_data
{
    if [[ ! -e /.loaded_data ]]; then
        /opt/ycsb-*/bin/ycsb.sh load "${DBTYPE}" -s -P "workloads/workload${WORKLETTER}" "${DBARGS}" && touch /.loaded_data
    fi
    return
}

# exit message
trap 'echo "ycsb-run has finished"' EXIT


# make sure all the params are set and go.
if [[ -z ${DBTYPE} || -z ${WORKLETTER} || -z ${DBARGS} ]]; then
  echo "Missing params! Exiting"
  exit 1
else
  if [[ ! -z "${ACTION}" ]]; then
    eval ./bin/ycsb "${ACTION}" "${DBTYPE}" -s -P "workloads/workload${WORKLETTER}" "${DBARGS}"
  else
    load_data
    eval ./bin/ycsb run "${DBTYPE}" -s -P "workloads/workload${WORKLETTER}" "${DBARGS}"
  fi
fi