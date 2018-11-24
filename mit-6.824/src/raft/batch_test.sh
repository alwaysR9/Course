#!/usr/bin/env bash

rm -rf backup
mkdir backup

for i in `seq 1 1`;
do
    log=backup/zfz_$i
    go test -run 2C &> ${log}
    n_fail=$(cat ${log} |grep FAIL |wc -l)
    if [[ ${n_fail} -gt 0 ]]; then
        echo "Fail  ${log}"
    else
        echo "Pass  ${log}"
    fi
done
