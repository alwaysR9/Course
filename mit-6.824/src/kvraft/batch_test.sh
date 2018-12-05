#!/usr/bin/env bash
source ~/.bashrc

rm -rf log
mkdir log

for i in `seq 1 1`; do
    echo "Begin to test: #$i"
    go test -run 3A &> log/run_$i.log
    
    n_fail=$(cat log/run_$i.log |grep FAIL |wc -l)
    if [[ n_fail -eq 0 ]]; then
        echo "SUCCESS"
    else
        echo "FAIL"
    fi
done