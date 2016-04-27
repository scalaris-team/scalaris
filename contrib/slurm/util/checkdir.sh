#!/bin/bash

WD=$1
if [[ -z $WD ]]; then
    exit 1
fi

if [ "$(ls $WD)" ]; then
    echo "[error] Directory ($WD) on $(hostname -s) is not empty, containing the following files/dirs:"
    ls -l1 $WD
    exit 1
fi

