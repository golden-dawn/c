#!/bin/bash

export SRC_FILE=$1
export EXE_FILE="${SRC_FILE/\.c/.exe}"
echo -e "gcc -o ${EXE_FILE} ${SRC_FILE} -O3 -I. -I/usr/include/postgresql -lpq -lm -lcjson -lcurl"
gcc -o ${EXE_FILE} ${SRC_FILE} -O3 -I. -I/usr/include/postgresql -lpq -lm -lcjson -lcurl
