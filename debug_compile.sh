#!/bin/bash

export SRC_FILE=$1
export EXE_FILE="${SRC_FILE/\.c/.exe}"
gcc -O0 -g -o ${EXE_FILE} ${SRC_FILE} -I. -I/usr/include/postgresql -lpq -lm -lcurl -lcjson
