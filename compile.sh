#!/bin/bash

export SRC_FILE=$1
export EXE_FILE="${SRC_FILE/\.c/\.exe}"
gcc -o ${EXE_FILE} ${SRC_FILE} -I/usr/include/postgresql -lpq -std=c99
