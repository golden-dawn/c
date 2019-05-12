#!/bin/bash

export SRC_FILE=$1
export EXE_FILE="${SRC_FILE/\.c/\.exe}"
gcc -DDEBUG -g -o ${EXE_FILE} ${SRC_FILE} -I. -I/usr/include/postgresql -lpq -lm