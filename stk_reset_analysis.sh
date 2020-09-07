#!/bin/bash

# Initially, set the reset start and end dates to the current date
# This will not do anything, as the start date is the last date for
# which we keep the data in the DB
RESET_START_DATE=$(date +'%Y-%m-%d')
RESET_END_DATE=$(date +'%Y-%m-%d')
RESET_DB_NAME=stx_test
STK_NAME=

# Parse the input arguments:
#  -s for start date
#  -e for end date
#  -d for the database name
while getopts s:e:d:n: flag
do
    case "${flag}" in
        s) RESET_START_DATE=${OPTARG};;
        e) RESET_END_DATE=${OPTARG};;
        d) RESET_DB_NAME=${OPTARG};;
        n) STK_NAME=${OPTARG};;
    esac
done

if [ -z "$STK_NAME" ]
then
      echo "\$STK_NAME is empty"
      exit -1
fi

echo -e "Resetting analysis in DB ${RESET_DB_NAME} for stock ${STK_NAME} after ${RESET_START_DATE} until ${RESET_END_DATE}"

echo -e "${RESET_DB_NAME}: remove jl_setups for stock ${STK_NAME} after ${RESET_START_DATE} until ${RESET_END_DATE}"
psql ${RESET_DB_NAME} -c "DELETE FROM jl_setups WHERE stk='${STK_NAME}' AND dt>'${RESET_START_DATE}' AND dt<='${RESET_END_DATE}'"

echo -e "${RESET_DB_NAME}: set setup_dates for stock ${STK_NAME} after ${RESET_START_DATE} until ${RESET_END_DATE} to ${RESET_START_DATE}"
psql ${RESET_DB_NAME} -c "UPDATE setup_dates SET dt='${RESET_START_DATE}' WHERE stk='${STK_NAME}' AND dt>'${RESET_START_DATE}' AND dt<='${RESET_END_DATE}'"

echo -e "${RESET_DB_NAME}: remove setup_scores for stock ${STK_NAME} after ${RESET_START_DATE} until ${RESET_END_DATE}"
psql ${RESET_DB_NAME} -c "DELETE FROM setup_scores WHERE stk='${STK_NAME}' AND dt>'${RESET_START_DATE}' AND dt<='${RESET_END_DATE}'"
