#!/bin/bash

# Initially, set the reset start and end dates to the current date
# This will not do anything, as the start date is the last date for
# which we keep the data in the DB
RESET_START_DATE=$(date +'%Y-%m-%d')
RESET_END_DATE=$(date +'%Y-%m-%d')
RESET_DB_NAME=stx_test

# Parse the input arguments:
#  -s for start date
#  -e for end date
#  -d for the database name
while getopts s:e:d: flag
do
    case "${flag}" in
        s) RESET_START_DATE=${OPTARG};;
        e) RESET_END_DATE=${OPTARG};;
        d) RESET_DB_NAME=${OPTARG};;
    esac
done

echo -e "Resetting analysis in DB ${RESET_DB_NAME} after ${RESET_START_DATE} until ${RESET_END_DATE}"

echo -e "${RESET_DB_NAME}: remove jl_setups after ${RESET_START_DATE} until ${RESET_END_DATE}"
psql ${RESET_DB_NAME} -c "DELETE FROM jl_setups WHERE dt>'${RESET_START_DATE}' AND dt<='${RESET_END_DATE}'"

echo -e "${RESET_DB_NAME}: set setup_dates after ${RESET_START_DATE} until ${RESET_END_DATE} to ${RESET_START_DATE}"
psql ${RESET_DB_NAME} -c "UPDATE setup_dates SET dt='${RESET_START_DATE}' WHERE dt>'${RESET_START_DATE}' AND dt<='${RESET_END_DATE}'"

echo -e "${RESET_DB_NAME}: remove setup_scores after ${RESET_START_DATE} until ${RESET_END_DATE}"
psql ${RESET_DB_NAME} -c "DELETE FROM setup_scores WHERE dt>'${RESET_START_DATE}' AND dt<='${RESET_END_DATE}'"

echo -e "${RESET_DB_NAME}: remove setups after ${RESET_START_DATE} until ${RESET_END_DATE}"
psql ${RESET_DB_NAME} -c "DELETE FROM setups WHERE dt>'${RESET_START_DATE}' AND dt<='${RESET_END_DATE}'"
