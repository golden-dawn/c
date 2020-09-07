#!/bin/bash
export START_DATE=$1
echo -e "Remove any existing instances of the stx_test database"
psql stx_test -c "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"
dropdb stx_test
# sleep for a little time here, so that we can see whether the database deletion was successful or not
echo -e "After removing the stx_test database"
sleep 20
echo -e "Create the stx_test database"
createdb stx_test
echo -e "Create the tables and indexes in the stx_test database"
./test.sh ./create_tables.exe
echo -e "Start date is: ${START_DATE}"
echo -e "Populate analyses test DB table"
psql stx -c "COPY (SELECT * FROM analyses WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY analyses FROM stdin csv"
echo -e "Populate calendar test DB table"
psql stx -c "COPY (SELECT * FROM calendar) TO stdout WITH csv" | psql stx_test -c "COPY calendar FROM stdin csv"
echo -e "Populate dividends test DB table"
psql stx -c "COPY (SELECT * FROM dividends WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY dividends FROM stdin csv"
echo -e "Populate eods test DB table"
psql stx -c "COPY (SELECT * FROM eods WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY eods FROM stdin csv"
echo -e "Populate excludes test DB table"
psql stx -c "COPY (SELECT * FROM excludes) TO stdout WITH csv" | psql stx_test -c "COPY excludes FROM stdin csv"
echo -e "Populate jl_setups test DB table"
psql stx -c "COPY (SELECT * FROM jl_setups WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY jl_setups FROM stdin csv"
echo -e "Populate leaders test DB table"
psql stx -c "COPY (SELECT * FROM leaders WHERE expiry>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY leaders FROM stdin csv"
echo -e "Populate setup_dates test DB table"
psql stx -c "COPY (SELECT * FROM setup_dates WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY setup_dates FROM stdin csv"
echo -e "Populate setup_scores test DB table"
psql stx -c "COPY (SELECT * FROM setup_scores WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY setup_scores FROM stdin csv"
echo -e "Populate setups test DB table"
psql stx -c "COPY (SELECT * FROM setups WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY setups FROM stdin csv"