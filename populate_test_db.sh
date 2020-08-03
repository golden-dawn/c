#!/bin/bash
export START_DATE=$1
echo -e "Start date is: ${START_DATE}"
psql stx -c "COPY (SELECT * FROM excludes) TO stdout WITH csv" | psql stx_test -c "COPY excludes FROM stdin csv"
psql stx -c "COPY (SELECT * FROM eods WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY eods FROM stdin csv"
psql stx -c "COPY (SELECT * FROM dividends WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY dividends FROM stdin csv"
psql stx -c "COPY (SELECT * FROM leaders WHERE expiry>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY leaders FROM stdin csv"
psql stx -c "COPY (SELECT * FROM setup_scores WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY setup_scores FROM stdin csv"
psql stx -c "COPY (SELECT * FROM setup_dates WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY setup_dates FROM stdin csv"
psql stx -c "COPY (SELECT * FROM setups WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY setups FROM stdin csv"
psql stx -c "COPY (SELECT * FROM jl_setups WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY jl_setups FROM stdin csv"
