#!/bin/bash
export START_DATE=$1
echo -e "Start date is: ${START_DATE}"
# psql stx -c "COPY (SELECT * FROM excludes) TO stdout WITH csv" | psql stx_test -c "COPY excludes FROM stdin csv"
# psql stx -c "COPY (SELECT * FROM calendar) TO stdout WITH csv" | psql stx_test -c "COPY calendar FROM stdin csv"
psql stx -c "COPY (SELECT * FROM eods WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY eods FROM stdin csv"
psql stx -c "COPY (SELECT * FROM dividends WHERE dt>'${START_DATE}') TO stdout WITH csv" | psql stx_test -c "COPY dividends FROM stdin csv"
