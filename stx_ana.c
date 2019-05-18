#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include "stx_core.h"
#include "stx_ts.h"

void expiry_analysis(char* dt) {
    /** 
     * special case when the date is an option expiry date
     * if the data is NULL, only run for the most recent business day
     * 1. wait until eoddata is downloaded. 
     * 2. calculate liquidity leaders
     * 3. download options for all liquidity leaders
     * 4. calculate option spread leaders
     * 5. populate leaders table
     **/
    /* 1. Get all the stocks as of a given date */
    LOGINFO("started expiry_analysis\n");
    char sql_cmd[128];
    strcpy(sql_cmd, "explain analyze select * from eods");
    PGresult *all_res = db_query(sql_cmd);
    PQclear(all_res);
    LOGINFO("got all records from eods\n");
    sprintf(sql_cmd, "select distinct stk from eods where dt='%s'", dt);
    PGresult *res = db_query(sql_cmd);
    int rows = PQntuples(res);
    if (rows <= 0) {
	LOGWARN("No stocks found for %s, exiting...\n", dt);
	return;
    }
    char all_stx[rows][16];
    for (int ix = 0; ix < rows; ix++)
	strcpy(all_stx[ix], PQgetvalue(res, ix, 0));
    LOGINFO("stored %d stocks in list\n", rows);
    PQclear(res);

    for (int ix = 0; ix < rows; ix++) {
	stx_data_ptr data = load_stk(all_stx[ix]);
	if (ix % 100 == 0)
	    LOGINFO("loaded %4d stocks\n", ix);
    }
    LOGINFO("loaded all %4d stocks\n", rows);
    /* 2. For each stock, get the data */
    /* 3. Set the time series to a specific date (dt) */
    /* 4. Get all the splits up to dt */
    /* 5. Adjust the data accordingly */
    /* 6. Run JL on the adjusted data */
    /* 7. Run the setups */
}


void eod_analysis(char* dt) {
    /** this runs at the end of the trading day.
     * 1. Get prices and options for hte leaders
     * 2. calculate eod setups
     * 3. email the results
     **/
}


void intraday_analysis() {
    /** this runs during the trading day
     * 1. download price data only for option spread leaders
     * 2. determine which EOD setups were triggered today
     * 3. Calculate intraday setups (?)
     * 4. email the results
     **/
}


int main(int argc, char** argv) {
    char* dt = "2019-04-24";
    expiry_analysis(dt);
}
