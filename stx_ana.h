#ifndef __STX_ANA_H__
#define __STX_ANA_H__

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include "stx_core.h"
#include "stx_ts.h"

#define AVG_DAYS 50
#define MIN_ACT 80000
#define MIN_RCR 15


int ana_calc_spread(PGresult* sql_res, int spot) {
    int itm_calls = 0, otm_calls = 0, itm_puts = 0, otm_puts = 0;
    int avg_spread = 0, bid, ask, strike, num_calls = 0, num_puts = 0;
    char* cp;
    bool call_atm = false, put_atm = false;
    int num = PQntuples(sql_res), num_spreads = 0;
    for(int ix = 0; ix < num; ix++) {
	cp = PQgetvalue(sql_res, ix, 0);
	strike = atoi(PQgetvalue(sql_res, ix, 1));
	if (!strcmp(cp, "c")) {
	    num_calls++;
	    if (strike < spot) itm_calls++;
	    if (strike == spot) { itm_calls++; otm_calls++; call_atm = true; }
	    if (strike > spot) otm_calls++;
	} else {
	    num_puts++;
	    if (strike < spot) otm_puts++;
	    if (strike == spot) { itm_puts++; otm_puts++; put_atm = true; }
	    if (strike > spot) itm_puts++;
	}
    }
    if ((itm_calls < 2) || (otm_calls < 2) || (itm_puts < 2) || (otm_puts < 2))
	return -1;
    for(int ix = itm_calls - 2; ix < itm_calls; ix++) {
	num_spreads++;
	int bid = PQgetvalue(sql_res, ix, 2), ask = PQgetvalue(sql_res, ix, 3);
	avg_spread += (100 - 100 * bid / ask);
    }
    for(int ix = itm_calls; ix < itm_calls + (call_atm? 1: 2); ix++) {
	num_spreads++;
	int bid = PQgetvalue(sql_res, ix, 2), ask = PQgetvalue(sql_res, ix, 3);
	avg_spread += (100 - 100 * bid / ask);
    }
    for(int ix = num_calls + otm_puts - 2; ix < num_calls + otm_puts; ix++) {
	num_spreads++;
	int bid = PQgetvalue(sql_res, ix, 2), ask = PQgetvalue(sql_res, ix, 3);
	avg_spread += (100 - 100 * bid / ask);
    }
    for(int ix = num_calls + otm_puts; 
	ix < num_calls + otm_puts + (put_atm? 1: 2); ix++) {
	num_spreads++;
	int bid = PQgetvalue(sql_res, ix, 2), ask = PQgetvalue(sql_res, ix, 3);
	avg_spread += (100 - 100 * bid / ask);
    }
    return avg_spread / num_spreads;
}

/** 
    This function returns the average option spread for stocks that are
    leaders, or -1, if the stock is not a leader. 
**/
int is_leader(stx_data_ptr data, char* as_of_date) {
    /** 
	A stock is a leader at a given date if:
	1. Its average activity is above a threshold.
	2. Its average range is above a threshold.
	3. It has call and put options for that date, expiring in one month,
	4. For both calls and puts, it has at least 2 strikes >= spot, and
	   2 strikes <= spot
     **/
    ts_set_day(data, as_of_date, 0);
    if (data->pos < AVG_DAYS - 1)
	return -1;
    int avg_act = 0, avg_rg = 0;
    for(int ix = ts->pos - AVG_DAYS + 1; ix < ts->pos; ix++) {
	avg_act += (data->data[ix].close * data->data[ix].volume);
	avg_rg += ts_true_range(data, ix);
    }
    avg_act /= AVG_DAYS;
    avg_rg /= AVG_DAYS;
    if ((avg_act < MIN_ACT) || 
	((1000 * avg_rg / data->data[ix].close) < MIN_RCR))
	return -1;
    char* exp;
    cal_expiry(cal_ix(as_of_date)+ 5, &exp);
    /** TODO: handle historical tickers **/
    char sql_cmd[256];
    sprintf(sql_cmd, "select spot from opt_spots where stk='%s' and dt='%s'",
	    data->stk, as_of_date);
    PGresult* res = db_query(sql_cmd);
    if (PQntuples(res) != 1) {
	PQclear(res);
	return -1;
    }
    int spot = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    sprintf(sql_cmd, "select cp, strike, bid, ask fron options where "
	    "und='%s' and dt='%s' and exp='%s' order by cp, strike",
	    data->stk, as_of_date, exp);
    res = db_query(sql_cmd);
    int num = PQntuples(res);
    if (num < 5) {
	PQclear(res);
	return -1;
    }
    int avg_spread = ana_calc_spread(res, spot);
    PQclear(res);
    return avg_spread;
}


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
    sprintf(sql_cmd, "select distinct stk from eods where dt='%s' and "
	    "c*(v/100)>10", dt);
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
	stx_data_ptr data = ts_load_stk(all_stx[ix]);
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

#endif
