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

typedef struct ldr_t {
    int activity;
    int range_ratio;
    int opt_spread;
    int atm_price;
    bool is_ldr;
} ldr, *ldr_ptr;

static hashtable_ptr stx = NULL;

hashtable_ptr ana_data() {
    if (stx == NULL) 
	stx = ht_new(NULL, 20000);
    return stx;
} 

void ana_option_analysis(ldr_ptr leader, PGresult* sql_res, int spot) {
    int itm_calls = 0, otm_calls = 0, itm_puts = 0, otm_puts = 0;
    int avg_spread = 0, bid, ask, strike, num_calls = 0, num_puts = 0;
    char* cp;
    bool call_atm = false, put_atm = false;
    int num = PQntuples(sql_res), num_spreads = 0, atm_price = 0;
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
	return;
    for(int ix = itm_calls - 2; ix < itm_calls; ix++) {
	num_spreads++;
	int bid = atoi(PQgetvalue(sql_res, ix, 2));
	int ask = atoi(PQgetvalue(sql_res, ix, 3));
	avg_spread += (100 - 100 * bid / ask);
	if (ix == itm_calls - 1)
	    atm_price += ask;
    }
    for(int ix = itm_calls; ix < itm_calls + (call_atm? 1: 2); ix++) {
	num_spreads++;
	int bid = atoi(PQgetvalue(sql_res, ix, 2));
	int ask = atoi(PQgetvalue(sql_res, ix, 3));
	avg_spread += (100 - 100 * bid / ask);
    }
    for(int ix = num_calls + otm_puts - 2; ix < num_calls + otm_puts; ix++) {
	num_spreads++;
	int bid = atoi(PQgetvalue(sql_res, ix, 2));
	int ask = atoi(PQgetvalue(sql_res, ix, 3));
	avg_spread += (100 - 100 * bid / ask);
	if (ix == num_calls + otm_puts - 1)
	    atm_price += ask;
    }
    for(int ix = num_calls + otm_puts; 
	ix < num_calls + otm_puts + (put_atm? 1: 2); ix++) {
	num_spreads++;
	int bid = atoi(PQgetvalue(sql_res, ix, 2));
	int ask = atoi(PQgetvalue(sql_res, ix, 3));
	avg_spread += (100 - 100 * bid / ask);
    }
    leader->opt_spread = avg_spread / num_spreads;
    if (leader->opt_spread >= 0)
	leader->is_ldr = true;
    leader->atm_price = atm_price / 2;
}

/** 
    This function returns the average option spread for stocks that are
    leaders, or -1, if the stock is not a leader. 
**/
ldr_ptr ana_leader(stx_data_ptr data, char* as_of_date, char* exp) {
    /** 
	A stock is a leader at a given date if:
	1. Its average activity is above a threshold.
	2. Its average range is above a threshold.
	3. It has call and put options for that date, expiring in one month,
	4. For both calls and puts, it has at least 2 strikes >= spot, and
	   2 strikes <= spot
     **/
    ldr_ptr leader = (ldr_ptr) calloc((size_t)1, sizeof(ldr));
    leader->is_ldr = false;
    ts_set_day(data, as_of_date, 0);
    if (data->pos < AVG_DAYS - 1)
	return leader;
    int avg_act = 0, avg_rg = 0;
    for(int ix = data->pos - AVG_DAYS + 1; ix < data->pos; ix++) {
	avg_act += (data->data[ix].close * data->data[ix].volume);
	avg_rg += ts_true_range(data, ix);
    }
    avg_act /= AVG_DAYS;
    avg_rg /= AVG_DAYS;
    leader->activity = avg_act;
    leader->range_ratio = avg_rg;
    if ((avg_act < MIN_ACT) || 
	((1000 * avg_rg / data->data[data->pos].close) < MIN_RCR))
	return leader;
    char und[16];
    strcpy(und, data->stk);
    char* dot = strchr(und, '.');
    if(dot != NULL) {
	if (( '0' <= *(dot + 1)) && (*(dot + 1)<= '9'))
            *dot = '\0';
    }
    char sql_cmd[256];
    sprintf(sql_cmd, "select spot from opt_spots where stk='%s' and dt='%s'",
	    und, as_of_date);
    PGresult* res = db_query(sql_cmd);
    if (PQntuples(res) != 1) {
	PQclear(res);
	return leader;
    }
    int spot = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    sprintf(sql_cmd, "select cp, strike, bid, ask from options where "
	    "und='%s' and dt='%s' and expiry='%s' order by cp, strike",
	    und, as_of_date, exp);
    res = db_query(sql_cmd);
    int num = PQntuples(res);
    if (num < 5) {
	PQclear(res);
	return leader;
    }
    ana_option_analysis(leader, res, spot);
    PQclear(res);
    return leader;
}


int ana_expiry_analysis(char* dt) {
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
    char sql_cmd[256];
    char *sql_1 = "select distinct stk from eods where dt='";
    char *sql_2 = "' and stk not like '#%' and stk not like '^*' and "
	"(c/100)*(v/100)>100";
    sprintf(sql_cmd, "%s%s%s", sql_1, dt, sql_2);
    PGresult *res = db_query(sql_cmd);
    int rows = PQntuples(res);
    LOGINFO("loaded %5d stocks\n", rows);
    FILE* fp = NULL;
    char *filename = "/tmp/leaders.csv", *exp;
    cal_expiry(cal_ix(dt) + 5, &exp);
    if((fp = fopen(filename, "w")) == NULL) {
	LOGERROR("Failed to open file %s for writing\n", filename);
	return -1;
    }
    for (int ix = 0; ix < rows; ix++) {
	char stk[16];
	strcpy(stk, PQgetvalue(res, ix, 0));
	ht_item_ptr ht_data = ht_get(ana_data(), stk);
	stx_data_ptr data = NULL;
	if (ht_data == NULL) {
	    data = ts_load_stk(stk);
	    ht_data = ht_new_data(stk, data);
	    ht_insert(ana_data(), ht_data);
	} else
	    data = ht_data->val.data;
	ldr_ptr leader = ana_leader(data, dt, exp);
	if (leader->is_ldr)
	    fprintf(fp, "%s\t%s\t%d\t%d\t%d\t%d\n", exp, stk, leader->activity,
		    leader->range_ratio, leader->opt_spread, 
		    leader->atm_price);
	free(leader);
	if (ix % 100 == 0)
	    LOGINFO("%s: analyzed %5d/%5d stocks\n", dt, ix, rows);
    }
    fclose(fp);
    LOGINFO("%s: analyzed %5d/%5d stocks\n", dt, rows, rows);
    PQclear(res);
    db_upload_file("leaders", filename);
    LOGINFO("%s: uploaded leaders in the database as of date %s\n", exp, dt);
    /* 2. For each stock, get the data */
    /* 3. Set the time series to a specific date (dt) */
    /* 4. Get all the splits up to dt */
    /* 5. Adjust the data accordingly */
    /* 6. Run JL on the adjusted data */
    /* 7. Run the setups */
    return 0;
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
