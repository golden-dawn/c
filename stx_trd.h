#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
#include "stx_ts.h"

#define TRD_CAPITAL 500
#define JL_200 "200"

typedef struct trade_t {
    char cp;
    char stk[16];
    char in_dt[16];
    char out_dt[16];
    char exp_dt[16];
    char setup[16];
    int in_spot;
    int in_range;
    int out_spot;
    int spot_pnl;
    int num_contracts;
    int strike;
    int in_ask;
    int out_bid;
    int opt_pnl;
    int opt_pct_pnl;
    int moneyness;
} trade, *trade_ptr;


static hashtable_ptr data_stx = NULL;
hashtable_ptr trd_data() {
    if (data_stx == NULL)
	data_stx = ht_new(NULL, 20000);
    return data_stx;
}


static hashtable_ptr jl = NULL;
hashtable_ptr trd_jl(const char* factor) {
    if (jl == NULL)
        jl = ht_new(NULL, 5);
    ht_item_ptr jlht = ht_get(jl, factor);
    hashtable_ptr jl_factor_ht = NULL;
    if (jlht == NULL) {
        jl_factor_ht = ht_new(NULL, 20000);
        jlht = ht_new_data(factor, (void *) jl_factor_ht);
        ht_insert(jl, jlht);
    } else
        jl_factor_ht = (hashtable_ptr) jlht->val.data;
    return jl_factor_ht;
}


int init_trade(trade_ptr trd)


void process_trade(trade_ptr trd) {
    ht_item_ptr ht_jl = ht_get(trd_jl(JL_200), trd->stk);
    jl_data_ptr jl_recs = NULL;
    if (ht_jl == NULL) {
        stx_data_ptr data = ts_load_stk(trd->stk);
        if (data == NULL) {
            LOGERROR("Could not load %s, skipping...\n", trd->stk);
            return;
        }
        jl_recs = jl_jl(data, dt, JL_FACTOR);
        ht_jl = ht_new_data(stk, (void*)jl_recs);
        ht_insert(ana_jl(JL_200), ht_jl);
    } else {
        jl_recs = (jl_data_ptr) ht_jl->val.data;
        jl_advance(jl_recs, trd->in_dt);
    }
    char *exp_date;
    cal_expiry_next(cal_ix(trd->in_dt), &exp_date);
    strcpy(trd->exp_date, exp_date);
    char sql_cmd[128];
    sprintf(sql_cmd, "select strike, bid, ask from options where und='%s' and "
            "dt='%s' and expiry='%s' and cp='%c' order by strike", trd->stk,
            trd->in_dt, trd->exp_dt, trd->cp);
    PGresult *opt_res = db_query(sql_cmd);
    int rows = PQntuples(opt_res);
    trd->in_spot = data->data[data->pos].close;
    int dist, dist_1 = 1000000, strike, strike_1, ask = 1000000;
    for(int ix = 0; ix < rows; ix++) {
	strike = atoi(PQgetvalue(sql_res, ix, 0));
	dist = abs(strike - trd->in_spot);
	if (dist > dist_1) {
	    trd->strike = strike_1;
	    trd->in_ask = ask;
	    break;
	}
	strike_1 = strike;
	dist_1 = dist;
	/** TODO: check if the strike is further than two daily ranges **/
    }
    PQclear(opt_res);
    bool exit_trade = false;
    while (!exit_trade) {
	if (ts_next(data) == -1) {
	    exit_trade = true;
	    data->pos = data->num_recs - 1;
	} else {
	    
	}
    }
    sprintf(sql_cmd, "select "

}

void trd_trade(char *start_date, char *end_date, char *stx, char *setups,
	       bool triggered) {
    /**
     v * 1. Tokenize stx, setups
     v * 2. Build query that retrieves all setups
     * 3. For each query result: 
       v  a. Load stock time series, if not already there
	  b. Load the option.
	  c. Iterate through the time series, check exit rules
	  d. When exit rules are triggered, calculate PnLs, insert trade in DB
     **/
    char *sql_cmd = (char *) calloc((size_t)256, sizeof(char));
    strcat(sql_cmd, "SELECT * FROM setups WHERE dt BETWEEN '");
    strcat(sql_cmd, start_date);
    strcat(sql_cmd, "' AND '");
    strcat(sql_cmd, end_date);
    strcat(sql_cmd, "' ");
    if (triggered)
	strcat(sql_cmd, "AND triggered='t' ");
    if (stx != NULL && strcmp(stx, "")) {
	char *stk_tokens = (char *) calloc((size_t) strlen(stx), sizeof(char));
	strcpy(stk_tokens, stx);
	strcat(sql_cmd, "AND stk IN ('");
	int ix = 0;
	char* token = strtok(stx, ",");
	while (token) {
	    if (ix++ > 0)
		strcat(sql_cmd, "', '");
	    strcat(sql_cmd, token);
	    token = strtok(NULL, ",");
	}
	strcat(sql_cmd, "') ");
	free(stk_tokens);
    }
    if (setups != NULL && strcmp(setups, "")) {
	char *setup_tokens = (char *) calloc((size_t) strlen(setups), 
					     sizeof(char));
	strcpy(setup_tokens, setups);
 	strcat(sql_cmd, "AND setup IN ('");
	printf("11\n");
	int ix = 0;
	char* token = strtok(setup_tokens, ",");
	while (token) {
	    printf("12 token = %s, ix = %d\n", token, ix);
	    if (ix++ > 0)
		strcat(sql_cmd, "', '");
	    strcat(sql_cmd, token);
	    token = strtok(NULL, ",");
	}
	strcat(sql_cmd, "') ");
	free(setup_tokens);
    }
    strcat(sql_cmd, "ORDER BY dt, stk");
    LOGINFO("Setup SQL: %s\n", sql_cmd);
    PGresult *setup_recs = db_query(sql_cmd);
    int rows = PQntuples(setup_recs);
    LOGINFO("loaded %d setups\n", rows);
    trade trd;
    for (int ix = 0; ix < rows; ix++) {
	memset(&trd, 0, sizeof(trade));
	strcpy(trd.in_dt, PQgetvalue(setup_recs, ix, 0));
	strcpy(trd.stk, PQgetvalue(setup_recs, ix, 1));
	strcpy(trd.setup, PQgetvalue(setup_recs, ix, 2));
	trd.cp = *(PQgetvalue(setup_recs, ix, 3));
        if (ix % 100 == 0)
            LOGINFO("Analyzed %5d/%5d setups\n", ix, rows);
	process_trade(&trd);
    }
    LOGINFO("Analyzed %5d/%5d setups\n", rows, rows);
    PQclear(setup_recs);
}
