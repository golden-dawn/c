#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"


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


void trd_trade(char *start_date, char *end_date, char *stx, char *setups,
	       bool triggered) {
    /**
     v * 1. Tokenize stx, setups
     v * 2. Build query that retrieves all setups
     * 3. For each query result: 
          a. Load stock time series, if not already there
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
    }
    LOGINFO("Analyzed %5d/%5d setups\n", rows, rows);
    PQclear(setup_recs);
}
