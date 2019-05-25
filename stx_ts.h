#ifndef __STX_TS_H__
#define __STX_TS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <time.h>
#include "stx_core.h"

/** BEGIN: macros */
#define module( x) (( x>  0)? x: -x)
#define sign( x) (( x> 0)? 1: -1)
/** END: macros */

float wprice( daily_record_ptr data, int ix) {
    return ( data[ ix].close+ data[ ix].high+ data[ ix].low)/ 3;
}


hashtable_ptr ts_load_splits(char* stk) {
    char sql_cmd[80];
    sprintf(sql_cmd, "select ratio, dt from dividends where stk='%s' "
	    "order by dt", stk);
    PGresult *res = db_query(sql_cmd);
#ifdef DEBUG
    LOGDEBUG("Found %d splits for %s\n", PQntuples(res), stk);
#endif
    hashtable_ptr result = ht_divis(res);
#ifdef DEBUG
    LOGDEBUG("Loaded splits for %s\n", stk);
#endif
    PQclear(res);
    return result;
}


stx_data_ptr ts_load_stk(char* stk) {
#ifdef DEBUG
    LOGDEBUG("Loading data for %s\n", stk);
#endif
    stx_data_ptr data = (stx_data_ptr) malloc(sizeof(stx_data));
    data->data = NULL;
    data->num_recs = 0;
    data->pos = 0;
    char sql_cmd[128];
    sprintf(sql_cmd, "select o, hi, lo, c, v, dt from eods where stk='%s' "
	    "and dt>'1985-01-01'order by dt", stk);
    PGresult *res = db_query(sql_cmd);
    if((data->num_recs = PQntuples(res)) <= 0) 
	return data;
    int num = data->num_recs;
    char sd[16], ed[16];
    strcpy(sd, PQgetvalue(res, 0, 5));
    strcpy(ed, PQgetvalue(res, num - 1, 5));
    int b_days = cal_num_busdays(sd, ed);
#ifdef DEBUG
    LOGDEBUG("Found %d records for %s\n", num, stk);
#endif
    data->data = (daily_record_ptr) calloc(b_days, sizeof(daily_record));
    int ts_idx = 0;
    int calix = cal_ix(sd);
    char* dt;
    calix = cal_prev_bday(calix, &dt);
    for(int ix = 0; ix < num; ix++) {
	calix = cal_next_bday(calix, &dt);
	char* db_date = PQgetvalue(res, ix, 5);
	if (strcmp(dt, db_date) > 0) {
	    LOGERROR("Something is very wrong: dt = %s, db_date = %s\n",
		     dt, db_date);
	    exit(-1);
	}
	while (strcmp(dt, db_date) < 0) {
#ifdef DEBUG
	    LOGDEBUG("Adding data for %s, not found in %s data\n", dt, stk);
#endif
	    data->data[ts_idx].open = data->data[ts_idx - 1].close;
	    data->data[ts_idx].high = data->data[ts_idx - 1].close;
	    data->data[ts_idx].low = data->data[ts_idx - 1].close;
	    data->data[ts_idx].close = data->data[ts_idx - 1].close;
	    data->data[ts_idx].volume = 0;
	    strcpy(data->data[ts_idx].date, dt); 
	    calix = cal_next_bday(calix, &dt);
	    ts_idx++;
	}   
	data->data[ts_idx].open = atoi(PQgetvalue(res, ix, 0));
	data->data[ts_idx].high = atoi(PQgetvalue(res, ix, 1));
	data->data[ts_idx].low = atoi(PQgetvalue(res, ix, 2));
	data->data[ts_idx].close = atoi(PQgetvalue(res, ix, 3));
	data->data[ts_idx].volume = atoi(PQgetvalue(res, ix, 4));
	strcpy(data->data[ts_idx].date, PQgetvalue(res, ix, 5)); 
	ts_idx++;
    }
    data->pos = b_days - 1;
    PQclear(res);
#ifdef DEBUG
    LOGDEBUG("Loading the splits for %s\n", stk);
#endif
    data->splits = ts_load_splits(stk);
    strcpy(data->stk, stk);
#ifdef DEBUG
    LOGDEBUG("Done loading %s\n", stk);
#endif
    return data;
}

int ts_find_date_record(stx_data_ptr data, char* date, int rel_pos) {
    /** rel_pos is a parameter that can take the following values:
     *  0 - do an exact search
     *  1 - return date, or next business day, if date not found
     * -1 - return date, or previous business day, if date not found
     **/
    char* first_date = data->data[0].date;
    int n = cal_num_busdays(first_date, date) - 1;
    if (n < 0) {
	if (rel_pos > 0)
	    return 0;
    } else if (n >= data->num_recs) {
	if (rel_pos < 0)
	    return data->num_recs - 1;;
    } else {
	if (strcmp(data->data[n].date, date) == 0)
	    return n;
	else {
	    if (rel_pos < 0)
		return n - 1;
	    if (rel_pos > 0)
		return n;
	}
    }
    return -1;
}

void ts_adjust_data(stx_data_ptr data, int split_ix) {
    if (split_ix < 0) 
	return;
    for(int ix = 0; ix <= split_ix; ix++) {
	/* char *date = data->splits->list[ix].key;  */
	/* float ratio = data->splits->list[ix].val.ratio; */
	/* find the index for the date in the data->data */
	/* adjust the data up to, and including that index */
    }
}

void ts_set_day(stx_data_ptr data, char* date, int rel_pos) {
    data->pos = ts_find_date_record(data, date, rel_pos);
    if (data->pos == -1) {
	LOGERROR("Could not set date to %s for %s\n", date, data->stk);
	return;
    }
    int split_ix = ht_seq_index(data->splits, date);
    if (split_ix >= 0)
	ts_adjust_data(data, split_ix);
}

void ts_free_data(stx_data_ptr data) {
    ht_free(data->splits);
    free(data->data);
    free(data);
}
#endif
