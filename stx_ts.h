#ifndef __STX_TS_H__
#define __STX_TS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <time.h>
#include "stx_core.h"

/** This defines a strong close, if (1+SC)*c>=SC*h+l, or (1+SC)*c<=h+SC*l*/
#define SC 4

/** BEGIN: macros */
#define module( x) (( x>  0)? x: -x)
#define sign( x) (( x> 0)? 1: -1)
/** END: macros */

int ts_true_range(stx_data_ptr data, int ix) {
    int res = data->data[ix].high - data->data[ix].low;
    if(ix == 0) 
        return res;
    if(res < data->data[ix].high - data->data[ix - 1].close)
        res = data->data[ix].high - data->data[ix - 1].close;
    if(res < data->data[ix - 1].close - data->data[ix].low)
        res = data->data[ix - 1].close - data->data[ix].low;
    return res;
}

int ts_strong_close(daily_record_ptr dr) {
    int sc_dir = 0;
    if (dr->high == dr->low)
        return sc_dir;
    if ((SC + 1) * dr->close >= SC * dr->high + dr->low)
        sc_dir = 1;
    if ((SC + 1) * dr->close <= dr->high + SC * dr->low)
        sc_dir = -1;
    return sc_dir;
}

int ts_weighted_price(stx_data_ptr data, int ix) {
    return (data->data[ix].high + data->data[ix].low + data->data[ix].close)
        / 3;
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
    data->last_adj = -1;
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
    data->num_recs = b_days;
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
            LOGERROR("%s: Something is very wrong: dt = %s, db_date = %s\n",
                     stk, dt, db_date);
            return NULL;
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
    for(int ix = data->last_adj + 1; ix <= split_ix; ix++) {
        char *date = data->splits->list[ix].key;
        float ratio = data->splits->list[ix].val.ratio;
        /* find the index for the date in the data->data */
        /* adjust the data up to, and including that index */
        int end = ts_find_date_record(data, date, 0);
        for(int ixx = 0; ixx <= end; ++ixx) {
            data->data[ixx].open = (int)(ratio * data->data[ixx].open);
            data->data[ixx].high = (int)(ratio * data->data[ixx].high);
            data->data[ixx].low = (int)(ratio * data->data[ixx].low);
            data->data[ixx].close = (int)(ratio * data->data[ixx].close);
            data->data[ixx].volume = (int)(data->data[ixx].volume / ratio);
        }
    }
    data->last_adj = split_ix;
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

int ts_next(stx_data_ptr data) {
    if (data->pos >= data->num_recs - 1)
        return -1;
    data->pos++;
    ht_item_ptr split = ht_get(data->splits, data->data[data->pos].date);
    if (split != NULL)
        ts_adjust_data(data, ht_seq_index(data->splits, 
                                          data->data[data->pos].date));
    return 0;
}

int ts_advance(stx_data_ptr data, char* end_date) {
    int res = 0, num_days = 0;
    while ((strcmp(data->data[data->pos].date, end_date) <= 0) &&
           (res != -1)) {
        res = ts_next(data);
        num_days++;
    }
    return num_days;
}

void ts_free_data(stx_data_ptr data) {
    ht_free(data->splits);
    free(data->data);
    free(data);
}

void ts_print(stx_data_ptr data, char* s_date, char* e_date) {
    int s_ix = ts_find_date_record(data, s_date, 1);
    int e_ix = ts_find_date_record(data, e_date, -1);
    for(int ix = s_ix; ix <= e_ix; ix++)
        fprintf(stderr, "%s %7d %7d %7d %7d %7d\n", 
                data->data[ix].date, data->data[ix].open, 
                data->data[ix].high, data->data[ix].low, 
                data->data[ix].close, data->data[ix].volume);
}

void ts_print_record(daily_record_ptr record) {
    fprintf(stderr, "%s %7d %7d %7d %7d %7d", record->date, record->open, 
            record->high, record->low, record->close, record->volume);
}

float rs_relative_strength(stx_data_ptr data, int ix, int rs_days) { 
    int rsd1 = rs_days / 4, rsd2 = rs_days / 2;
    float rs_1, rs_2, rs_3, res;
    float cc = (float)data->data[ix].close;
    float cc_0 = (float)data->data[0].close;
    float cc_1 = (float)data->data[ix + 1 - rsd1].close;
    float cc_2 = (float) data->data[ix + 1 - rsd2].close;
    float cc_3 = (float)data->data[ix + 1 - rs_days].close;
    if (ix >= rs_days - 1) {
        rs_1 = (cc_1 == 0)? 0: cc / cc_1 - 1;
        rs_2 = (cc_2 == 0)? 0: cc / cc_2 - 1;
        rs_3 = (cc_3 == 0)? 0: cc / cc_3 - 1;
    } else if (ix >= rsd2 - 1) {
        rs_1 = (cc_1 == 0)? 0: cc / cc_1 - 1;
        rs_2 = (cc_2 == 0)? 0: cc / cc_2 - 1;
        rs_3 = (cc_0 == 0)? 0: cc / cc_0 - 1;
    } else if (ix >= rsd1 - 1) {
        rs_1 = (cc_1 == 0)? 0: cc / cc_1 - 1;
        rs_2 = (cc_0 == 0)? 0: cc / cc_0 - 1;
        rs_3 = rs_2;
    } else {
        rs_1 = (cc_0 == 0)? 0: cc / cc_0 - 1;
        rs_2 = rs_1;
        rs_3 = rs_1;
    }
    res = 40 * rs_1 + 30 * rs_2 + 30 * rs_3;
    return (int) res;
}

#endif
