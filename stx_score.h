#ifndef __STX_SCORE_H__
#define __STX_SCORE_H__

#include <cjson/cJSON.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include "stx_ana.h"
#include "stx_core.h"
#include "stx_jl.h"
#include "stx_net.h"
#include "stx_setups.h"
#include "stx_ts.h"

typedef struct ldr_t {
    int activity;
    int range_ratio;
    int opt_spread;
    int atm_price;
    bool is_ldr;
} ldr, *ldr_ptr;

static hashtable_ptr stx = NULL;
static hashtable_ptr jl = NULL;

/** Return the hash table with EOD stock data. */
hashtable_ptr ana_data() {
    if (stx == NULL) 
        stx = ht_new(NULL, 20000);
    return stx;
}

/** Return the hash table with JL stock data for a given factor */
hashtable_ptr ana_jl(const char* factor) {
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

/** 
 * Analyze option data to determine whether a stock is a leader or
 * not. To be a leader a stock must:
 * 1. Have at least 2 ATM/ITM calls and puts, and 2 ATM/OTM calls and puts.
 * 2. Have a non-negative average spread for the calls and puts found above.
 * 
 * The average spread and ATM price (an average between ATM call and
 * put) are stored for each leader.
 * 
 * Subsequently, leaders can be filtered, by choosing only those with
 * the average spread less than a threshold, and the ATM price less
 * than a max price.

*/
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
ldr_ptr ana_leader(stx_data_ptr data, char* as_of_date, char* exp, 
                   bool realtime_analysis) {
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
        avg_act += ((data->data[ix].close / 100) * 
                    (data->data[ix].volume / 100));
        avg_rg += (1000 * ts_true_range(data, ix) / data->data[ix].close);
    }
    avg_act /= AVG_DAYS;
    avg_rg /= AVG_DAYS;
    leader->activity = avg_act;
    leader->range_ratio = avg_rg;
    if ((avg_act < MIN_ACT) || (avg_rg < MIN_RCR))
        return leader;
    char und[16];
    strcpy(und, data->stk);
    char* dot = strchr(und, '.');
    if (dot != NULL) {
        if (('0' <= *(dot + 1)) && (*(dot + 1) <= '9'))
            *dot = '\0';
    }
    char sql_cmd[256];
    bool current_analysis = !strcmp(as_of_date, cal_current_busdate(5));
    if (realtime_analysis) 
        sprintf(sql_cmd, "select c from eods where stk='%s' and dt='%s' "
                "and oi=0", und, as_of_date);
    else
        sprintf(sql_cmd, "select spot from opt_spots where stk='%s' and "
                "dt='%s'", und, as_of_date);
    PGresult* res = db_query(sql_cmd);
    if (PQntuples(res) != 1) {
        PQclear(res);
        return leader;
    }
    int spot = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    if (realtime_analysis) {
        FILE *opt_fp = fopen("/tmp/options.csv", "w");
        if (opt_fp == NULL) {
            LOGERROR("Failed to open /tmp/options.csv file");
            opt_fp = stderr;
        } else {
            net_get_option_data(NULL, opt_fp, und, as_of_date, exp, 
                                cal_long_expiry(exp));
            fclose(opt_fp);
            db_upload_file("options", "/tmp/options.csv");
        }
    }
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

int ana_expiry_analysis(char* dt, bool realtime_analysis) {
    /** 
     * special case when the date is an option expiry date
     * if the data is NULL, only run for the most recent business day
     * 1. wait until eoddata is downloaded. 
     **/
    LOGINFO("<begin>ana_expiry_analysis(%s)\n", dt);
    char sql_cmd[256], *exp;
    cal_expiry(cal_ix(dt) + 5, &exp);
    sprintf(sql_cmd, "select * from analyses where dt='%s' and "
            "analysis='leaders'", dt);
    PGresult *res = db_query(sql_cmd);
    int rows = PQntuples(res);
    PQclear(res);
    if (rows >= 1) {
        LOGINFO("Found %d leaders analyses for %s (expiry %s)\n", 
                rows, dt, exp);
        LOGINFO("Will skip leaders analyses for %s (expiry %s)\n", dt, exp);
        return 0;
    }
    char *sql_1 = "select distinct stk from eods where dt='";
    char *sql_2 = "' and stk not like '#%' and stk not like '^%' and oi=0 "
        "and (c/100)*(v/100)>100";
    sprintf(sql_cmd, "%s%s%s", sql_1, dt, sql_2);
    res = db_query(sql_cmd);
    rows = PQntuples(res);
    LOGINFO("loaded %5d stocks\n", rows);
    FILE* fp = NULL;
    char *filename = "/tmp/leaders.csv";
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
            if (data == NULL)
                continue;
            ht_data = ht_new_data(stk, (void*)data);
            ht_insert(ana_data(), ht_data);
        } else
            data = (stx_data_ptr) ht_data->val.data;
        ldr_ptr leader = ana_leader(data, dt, exp, realtime_analysis);
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
    if (rows > 0) {
        memset(sql_cmd, 0, 256 * sizeof(char));
        sprintf(sql_cmd, "INSERT INTO analyses VALUES ('%s', 'leaders')", dt);
        db_transaction(sql_cmd);
    }
    LOGINFO("<end>ana_expiry_analysis(%s)\n", dt);
    return 0;
}

cJSON* ana_get_leaders(char* exp, int max_atm_price, int max_opt_spread,
                       int max_num_ldrs) {
    cJSON *leader_list = cJSON_CreateArray();
    if (leader_list == NULL) {
        LOGERROR("Failed to create leader_list cJSON Array.\n");
        return NULL;
    }
    char sql_cmd[256];
    sprintf(sql_cmd, "select stk from leaders where expiry='%s'", exp);
    if (max_atm_price > 0)
        sprintf(sql_cmd, "%s and atm_price <= %d", sql_cmd, max_atm_price);
    if (max_opt_spread > 0)
        sprintf(sql_cmd, "%s and opt_spread <= %d", sql_cmd, max_opt_spread);
    sprintf(sql_cmd, "%s and stk not in (select * from excludes)", sql_cmd);
    if (max_num_ldrs > 0)
        sprintf(sql_cmd, "%s order by opt_spread limit %d", sql_cmd, 
                max_num_ldrs);
    LOGINFO("ana_get_leaders():\n  sql_cmd %s\n", sql_cmd);
    PGresult *res = db_query(sql_cmd);
    int rows = PQntuples(res);
    LOGINFO("  returned %d leaders\n", rows);
    cJSON *ldr_name = NULL;
    char* stk = NULL;
    for (int ix = 0; ix < rows; ix++) {
        stk = PQgetvalue(res, ix, 0);
        ldr_name = cJSON_CreateString(stk);
        if (ldr_name == NULL) {
            LOGERROR("Failed to create cJSON string for %s\n", stk);
            continue;
        }
        cJSON_AddItemToArray(leader_list, ldr_name);
    }
    PQclear(res);
    return leader_list;    
}

void ana_pullbacks(FILE* fp, char* stk, char* dt, jl_data_ptr jl_recs) {
    daily_record_ptr dr = jl_recs->data->data;
    int ix = jl_recs->data->pos, trigrd = 1;
    bool res;
    if ((jl_recs->last->prim_state == UPTREND) && 
        (jl_recs->last->state == UPTREND) && (dr[ix].high > dr[ix - 1].high)) {
        if (stp_jc_1234(dr, ix - 1, UP))
            fprintf(fp, "%s\t%s\tJC_1234\t%c\t%d\n", dt, stk, UP, trigrd);
        if (stp_jc_5days(dr, ix - 1, UP))
            fprintf(fp, "%s\t%s\tJC_5DAYS\t%c\t%d\n", dt, stk, UP, trigrd);
    } else if ((jl_recs->last->prim_state == DOWNTREND) && 
               (jl_recs->last->state == DOWNTREND) && 
               (dr[ix].low < dr[ix - 1].low)) {
        if (stp_jc_1234(dr, ix - 1, DOWN))
            fprintf(fp, "%s\t%s\tJC_1234\t%c\t%d\n", dt, stk, DOWN, trigrd);
        if (stp_jc_5days(dr, ix - 1, DOWN))
            fprintf(fp, "%s\t%s\tJC_5DAYS\t%c\t%d\n", dt, stk, DOWN, trigrd);
    }
}

void ana_gaps_8(FILE* fp, char* stk, char* dt, jl_data_ptr jl_recs) {
    daily_record_ptr dr = jl_recs->data->data;
    int ix = jl_recs->data->pos;
    bool res;
    char setup_name[16];
    /* Find gaps */
    if ((dr[ix].open > dr[ix - 1].high) || (dr[ix].open < dr[ix - 1].low)) {
        strcpy(setup_name,  (dr[ix].volume > 1.1 * jl_recs->recs[ix].volume)? 
               "GAP_HV": "GAP");
        fprintf(fp, "%s\t%s\t%s\t%c\t1\n", dt, stk, setup_name, 
                (dr[ix].open > dr[ix - 1].high)? UP: DOWN);
    }
    /* Find strong closes on significant range and volume */
    if ((ts_true_range(jl_recs->data, ix) >= jl_recs->recs[ix].rg) && 
        (dr[ix].volume >= jl_recs->recs[ix].volume)) {
        if (4 * dr[ix].close >= 3 * dr[ix].high + dr[ix].low)
            fprintf(fp, "%s\t%s\tSTRONG_CLOSE\t%c\t1\n", dt, stk, UP);
        if (4 * dr[ix].close <= dr[ix].high + 3 * dr[ix].low)
            fprintf(fp, "%s\t%s\tSTRONG_CLOSE\t%c\t1\n", dt, stk, DOWN);
    }     
}

void ana_setups_tomorrow(FILE* fp, char* stk, char* dt, char* next_dt,
                         jl_data_ptr jl_recs) {
    daily_record_ptr dr = jl_recs->data->data;
    int ix = jl_recs->data->pos, trigrd = 0;
    bool res;
    if ((jl_recs->last->prim_state == UPTREND) && 
        (jl_recs->last->state == UPTREND)) {
        if (stp_jc_1234(dr, ix, UP))
            fprintf(fp, "%s\t%s\tJC_1234\t%c\t0\n", next_dt, stk, UP);
        if (stp_jc_5days(dr, ix, UP))
            fprintf(fp, "%s\t%s\tJC_5DAYS\t%c\t0\n", next_dt, stk, UP);
    } else if ((jl_recs->last->prim_state == DOWNTREND) && 
               (jl_recs->last->state == DOWNTREND)) {
        if (stp_jc_1234(dr, ix, DOWN))
            fprintf(fp, "%s\t%s\tJC_1234\t%c\t0\n", next_dt, stk, DOWN);
        if (stp_jc_5days(dr, ix, DOWN))
            fprintf(fp, "%s\t%s\tJC_5DAYS\t%c\t0\n", next_dt, stk, DOWN);
    }
}

void ana_setups(FILE* fp, char* stk, char* dt, char* next_dt, bool eod) {
    ht_item_ptr ht_jl = ht_get(ana_jl(JL_200), stk);
    jl_data_ptr jl_recs = NULL;
    if (ht_jl == NULL) {
        stx_data_ptr data = ts_load_stk(stk);
        if (data == NULL) {
            LOGERROR("Could not load %s, skipping...\n", stk);
            return;
        }
        jl_recs = jl_jl(data, dt, JL_FACTOR);
        ht_jl = ht_new_data(stk, (void*)jl_recs);
        ht_insert(ana_jl(JL_200), ht_jl);
    } else {
        jl_recs = (jl_data_ptr) ht_jl->val.data;
        jl_advance(jl_recs, dt);
    }
    ana_pullbacks(fp, stk, dt, jl_recs);
    ana_gaps_8(fp, stk, dt, jl_recs);
    if (eod == true)
        ana_setups_tomorrow(fp, stk, dt, next_dt, jl_recs);
}

jl_data_ptr ana_get_jl(char* stk, char* dt, const char* label, float factor) {
    ht_item_ptr ht_jl = ht_get(ana_jl(label), stk);
    jl_data_ptr jl_recs = NULL;
    if (ht_jl == NULL) {
        stx_data_ptr data = ts_load_stk(stk);
        if (data == NULL) {
            LOGERROR("Could not load JL_%s for %s, skipping...\n", label, stk);
            return NULL;
        }
        jl_recs = jl_jl(data, dt, factor);
        ht_jl = ht_new_data(stk, (void*)jl_recs);
        ht_insert(ana_jl(label), ht_jl);
    } else {
        jl_recs = (jl_data_ptr) ht_jl->val.data;
        jl_advance(jl_recs, dt);
    }
    return jl_recs;
}

/** Calculates the point where trend channel defined by the points
 *  (p1->date, p1->price), and (p2->date, p2->price) would intersect
 *  the y-axis at the current day.
 */
int ana_interpolate(jl_data_ptr jl, jl_pivot_ptr p1, jl_pivot_ptr p2) {
#ifdef DEBUGGGG
    LOGDEBUG("p1dt = %s, p2dt = %s, p1px = %d, p2px = %d\n",
             p1->date, p2->date, p1->price, p2->price);
#endif
    char *crt_date = jl->data->data[jl->data->pos - 1].date;
#ifdef DEBUGGGG
    LOGDEBUG("crt_date = %s\n", crt_date);
#endif
    float slope = (p2->price - p1->price) /
        cal_num_busdays(p1->date, p2->date);
#ifdef DEBUGGGG
    LOGDEBUG("The slope is: %f\n", slope);
#endif
    int intersect_price = (int)
        (p1->price + slope * cal_num_busdays(p1->date, crt_date));
#ifdef DEBUGGGG
    LOGDEBUG("The intersect_price is: %d\n", intersect_price);
#endif
    return intersect_price;
}

int ana_clip(int value, int lb, int ub) {
    int res = value;
    if (res < lb)
        res = lb;
    if (res > ub)
        res = ub;
    return res;
}

void ana_update_score(char *stk, char *setup_date) {
    char sql_cmd[256];
    sprintf(sql_cmd, "SELECT * FROM jl_setups where stk='%s' and dt='%s'",
            stk, setup_date);
    PGresult *res = db_query(sql_cmd);
    int trigger_score = 0, trend_score = 0;
    int rows = PQntuples(res);
    for(int ix = 0; ix < rows; ++ix) {
        char *setup_name = PQgetvalue(res, ix, 2);
        int setup_score = atoi(PQgetvalue(res, ix, 6));
        if (!strcmp(setup_name, "JL_B") || !strcmp(setup_name, "JL_P") ||
            !strcmp(setup_name, "EngHarami") || !strcmp(setup_name, "Star") ||
            !strcmp(setup_name, "Piercing") || !strcmp(setup_name, "Kicking"))
            trigger_score += setup_score;
        if (strcmp(setup_name, "JL_B") && strcmp(setup_name, "JL_P"))
            trend_score += setup_score;
    }
    PQclear(res);
    char *prev_date;
    cal_prev_bday(cal_ix(setup_date), &prev_date);
    memset(sql_cmd, 0, 256 * sizeof(char));
    sprintf(sql_cmd, "SELECT * FROM setup_scores WHERE stk='%s' AND "
            "dt BETWEEN '%s' AND '%s'", stk, prev_date, setup_date);
    res = db_query(sql_cmd);
    rows = PQntuples(res);
    if (rows == 1) {
        char *score_date = PQgetvalue(res, 0, 0);
        if (!strcmp(score_date, setup_date)) {
            LOGWARN("Scores already calculated for %s on %s\n", stk, setup_date);
            goto end;
        } else
            trend_score += 7 * atoi(PQgetvalue(res, 0, 3)) / 8;
    } else if (rows == 2) {
        LOGWARN("Scores already calculated for %s on %s\n", stk, setup_date);
        goto end;
    } else if (rows == 0)
        LOGINFO("Starting setup score calculation for %s on %s\n",
                stk, setup_date);
    memset(sql_cmd, 0, 256 * sizeof(char));
    sprintf(sql_cmd, "INSERT INTO setup_scores VALUES ('%s', '%s', %d, %d)",
            setup_date, stk, trigger_score, trend_score);
    db_transaction(sql_cmd);
end:
    if (rows > 0)
        PQclear(res);
}

int ana_daily_score(char* stk, char* start_date, char* end_date) {
    int score = 0;
    char sql_cmd[256];
    sprintf(sql_cmd, "SELECT * FROM jl_setups WHERE stk='%s' AND dt BETWEEN"
            " '%s' and '%s' ORDER BY dt", stk, start_date, end_date);
    PGresult* res = db_query(sql_cmd);
    int rows = PQntuples(res);
    if (rows == 0)
        return score;
    char *crs_date = start_date;
    for (int ix = 0; ix < rows; ix++) {
        char setup_dt[16];
        strcpy(setup_dt, PQgetvalue(res, ix, 0));
        while(strcmp(crs_date, setup_dt) < 0) {
            cal_next_bday(cal_ix(crs_date), &crs_date);
            score = score * 7 / 8;
        }
        score += atoi(PQgetvalue(res, ix, 6));
    }
    while(strcmp(crs_date, end_date) < 0) {
        cal_next_bday(cal_ix(crs_date), &crs_date);
        score = score * 7 / 8;
    }
    PQclear(res);
    return score;
}

int ana_calculate_score(cJSON *setup) {
    char* setup_name = cJSON_GetObjectItem(setup, "setup")->valuestring;
    char* dir_str = cJSON_GetObjectItem(setup, "direction")->valuestring;
    cJSON* info = cJSON_GetObjectItem(setup, "info");
    int dir = (*dir_str == 'U')? 1: -1;
    int score = 0;
    if (!strcmp(setup_name, "SC")) {
        int vr = ana_clip(cJSON_GetObjectItem(info, "vr")->valueint, 50, 250);
        int rr = ana_clip(cJSON_GetObjectItem(info, "rr")->valueint, 50, 250);
        score = (vr - 50 + rr - 50) * dir;
    } else if (!strcmp(setup_name, "Gap")) { 
        /** TODO: add more params to distinguish between breakaway and exhaustion gaps */
        int vr = cJSON_GetObjectItem(info, "vr")->valueint;
        int eod_gain = cJSON_GetObjectItem(info, "eod_gain")->valueint;
        int drawdown = cJSON_GetObjectItem(info, "drawdown")->valueint;
        score = vr * (eod_gain + drawdown) / 150;
    } else if (!strcmp(setup_name, "RDay")) {
        int vr = cJSON_GetObjectItem(info, "vr")->valueint;
        int rd_gain = cJSON_GetObjectItem(info, "rd_gain")->valueint;
        int rd_drawdown = cJSON_GetObjectItem(info, "rd_drawdown")->valueint;
        int rdr = ana_clip(abs(rd_gain) + abs(rd_drawdown), 0, 300);
        score = dir * vr * rdr / 150;
    } else if (!strcmp(setup_name, "JL_P")) {
        int ls = cJSON_GetObjectItem(info, "ls")->valueint;
        int lvd = cJSON_GetObjectItem(info, "lvd")->valueint;
        int obv = cJSON_GetObjectItem(info, "obv")->valueint;
        score = 5 * obv;
        if (((dir == 1) && (ls == UPTREND)) ||
            ((dir == -1) && (ls == DOWNTREND)))
                score += (dir * 50);
        else if ((ls == RALLY) || (ls == REACTION))
                score += (dir * 25);
        if (lvd * dir > 0)
            score -= (5 * lvd);
        else
            score -= (2 * lvd);
        if (dir * score < 0)
            score = 0;
    } else if (!strcmp(setup_name, "JL_B")) {
        int vr = cJSON_GetObjectItem(info, "vr")->valueint;
        int len = cJSON_GetObjectItem(info, "len")->valueint;
        int obv = cJSON_GetObjectItem(info, "obv")->valueint;
        int last_ns = cJSON_GetObjectItem(info, "last_ns")->valueint;
        int prev_ns = cJSON_GetObjectItem(info, "prev_ns")->valueint;
        score = 5 * obv;
        score = score * vr / 150;
        score = score * ana_clip(len, 50, 250) / 50;
        if (dir == 1) {
            if (last_ns == UPTREND) {
                if (prev_ns == UPTREND)
                    score = score / 2;
                else if (prev_ns == RALLY)
                    score = score * 2;
            }
        } else { /** dir == -1 */
            if (last_ns == DOWNTREND) {
                if (prev_ns == DOWNTREND)
                    score = score / 2;
                else if (prev_ns == REACTION)
                    score = score * 2;
            }
        }
    } else if (!strcmp(setup_name, "Piercing") ||
               !strcmp(setup_name, "Kicking") ||
               !strcmp(setup_name, "EngHarami"))
        score = dir * 100;
    else if (!strcmp(setup_name, "Star"))
        score = dir * 150;
    else if (!strcmp(setup_name, "Engulfing"))
        score = dir * 50;
    else if (!strcmp(setup_name, "3out"))
        score = dir * 50;
    return score;
}

/** Utility function to add a setup to an array for jl_setups table */
void ana_add_to_setups(cJSON *setups, jl_data_ptr jl, char *setup_name,
                       int dir, cJSON *info, bool triggered) {
    if (setups == NULL)
        setups = cJSON_CreateArray();
    /** Check if the last setup in the array is JL_P or JL_B.  Remove it,
     * because it is a lower strength setup for a smaller factor */
    if (!strcmp(setup_name, "JL_B") || !strcmp(setup_name, "JL_P")) {
        int num_setups = cJSON_GetArraySize(setups);
        if (num_setups > 0) {
            cJSON *last_setup = cJSON_GetArrayItem(setups, num_setups - 1);
            char *last_setup_name =
                cJSON_GetObjectItem(last_setup, "setup")->valuestring;
            if (!strcmp(setup_name, last_setup_name))
                cJSON_DeleteItemFromArray(setups, num_setups - 1);
        }
    }
    char *direction = (dir > 0)? "U": "D";
    int factor = (jl == NULL)? 0: (int) (100 * jl->factor);
    cJSON *res = cJSON_CreateObject();
    cJSON_AddStringToObject(res, "setup", setup_name);
    cJSON_AddNumberToObject(res, "factor", factor);
    cJSON_AddStringToObject(res, "direction", direction);
    cJSON_AddStringToObject(res, "triggered", triggered? "TRUE": "FALSE");
    cJSON_AddItemToObject(res, "info", info);
    int score = ana_calculate_score(res);
    cJSON_AddNumberToObject(res, "score", score);
    cJSON_AddItemToArray(setups, res);
}

/** Check whether a given day intersects a channel built by connecting
 * two pivots.  This applies to both breakout detection, and trend
 * channel break detection.
 */

void ana_check_for_breaks(cJSON *setups, jl_data_ptr jl, jl_piv_ptr pivs,
                          int ls_050) {
    int i = jl->data->pos - 1, num = pivs->num;
    if (num < 5)
        return;
    jl_pivot_ptr pivots = pivs->pivots;
    daily_record_ptr r = &(jl->data->data[i]), r_1 = &(jl->data->data[i - 1]);
    int len_1 = cal_num_busdays(pivots[num - 4].date, r->date);
    int len_2 = cal_num_busdays(pivots[num - 5].date, r->date);
    if ((len_1 < MIN_CHANNEL_LEN) && (len_2 < MIN_CHANNEL_LEN))
        return;
    int px_up = -1, px_down = -1, upper_channel_len, lower_channel_len;
    if (jl_up(pivots[num - 1].state)) { /* stock in uptrend/rally */
        px_up = ana_interpolate(jl, &(pivots[num - 5]), &(pivots[num - 3]));
        upper_channel_len = len_2;
        px_down = ana_interpolate(jl, &(pivots[num - 4]), &(pivots[num - 2]));
        lower_channel_len = len_1;
    } else { /* stock in downtrend/reaction */
        px_up = ana_interpolate(jl, &(pivots[num - 4]), &(pivots[num - 2]));
        upper_channel_len = len_1;
        px_down = ana_interpolate(jl, &(pivots[num - 5]), &(pivots[num - 3]));
        lower_channel_len = len_2;
    }
    /* filter cases when upper channel is below lower channel */
    if (px_up <= px_down)
        return;
    /* find the extremes for today's price either the high/low for the
       day, or yesterday's close */
    int ub = (r->high > r_1->close)? r->high: r_1->close;
    int lb = (r->low < r_1->close)? r->low: r_1->close;
    int v_pos_2 = (jl->recs[jl->pos - 2].volume == 0)? 1:
        jl->recs[jl->pos - 2].volume;
    /** only add JL_B setup if the current record is a primary record for the
     * factor and in the direction of the trend (e.g. RALLY or UPTREND for an
     * up direction setup) */
    if (jl_up_all(ls_050) && (upper_channel_len >= MIN_CHANNEL_LEN) &&
        (px_up > lb) && (px_up < ub) && (jl->recs[i].lns == i) &&
        jl_up(jl->last->prim_state)) {
        cJSON *info = cJSON_CreateObject();
        cJSON_AddNumberToObject(info, "ipx", px_up);
        cJSON_AddNumberToObject(info, "len", upper_channel_len);
        cJSON_AddNumberToObject(info, "vr", 100 * r->volume / v_pos_2);
        cJSON_AddNumberToObject(info, "last_ns", jl->last->prim_state);
        cJSON_AddNumberToObject(info, "prev_ns", jl_prev_ns(jl));
        cJSON_AddNumberToObject(info, "obv", pivots[num - 2].obv);
        ana_add_to_setups(setups, jl, "JL_B", 1, info, true);
    }
    if (jl_down_all(ls_050) && (lower_channel_len >= MIN_CHANNEL_LEN) &&
        (px_down > lb) && (px_down < ub) && (jl->recs[i].lns == i) &&
        jl_down(jl->last->prim_state)) {
        cJSON* info = cJSON_CreateObject();
        cJSON_AddNumberToObject(info, "ipx", px_down);
        cJSON_AddNumberToObject(info, "len", lower_channel_len);
        cJSON_AddNumberToObject(info, "vr", 100 * r->volume / v_pos_2);
        cJSON_AddNumberToObject(info, "last_ns", jl->last->prim_state);
        cJSON_AddNumberToObject(info, "prev_ns", jl_prev_ns(jl));
        cJSON_AddNumberToObject(info, "obv", pivots[num - 2].obv);
        ana_add_to_setups(setups, jl, "JL_B", -1, info, true);
    }
}

void ana_add_jl_pullback_setup(cJSON *setups, jl_data_ptr jl, int direction,
                               int piv_ix, bool lt_piv, jl_piv_ptr pivs,
                               jl_piv_ptr pivs_150, jl_piv_ptr pivs_200) {
    int num = pivs->num, num_150 = pivs_150->num, num_200 = pivs_200->num;
    jl_pivot_ptr pivots = pivs->pivots, pivots_150 = pivs_150->pivots;
    jl_pivot_ptr pivots_200 = pivs_200->pivots;
    jl_pivot_ptr lns_150 = &(pivots_150[num_150 - (lt_piv? 2: 1)]);
    jl_pivot_ptr lns_200 = &(pivots_200[num_200 - (lt_piv? 2: 1)]);
    bool lt_200 = jl_same_pivot(lns_200, &(pivots[num - piv_ix]));
    if (piv_ix == 3)
        lt_200 = lt_200 &&
            (lns_200->state == ((direction > 0)? UPTREND: DOWNTREND));
    int lt_factor = lt_200? 200: 150;
    int vd = pivots[num - 2].obv - pivots[num - 4].obv;
    int lt_vd = vd;
    if (!lt_piv)
        lt_vd = lt_200? (lns_200->obv - pivots_200[num_200 - 3].obv):
            (lns_150->obv - pivots_150[num_150 - 3].obv);
    cJSON *info = cJSON_CreateObject();
    cJSON_AddNumberToObject(info, "obv", pivots[num - 4].obv);
    cJSON_AddNumberToObject(info, "vd", vd);
    cJSON_AddNumberToObject(info, "s1", pivots[num - 2].state);
    cJSON_AddNumberToObject(info, "s2", pivots[num - 4].state);
    cJSON_AddNumberToObject(info, "lf", lt_factor);
    cJSON_AddNumberToObject(info, "ls",
                            (lt_200? lns_200->state: lns_150->state));
    cJSON_AddNumberToObject(info, "lls", lns_200->state);
    cJSON_AddNumberToObject(info, "lvd", lt_vd);
    ana_add_to_setups(setups, jl, "JL_P", direction, info, true);
}

/** Check whether a given day creates a new pivot point, and determine
 * if that pivot point represents a change in trend.
 */
void ana_check_for_pullbacks(cJSON *setups, jl_data_ptr jl, jl_piv_ptr pivs,
                             jl_piv_ptr pivs_150, jl_piv_ptr pivs_200) {
    int i = jl->data->pos - 1, num = pivs->num;
    jl_pivot_ptr pivots = pivs->pivots;
    daily_record_ptr r = &(jl->data->data[i]);
    jl_record_ptr jlr = &(jl->recs[i]), jlr_1 = &(jl->recs[i - 1]);
    /** Return if the current record is not a primary record */
    if (strcmp(pivots[num - 1].date, r->date))
        return;
    /** Return if the current record is not the first record in a new trend */
    int last_ns = pivots[num - 1].state;
    int prev_ns = jl_prev_ns(jl);
    if ((jl_up(last_ns) && jl_up(prev_ns)) ||
        (jl_down(last_ns) && jl_down(prev_ns)))
        return;
    jl_pivot_ptr lns_150 = &(pivs_150->pivots[pivs_150->num - 1]);
    jl_pivot_ptr lns_200 = &(pivs_200->pivots[pivs_200->num - 1]);
    /** Record pullback if short-term pivots have changed from DOWNTREND to
     * REACTION, DOWNTREND pivot matches last primary record from a long-term
     * trend, and there are no volume anomalies.
    */
    if (jl_up(last_ns) &&
        ((pivots[num - 2].state == REACTION) &&
         (pivots[num - 4].state == DOWNTREND) &&
        (jl_same_pivot(lns_150, &(pivots[num - 4])) ||
         jl_same_pivot(lns_200, &(pivots[num - 4]))) &&
         (pivots[num - 2].obv < pivots[num - 4].obv + 5)))
        ana_add_jl_pullback_setup(setups, jl, 1, 4, false, pivs, pivs_150,
                                  pivs_200);
    /** Record pullback for the down direction, when short-term pivots change
     * from UPTREND to RALLY, and under same conditions as above.
    */
    if (jl_down(last_ns) &&
        (((pivots[num - 2].state == RALLY) &&
         (pivots[num - 4].state == UPTREND) &&
        (jl_same_pivot(lns_150, &(pivots[num - 4])) ||
         jl_same_pivot(lns_200, &(pivots[num - 4]))) &&
         (pivots[num - 2].obv > pivots[num - 4].obv + 5))))
        ana_add_jl_pullback_setup(setups, jl, -1, 4, false, pivs, pivs_150,
                                  pivs_200);
    /** Record pullback if the stock is in a long-term uptrend, and it gets in
     * a short-term pullback (REACTION), but the volume is still ok, and the
     * up pivot coincides with the last long-term primary record.
     */
    if ((((lns_200->state == UPTREND) &&
          (jl_same_pivot(&(pivots[num - 3]), lns_200) ||
          jl_same_pivot(&(pivots[num - 1]), lns_200))) ||
          ((lns_150->state == UPTREND) &&
          (jl_same_pivot(&(pivots[num - 3]), lns_150) ||
          jl_same_pivot(&(pivots[num - 1]), lns_150)))) &&
        (pivots[num - 2].state == REACTION) &&
        (pivots[num - 2].obv < pivots[num - 4].obv + 5))
        ana_add_jl_pullback_setup(setups, jl, 1, 3, false, pivs, pivs_150,
                                  pivs_200);
    /** Record pullback if the stock is in a long-term downtrend, and it gets in
     * a short-term pullback (RALLY), but the volume is still ok, and the down
     * pivot coincides with the last long-term primary record.
     */
    if ((((lns_200->state == DOWNTREND) &&
          (jl_same_pivot(&(pivots[num - 3]), lns_200) ||
          jl_same_pivot(&(pivots[num - 1]), lns_200))) ||
          ((lns_150->state == DOWNTREND) &&
          (jl_same_pivot(&(pivots[num - 3]), lns_150) ||
          jl_same_pivot(&(pivots[num - 1]), lns_150)))) &&
        (pivots[num - 2].state == RALLY) &&
        (pivots[num - 2].obv > pivots[num - 4].obv - 5))
        ana_add_jl_pullback_setup(setups, jl, -1, 3, false, pivs, pivs_150,
                                  pivs_200);
    /** Record pullback if short-term pivots have changed from DOWNTREND to
     * REACTION, DOWNTREND pivot matches last pivot from a long-term
     * trend, and there are no volume anomalies.
     */
    jl_pivot_ptr p1_150 = &(pivs_150->pivots[pivs_150->num - 2]);
    jl_pivot_ptr p1_200 = &(pivs_200->pivots[pivs_200->num - 2]);
    if (jl_up(last_ns) &&
        (((pivots[num - 2].state == REACTION) &&
         (pivots[num - 4].state == DOWNTREND) &&
        (jl_same_pivot(p1_150, &(pivots[num - 4])) ||
         jl_same_pivot(p1_200, &(pivots[num - 4]))) &&
         (pivots[num - 2].obv < pivots[num - 4].obv - 5))))
        ana_add_jl_pullback_setup(setups, jl, 1, 4, true, pivs, pivs_150,
                                  pivs_200);
    if (jl_down(last_ns) &&
        (((pivots[num - 2].state == RALLY) &&
         (pivots[num - 4].state == UPTREND) &&
        (jl_same_pivot(p1_150, &(pivots[num - 4])) ||
         jl_same_pivot(p1_200, &(pivots[num - 4]))) &&
         (pivots[num - 2].obv > pivots[num - 4].obv + 5))))
        ana_add_jl_pullback_setup(setups, jl, -1, 4, true, pivs, pivs_150,
                                  pivs_200);

}

/** Check whether the action on a given day stops at a
 * resistance/support point, or whether it pierces that
 * resistance/support (on high volume), and whether it recovers after
 * piercing or not
 */ 
void ana_check_for_support_resistance(cJSON *setups, jl_data_ptr jl,
                                      jl_piv_ptr pivs) {
    int i = jl->data->pos - 1, num_pivots = pivs->num;
    jl_pivot_ptr pivots = pivs->pivots;
    daily_record_ptr r = &(jl->data->data[i]);
    jl_record_ptr jlr = &(jl->recs[i]);
    int jlr_volume = (jlr->volume == 0)? 1: jlr->volume;
    if (jl_primary(jlr->state)) {
        for(int ix = 0; ix < num_pivots - 1; ix++) {
            if (abs(jlr->price - pivots[ix].price) < jlr->rg / 5) {
                cJSON* info = cJSON_CreateObject();
                int dir = jl_up(jlr->state)? -1: 1;
                cJSON_AddNumberToObject(info, "sr", pivots[ix].price);
                cJSON_AddNumberToObject(info, "vr",
                                        100 * r->volume / jlr_volume);
                ana_add_to_setups(setups, jl, "JL_SR", dir, info, true);
            }
        }
    }
    if (jl_primary(jlr->state2)) {
        for(int ix = 0; ix < num_pivots; ix++) {
            if (abs(jlr->price2 - pivots[ix].price) < jlr->rg / 5) {
                cJSON* info = cJSON_CreateObject();
                int dir = jl_up(jlr->state2)? -1: 1;
                cJSON_AddNumberToObject(info, "sr", pivots[ix].price);
                cJSON_AddNumberToObject(info, "vr",
                                        100 * r->volume / jlr_volume);
                ana_add_to_setups(setups, jl, "JL_SR", dir, info, true);
            }
        }
    }
}

void ana_add_candle_setup(cJSON *candles, char* stp_name, int direction) {
    cJSON *stp = cJSON_CreateObject();
    cJSON_AddStringToObject(stp, "stp", stp_name);
    cJSON_AddNumberToObject(stp, "dir", direction);
    cJSON_AddItemToArray(candles, stp);
}

void ana_insert_candle_setup(char* stk, char* dt, char* stp_name, int dir) {
    char sql_cmd[1024];
        sprintf(sql_cmd, "insert into setups values ('%s','%s','%s','%c',"
                "TRUE)", dt, stk, stp_name, (dir == 1)? 'U': 'D') ;
        db_transaction(sql_cmd);

}

void ana_insert_setups_in_database(cJSON *setups, char *dt, char *stk) {
    int num_setups = cJSON_GetArraySize(setups);
    if (num_setups > 0) {
        /* LOGINFO("Inserting %d setups for %s on %s\n", num_setups, stk, dt); */
        cJSON* setup;
        cJSON_ArrayForEach(setup, setups) {
            cJSON *info = cJSON_GetObjectItem(setup, "info");
            char *info_string = (info != NULL)? cJSON_Print(info): "{}";
            char sql_cmd[2048];
            sprintf(sql_cmd, "insert into jl_setups values ('%s','%s','%s',%d,"
                    "'%s',%s,%d,'%s')", dt, stk,
                    cJSON_GetObjectItem(setup, "setup")->valuestring,
                    cJSON_GetObjectItem(setup, "factor")->valueint,
                    cJSON_GetObjectItem(setup, "direction")->valuestring,
                    cJSON_GetObjectItem(setup, "triggered")->valuestring,
                    cJSON_GetObjectItem(setup, "score")->valueint,
                    info_string);
            db_transaction(sql_cmd);
        }
    }
}

void ana_daily_setups(jl_data_ptr jl) {
    cJSON *setups = cJSON_CreateArray();
    daily_record_ptr r[2];
    jl_record_ptr jlr[2];
    int ix_0 = jl->data->pos - 1;
    for(int ix = 0; ix < 2; ix++) {
        r[ix] = &(jl->data->data[ix_0 - ix]);
        jlr[ix] = &(jl->recs[ix_0 - ix]);
    }
    int jlr_1_volume = (jlr[1]->volume == 0)? 1: jlr[1]->volume;
    int jlr_1_rg = (jlr[1]->rg == 0)? 1: jlr[1]->rg;
    char *stk = jl->data->stk, *dt = r[0]->date;
    /* Find strong closes up or down; rr/vr capture range/volume significance */
    int sc_dir = ts_strong_close(r[0]);
    if (sc_dir != 0) {
        int rr = 100 * ts_true_range(jl->data, ix_0) / jlr_1_rg;
        if (((sc_dir == -1) &&
             (r[0]->close > ts_weighted_price(jl->data, ix_0 - 1))) ||
            ((sc_dir == 1) &&
             (r[0]->close < ts_weighted_price(jl->data, ix_0 - 1))))
            rr = 0;
        cJSON *info = cJSON_CreateObject();
        cJSON_AddNumberToObject(info, "vr",
                                100 * r[0]->volume / jlr_1_volume);
        cJSON_AddNumberToObject(info, "rr", rr);
        ana_add_to_setups(setups, NULL, "SC", sc_dir, info, true);
    }
    /* Find gaps */
    int gap_dir = 0;
    if (r[0]->open > r[1]->high)
        gap_dir = 1;
    if (r[0]->open < r[1]->low)
        gap_dir = -1;
    if (gap_dir != 0) {
        int drawdown = 0;
        if (gap_dir == 1)
            drawdown = (r[0]->close - r[0]->high);
        else
            drawdown = (r[0]->close - r[0]->low);
        cJSON *info = cJSON_CreateObject();
        cJSON_AddNumberToObject(info, "vr",
                                100 * r[0]->volume / jlr_1_volume);
        cJSON_AddNumberToObject(info, "eod_gain",
                                100 * (r[0]->close - r[1]->close) /
                                jlr_1_rg);
        cJSON_AddNumberToObject(info, "drawdown", 100 * drawdown / jlr_1_rg);
        ana_add_to_setups(setups, NULL, "Gap", gap_dir, info, true);
    }
    /* Find reversal days */
    int rd_dir = 0, min_oc = MIN(r[0]->open, r[1]->close);
    int max_oc = MAX(r[0]->open, r[1]->close);
    if ((r[0]->low < r[1]->low) && (r[0]->low < min_oc - jlr[1]->rg) &&
        (r[0]->close > r[0]->open) && (sc_dir == 1))
        rd_dir = 1;
    if ((r[0]->high > r[1]->high) && (r[0]->high > max_oc + jlr[1]->rg) &&
        (r[0]->close < r[0]->open) && (sc_dir == -1))
        rd_dir = -1;
    if (rd_dir != 0) {
        cJSON *info = cJSON_CreateObject();
        cJSON_AddNumberToObject(info, "vr",
                                100 * r[0]->volume / jlr_1_volume);
        int rd_drawdown = (rd_dir == 1)? (min_oc - r[0]->low):
            (r[0]->high - max_oc);
        cJSON_AddNumberToObject(info, "rd_drawdown",
                                100 * rd_drawdown / jlr_1_rg);
        cJSON_AddNumberToObject(info, "rd_gain",
                                100 * (r[0]->close - r[0]->open) / jlr_1_rg);
        ana_add_to_setups(setups, NULL, "RDay", rd_dir, info, true);
    }
    ana_insert_setups_in_database(setups, dt, stk);
}

/** Implement these candlestick patterns:
 - hammer
 - engulfing
 - piercing/dark cloud cover/kicking
 - harami
 - star
 - engulfing harami
 - reversal day
 */
void ana_candlesticks(jl_data_ptr jl) {
    daily_record_ptr r[6];
    cJSON *info = cJSON_CreateObject(), *setups = cJSON_CreateArray();
    int ix_0 = jl->data->pos - 1;
    for(int ix = 0; ix < 6; ix++)
        r[ix] = &(jl->data->data[ix_0 - ix]);
    int body[6], max_oc[6], min_oc[6];
    for(int ix = 0; ix < 6; ix++) {
        body[ix] = r[ix]->close - r[ix]->open;
        max_oc[ix] = MAX(r[ix]->open, r[ix]->close);
        min_oc[ix] = MIN(r[ix]->open, r[ix]->close);
    }
    int marubozu[6], engulfing[2], harami[5], piercing = 0, star = 0, cbs = 0;
    int three = 0, three_in = 0, three_out = 0, kicking = 0, eng_harami = 0;
    /** Calculate marubozu and harami patterns for the last 6 (5) days */
    for(int ix = 0; ix < 6; ix++) {
        /** Handle the case when open == high == low == close */
        int h_l = r[ix]->high - r[ix]->low;
        if (h_l == 0)
            h_l = 1;
        int ratio = 100 * body[ix] / h_l;
        marubozu[ix] = (abs(ratio) < CANDLESTICK_MARUBOZU_RATIO)? 0: ratio;
        if (ix >= 5)
            continue;
        if ((100 * abs(body[ix + 1]) > jl->recs[ix_0 - ix - 2].rg *
             CANDLESTICK_LONG_DAY_AVG_RATIO) &&
            (100 * body[ix] <= CANDLESTICK_HARAMI_RATIO * body[ix + 1]) &&
            (max_oc[ix] < max_oc[ix + 1]) && (min_oc[ix] > min_oc[ix + 1]))
            harami[ix] = 1;
        else
            harami[ix] = 0;
    }
    /** Calculate engulfing pattern for the last two days */
    for(int ix = 0; ix < 2; ix++) {
        if ((body[ix] * body[ix + 1] < 0) &&
            (abs(body[ix]) > abs(body[ix + 1])) &&
            (max_oc[ix] >= max_oc[ix + 1]) &&
            (min_oc[ix] <= min_oc[ix + 1]))
            engulfing[ix] = (body[ix] > 0)? 1: -1;
        else
            engulfing[ix] = 0;
    }
    /** Calculate piercing pattern */
    if ((body[0] * body[1] < 0) &&
        (100 * abs(body[1]) > jl->recs[ix_0 - 2].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO) &&
        (100 * abs(body[0]) > jl->recs[ix_0 - 1].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO)) {
        if ((body[0] > 0) && (r[0]->open < r[1]->low) &&
            (2 * r[0]->close > (r[1]->low + r[1]->high)))
            piercing = 1;
        if ((body[0] < 0) && (r[0]->open > r[1]->high) &&
            (2 * r[0]->close < (r[1]->low + r[1]->high)))
            piercing = -1;
    }
    /** Check for star patterns. Rules of Recognition
     * 1. First day always the color established by ensuing trend
     * 2. Second day always gapped from body of first day. Color not
     *    important.
     * 3. Third day always opposite color of first day.
     * 4. First day, maybe the third day, are long days.
     *
     * If third day closes deeply (more than halfway) into first day
     * body, a much stronger move should ensue, especially if heavy
     * volume occurs on third day.
     */
    if ((100 * abs(body[2]) > jl->recs[ix_0 - 3].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO) && (body[0] * body[2] < 0) &&
        (100 * abs(body[1]) < jl->recs[ix_0 - 2].rg *
         CANDLESTICK_SHORT_DAY_AVG_RATIO)) {
        int max_r1oc = MAX(r[1]->open, r[1]->close);
        int min_r1oc = MIN(r[1]->open, r[1]->close);
        if ((body[2] < 0) && (max_r1oc < r[2]->close) &&
            (max_r1oc < r[0]->open) &&
            (2 * r[0]->close > r[2]->open + r[2]->close))
            star = 1;
        if ((body[2] > 0) && (min_r1oc > r[2]->close) &&
            (min_r1oc > r[0]->open) &&
            (2 * r[0]->close < r[2]->open + r[2]->close))
            star = -1;
    }
    /** Three (white soldiers / black crows). 
        1. Three consecutive long white days occur, each with a higher close.
        2. Each day opens within the body of the previous day.
        3. Each day closes at or near its high.
        1. Three consecutive long black days occur, each with a lower close.
        2. Each day opens within the body of the previous day.
        3. Each day closes at or near its lows.
     */
    if (((body[2] > 0 && body[1] > 0 && body[0] > 0) ||
         (body[2] < 0 && body[1] < 0 && body[0] < 0)) &&
        (100 * abs(body[2]) > jl->recs[ix_0 - 3].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO) &&
        (100 * abs(body[1]) > jl->recs[ix_0 - 2].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO) &&
        (100 * abs(body[0]) > jl->recs[ix_0 - 1].rg *
         CANDLESTICK_LONG_DAY_AVG_RATIO)) {
        if ((r[1]->close > r[0]->close) && (r[2]->close > r[1]->close) &&
            (r[1]->open <= r[0]->close) && (r[2]->open <= r[1]->close) &&
            (4 * r[0]->close > 3 * r[0]->high + r[0]->low) &&
            (4 * r[1]->close > 3 * r[1]->high + r[1]->low) &&
            (4 * r[2]->close > 3 * r[2]->high + r[2]->low))
            three = 1;
        if ((r[1]->close < r[0]->close) && (r[2]->close < r[1]->close) &&
            (r[1]->open >= r[0]->close) && (r[2]->open >= r[1]->close) &&
            (4 * r[0]->close < 3 * r[0]->low + r[0]->high) &&
            (4 * r[1]->close < 3 * r[1]->low + r[1]->high) &&
            (4 * r[2]->close < 3 * r[2]->low + r[2]->high))
            three = -1;
    }
    /** Kicking:
     * 1. A Marubozu of one color is followed by another of opposite color
     * 2. A gap must occur between the two lines.
     */
    if (marubozu[1] * marubozu[0] < 0) {
        if ((marubozu[0] > 0) && (r[0]->open > r[1]->open))
            kicking = 1;
        if ((marubozu[0] < 0) && (r[0]->open < r[1]->open))
            kicking = -1;
    }
    /** CBS:
     * 1. First two days are Black Marubozu
     * 2. Third day is black, gaps down at open, pierces previous day body.
     * 3. Fourth black day completely engulfs third day, including shadow.
     */
    if ((marubozu[3] < 0) && (marubozu[2] < 0) && (body[1] < 0) &&
        (r[1]->open < r[2]->close) && (body[0] < 0) &&
        (r[0]->open > r[1]->high) && (r[0]->close < r[1]->low))
        cbs = 1;

    /** 3in, 3out, eng harami */
    if ((engulfing[1] > 0) && (body[0] > 0) && (r[0]->close > r[1]->close))
        three_out = 1;
    if ((engulfing[1] < 0) && (body[0] < 0) && (r[0]->close < r[1]->close))
        three_out = -1;

    /** The bearish engulfing harami pattern consists of two
     * combination patterns. The first is a harami pattern and the
     * second is an engulfing pattern. The end result is a pattern
     * whose two sides are black marubozu candlesticks.
     */
    int min_40o = MIN(r[4]->open, r[0]->open);
    int min_40c = MIN(r[4]->close, r[0]->close);
    int max_40o = MAX(r[4]->open, r[0]->open);
    int max_40c = MAX(r[4]->close, r[0]->close);
    if ((harami[2] == 1) && (body[3] > 0) && engulfing[0] == 1)
        eng_harami = 1;
    if ((harami[3] == 1) && (body[4] > 0) && (engulfing[0] == 1) &&
        (max_oc[2] < max_40c) && (min_oc[2] > min_40o))
        eng_harami = 1;
    if ((harami[2] == 1) && (body[3] < 0) && engulfing[0] == -1)
        eng_harami = -1;
    if ((harami[3] == 1) && (body[4] < 0) && (engulfing[0] == -1) &&
        (max_oc[2] < max_40o) && (min_oc[2] > min_40c))
        eng_harami = -1;

    char *stk = jl->data->stk, *dt = r[0]->date;
    if (engulfing[0] != 0)
        ana_add_to_setups(setups, NULL, "Engulfing", engulfing[0], NULL, true);
    if (piercing != 0)
        ana_add_to_setups(setups, NULL, "Piercing", piercing, NULL, true);
    if (star != 0)
        ana_add_to_setups(setups, NULL, "Star", star, NULL, true);
    if (cbs != 0)
        ana_add_to_setups(setups, NULL, "Cbs", cbs, NULL, true);
    if (three != 0)
        ana_add_to_setups(setups, NULL, "3", three, NULL, true);
    if (three_in != 0)
        ana_add_to_setups(setups, NULL, "3in", three_in, NULL, true);
    if (three_out != 0)
        ana_add_to_setups(setups, NULL, "3out", three_out, NULL, true);
    if (kicking != 0)
        ana_add_to_setups(setups, NULL, "Kicking", kicking, NULL, true);
    if (eng_harami != 0)
        ana_add_to_setups(setups, NULL, "EngHarami", eng_harami, NULL, true);
    ana_insert_setups_in_database(setups, dt, stk);
}

int ana_jl_setups(char* stk, char* dt) {
    int res = 0;
    jl_data_ptr jl_050 = ana_get_jl(stk, dt, JL_050, JLF_050);
    jl_data_ptr jl_100 = ana_get_jl(stk, dt, JL_100, JLF_100);
    jl_data_ptr jl_150 = ana_get_jl(stk, dt, JL_150, JLF_150);
    jl_data_ptr jl_200 = ana_get_jl(stk, dt, JL_200, JLF_200);
    if ((jl_050 == NULL) || (jl_100 == NULL) || (jl_150 == NULL) ||
        (jl_200 == NULL))
        return -1;
    cJSON *setups = cJSON_CreateArray();
    jl_piv_ptr pivots_050 = NULL, pivots_100 = NULL, pivots_150 = NULL,
        pivots_200 = NULL;
    pivots_150 = jl_get_pivots(jl_150, 4);
    pivots_200 = jl_get_pivots(jl_200, 4);
    if ((pivots_150->num < 5) || (pivots_200->num < 5)) {
        /* LOGERROR("Got %d %s pivots, needed 5\n", pivots_150->num, JL_150); */
        /* LOGERROR("Got %d %s pivots, needed 5\n", pivots_200->num, JL_200); */
        goto end;
    }
    char *lrdt_150 = pivots_150->pivots[pivots_150->num - 3].date,
        *lrdt_200 = pivots_200->pivots[pivots_200->num - 3].date;
    char *lrdt = (strcmp(lrdt_150, lrdt_200) >= 0)? lrdt_200: lrdt_150;
    pivots_100 = jl_get_pivots_date(jl_100, lrdt);
    if (pivots_100->num < 5) {
        free(pivots_100->pivots);
        free(pivots_100);
        pivots_100 = NULL;
        pivots_100 = jl_get_pivots(jl_100, 4);
    }
    pivots_050 = jl_get_pivots_date(jl_050, lrdt);
    if (pivots_050->num < 5) {
        free(pivots_050->pivots);
        free(pivots_050);
        pivots_050 = NULL;
        pivots_050 = jl_get_pivots(jl_050, 4);
    }
    int ls_050 = jl_050->last->state;
    ana_check_for_breaks(setups, jl_050, pivots_050, ls_050);
    ana_check_for_breaks(setups, jl_100, pivots_100, ls_050);
    ana_check_for_breaks(setups, jl_150, pivots_150, ls_050);
    ana_check_for_breaks(setups, jl_200, pivots_200, ls_050);
    ana_candlesticks(jl_050);
    ana_daily_setups(jl_050);
    ana_check_for_pullbacks(setups, jl_050, pivots_050, pivots_150, pivots_200);
    ana_check_for_pullbacks(setups, jl_100, pivots_100, pivots_150, pivots_200);
    /* ana_check_for_support_resistance(setups, jl_100, pivots_100); */
    /* ana_check_for_support_resistance(setups, jl_050, pivots_050, num_050); */
    ana_insert_setups_in_database(setups, dt, stk);
 end:
    if (pivots_050 != NULL)
        free(pivots_050);
    if (pivots_100 != NULL)
        free(pivots_100);
    if (pivots_150 != NULL)
        free(pivots_150);
    if (pivots_200 != NULL)
        free(pivots_200);
    cJSON_Delete(setups);
    ana_update_score(stk, dt);
    return 0;
}

void get_quotes(cJSON *leaders, char *dt, char *exp_date, char *exp_date2,
                bool eod) {
    char *filename = "/tmp/intraday.csv", *opt_filename = "/tmp/options.csv";
    cJSON *ldr;
    int num = 0, total = cJSON_GetArraySize(leaders);
    FILE *fp = NULL;
    if ((fp = fopen(filename, "w")) == NULL) {
        LOGERROR("Failed to open file %s for writing\n", filename);
        return;
    }
    curl_global_init(CURL_GLOBAL_ALL);
    cJSON_ArrayForEach(ldr, leaders) {
        if (cJSON_IsString(ldr) && (ldr->valuestring != NULL)) {
            net_get_eod_data(fp, ldr->valuestring, dt);
            if (eod == true) {
                FILE *opt_fp = fopen(opt_filename, "w");
                if (opt_fp == NULL)
                    LOGERROR("Failed to open %s file", opt_filename);
                else {
                    net_get_option_data(NULL, opt_fp, ldr->valuestring, dt, 
                                        exp_date, cal_long_expiry(exp_date));
                    net_get_option_data(NULL, opt_fp, ldr->valuestring, dt, 
                                        exp_date2, cal_long_expiry(exp_date2));
                    fclose(opt_fp);
                    db_upload_file("options", opt_filename);
                }
            }
        }
        num++;
        if (num % 100 == 0)
            LOGINFO("%s: got quote for %4d / %4d leaders\n", dt, num, total);
    }
    LOGINFO("%s: got quote for %4d / %4d leaders\n", dt, num, total);
    fclose(fp);
    char sql_cmd[256];
    sprintf(sql_cmd, "DELETE FROM eods WHERE dt='%s' AND oi=1", dt);
    db_transaction(sql_cmd);
    db_upload_file("eods", filename);
    fp = NULL;
    curl_global_cleanup();
}

void ana_daily_analysis(char* dt, bool eod, bool download_data) {
    /** this can run during the trading day (eod is false), or at eod
     * 1. Download price data only for option spread leaders
     * 2. Determine which EOD setups were triggered today
     * 3. Calculate intraday setups
     **/
    char *exp_date, *exp_date2;
    int exp_ix = cal_expiry(cal_ix(dt) + (eod? 1: 0), &exp_date);
    cal_expiry(exp_ix + 1, &exp_date2);
    cJSON *ldr = NULL, *leaders = ana_get_leaders(exp_date, MAX_ATM_PRICE,
                                                  MAX_OPT_SPREAD, 0);
    char sql_cmd[256];
    sprintf(sql_cmd, "DELETE FROM setups WHERE dt='%s' AND setup IN "
            "('GAP', 'GAP_HV', 'STRONG_CLOSE')", dt);
    db_transaction(sql_cmd);
    int num = 0, total = cJSON_GetArraySize(leaders);
    if (download_data)
        get_quotes(leaders, dt, exp_date, exp_date2, eod);
    FILE *fp = NULL;
    char *filename = "/tmp/setups.csv";
    if ((fp = fopen(filename, "w")) == NULL) {
        LOGERROR("Failed to open file %s for writing\n", filename);
        fp = stderr;
    }
    num = 0;
    char* next_dt = NULL;
    if (eod == true)
        cal_next_bday(cal_ix(dt), &next_dt);
    cJSON_ArrayForEach(ldr, leaders) {
        if (cJSON_IsString(ldr) && (ldr->valuestring != NULL))
            ana_setups(fp, ldr->valuestring, dt, next_dt, eod);
        num++;
        if (num % 100 == 0)
            LOGINFO("%s: analyzed %4d / %4d leaders\n", dt, num, total);
    }
    LOGINFO("%s: analyzed %4d / %4d leaders\n", dt, num, total);
    fclose(fp);
    LOGINFO("Closed fp\n");
    if((fp = fopen(filename, "r")) == NULL) {
        LOGERROR("Failed to open file %s\n", filename);
    } else {
        char line[80], stp_dir, stp_dt[16], stp[16], stp_stk[16];
        int triggered, num_triggered = 0, num_untriggered = 0;
        while(fgets(line, 80, fp)) {
            sscanf(line, "%s\t%s\t%s\t%c\t%d\n", &stp_dt[0], &stp_stk[0],
                   &stp[0], &stp_dir, &triggered);
            char *trigger_str = triggered? "true": "false";
            sprintf(sql_cmd, "insert into setups values "
                    "('%s','%s','%s','%c',%s) on conflict on constraint "
                    "setups_pkey do update set triggered=%s", 
                    stp_dt, stp_stk, stp, stp_dir, trigger_str, trigger_str);
            db_transaction(sql_cmd);
            if (triggered == 1) 
                num_triggered++;
            else
                num_untriggered++;
        }
        LOGINFO("%s: inserted %d triggered setups\n", dt, num_triggered);
        LOGINFO("%s: inserted %d not-triggered setups\n", next_dt, 
                num_untriggered);
        fclose(fp);
    }
    cJSON_Delete(leaders);
}

void score_leader_setups(char* stk, char* ana_date, char *tag_name) {
    /** calculate setups for a single stock (stk) up to ana_date. If ana_date
     * is NULL, setups (and their scores) will be calculated up to the current
     * business date. If clear_db is set to true, this funtion will clear all
     * the setup info in the DB, and will recalculate the setups for that stock.
     */
    char sql_cmd[256];
    char *setup_date = NULL;
    sprintf(sql_cmd, "SELECT max(dt) FROM score_setups WHERE stk='%s' AND "
            "tag='%s'", stk, tag_name);
    PGresult* res = db_query(sql_cmd);
    int rows = PQntuples(res);
    if (rows == 0) {
        /** Could not find a previous analysis date in the DB, will start
         * from the first date from which EOD is available for the stock
         */
        PQclear(res);
        sprintf(sql_cmd, "SELECT min(dt) FROM eods WHERE stk='%s'", stk);
        res = db_query(sql_cmd);
        rows = PQntuples(res);
        if (rows == 0) {
            PQclear(res);
            LOGERROR("Could not find EOD data for stock %s, exit\n", stk);
            return;
        }
        setup_date = PQgetvalue(res, 0, 0);
        cal_move_bdays(setup_date, 45, &setup_date);
        char *setup_date_1 = NULL;
        cal_move_bdays(ana_date, -252, &setup_date_1);
        if (strcmp(setup_date, setup_date_1) < 0)
            setup_date = setup_date_1;
    } else {
        /** Found last setup analysis date in DB. Start analysis from the next
         * business day.
        */
        setup_date = PQgetvalue(res, 0, 0);
        cal_next_bday(cal_ix(setup_date), &setup_date);
    }
    PQclear(res);
    int ana_res = 0;
    while((ana_res == 0) && (strcmp(setup_date, ana_date) <= 0)) {
        // if (!strcmp(stk, "XOM") && !strcmp(setup_date, "2002-02-06"))
        //     printf("Break\n");
        ana_res = ana_jl_setups(stk, setup_date);
        if (ana_res == 0)
            cal_next_bday(cal_ix(setup_date), &setup_date);
    }
    cal_prev_bday(cal_ix(setup_date), &setup_date);
    memset(sql_cmd, 0, 256 * sizeof(char));
    sprintf(sql_cmd, "INSERT INTO setup_dates VALUES ('%s', '%s') ON CONFLICT"
            " (stk) DO UPDATE SET dt='%s'", stk, setup_date, setup_date);
    db_transaction(sql_cmd);
}

int score_clip(int value, int lb, int ub) {
    int res = value;
    if (res < lb)
        res = lb;
    if (res > ub)
        res = ub;
    return res;
}

int score_sc_setup(cJSON *setup_json, int dir, char *tag_name) {
    int vr = score_clip(cJSON_GetObjectItem(setup_json, "vr")->valueint, 50, 250);
    int rr = score_clip(cJSON_GetObjectItem(setup_json, "rr")->valueint, 50, 250);
    return (vr - 50 + rr - 50) * dir;
}

int score_gap_setup(cJSON *setup_json, int dir, char *tag_name) {
    int vr = cJSON_GetObjectItem(setup_json, "vr")->valueint;
    int eod_gain = cJSON_GetObjectItem(setup_json, "eod_gain")->valueint;
    int drawdown = cJSON_GetObjectItem(setup_json, "drawdown")->valueint;
    return vr * (eod_gain + drawdown) / 150;
}

int score_rday_setup(cJSON *setup_json, int dir, char *tag_name) {
    int vr = cJSON_GetObjectItem(setup_json, "vr")->valueint;
    int rd_gain = cJSON_GetObjectItem(setup_json, "rd_gain")->valueint;
    int rd_drawdown = cJSON_GetObjectItem(setup_json, "rd_drawdown")->valueint;
    int rdr = ana_clip(abs(rd_gain) + abs(rd_drawdown), 0, 300);
    return dir * vr * rdr / 150;
}

int score_jlpullback_setup(cJSON *setup_json, int dir, char *tag_name) {
    int ls = cJSON_GetObjectItem(setup_json, "ls")->valueint;
    int lvd = cJSON_GetObjectItem(setup_json, "lvd")->valueint;
    int obv = cJSON_GetObjectItem(setup_json, "obv")->valueint;
    int score = 5 * obv;
    if (((dir == 1) && (ls == UPTREND)) ||
        ((dir == -1) && (ls == DOWNTREND)))
        score += (dir * 50);
    else if ((ls == RALLY) || (ls == REACTION))
        score += (dir * 25);
    if (lvd * dir > 0)
        score -= (5 * lvd);
    else
        score -= (2 * lvd);
    if (dir * score < 0)
        score = 0;
    return score;
}

int score_jlbreakout_setup(cJSON *setup_json, int dir, char *tag_name) {
    int vr = cJSON_GetObjectItem(setup_json, "vr")->valueint;
    int len = cJSON_GetObjectItem(setup_json, "len")->valueint;
    int obv = cJSON_GetObjectItem(setup_json, "obv")->valueint;
    int last_ns = cJSON_GetObjectItem(setup_json, "last_ns")->valueint;
    int prev_ns = cJSON_GetObjectItem(setup_json, "prev_ns")->valueint;
    int score = 0;
    if (!strcmp(tag_name, "ALL") || !strcmp(tag_name, "JLB") ||
        !strcmp(tag_name, "BKO")) {
        score = 5 * obv;
        score = score * vr / 150;
        score = score * ana_clip(len, 50, 250) / 50;
        if (dir == 1) {
            if (last_ns == UPTREND) {
                if (prev_ns == UPTREND)
                    score = score / 2;
                else if (prev_ns == RALLY)
                    score = score * 2;
            }
        } else { /** dir == -1 */
            if (last_ns == DOWNTREND) {
                if (prev_ns == DOWNTREND)
                    score = score / 2;
                else if (prev_ns == REACTION)
                    score = score * 2;
            }
        }
    }
    return score;
}

int score_piercing_setup(cJSON *setup_json, int dir, char *tag_name) {
    return dir * 100;
}

int score_kicking_setup(cJSON *setup_json, int dir, char *tag_name) {
    return dir * 100;
}

int score_engharami_setup(cJSON *setup_json, int dir, char *tag_name) {
    return dir * 100;
}

int score_star_setup(cJSON *setup_json, int dir, char *tag_name) {
    return dir * 150;
}

int score_engulfing_setup(cJSON *setup_json, int dir, char *tag_name) {
    return dir * 50;
}

int score_3out_setup(cJSON *setup_json, int dir, char *tag_name) {
    return dir * 50;
}

int score_calc_setup_score(char *setup_name, int factor, int direction,
                           char *setup_str, char *tag_name) {
    cJSON *setup_json = cJSON_Parse(setup_str);
    int score = 0;
    if (!strcmp(setup_name, "SC"))
        score = score_sc_setup(setup_json, direction, tag_name);
    else if (!strcmp(setup_name, "Gap"))
        /** TODO: add more params to distinguish between breakaway and exhaustion gaps */
        score = score_gap_setup(setup_json, direction, tag_name);
    else if (!strcmp(setup_name, "RDay"))
        score = score_rday_setup(setup_json, direction, tag_name);
    else if (!strcmp(setup_name, "JL_P"))
        score = score_jlpullback_setup(setup_json, direction, tag_name);
    else if (!strcmp(setup_name, "JL_B"))
        score = score_jlbreakout_setup(setup_json, direction, tag_name);
    else if (!strcmp(setup_name, "Piercing")
        score = score_piercing_setup(setup_json, direction, tag_name);
    else if (!strcmp(setup_name, "Kicking")
        score = score_kicking_setup(setup_json, direction, tag_name);
    else if (!strcmp(setup_name, "EngHarami")
        score = score_engharami_setup(setup_json, direction, tag_name);
    else if (!strcmp(setup_name, "Star"))
        score = score_star_setup(setup_json, direction, tag_name);
    else if (!strcmp(setup_name, "Engulfing"))
        score = score_engulfing_setup(setup_json, direction, tag_name);
    else if (!strcmp(setup_name, "3out"))
        score = score_3out_setup(setup_json, direction, tag_name);
end:
    cJSON_Delete(setup_json);
    return score;
}

void score_setups(char *stk, char *s_date, char *e_date, char *tag_name) {

    char *start_date = NULL, sql_cmd[256];
    cal_move_bdays(s_date, -10, &start_date);
    sprintf(sql_cmd, "SELECT * FROM jl_setups WHERE stk='%s' AND dt BETWEEN "
            "'%s' AND '%s' ORDER BY dt", stk, start_date, e_date);
    PGresult res = db_query(sql_cmd);
    int num_stps = PQntuples(res);
    for(int ix = 0; ix < num_stps; ix++) {
        char *setup_name = PQgetvalue(res, ix, 2);
        int factor = atoi(PQgetvalue(res, ix, 3));
        int dir = (*PQgetvalue(res, ix, 4) == 'U')? 1: -1;
        char *setup_str = PQgetvalue(res, ix, 7);
        int setup_score = score_calc_setup_score(setup_name, factor, dir,
                                                 setup_str, tag_name);
    }   

        cal_move_bdays(setup_date, 45, &setup_date);
        char *setup_date_1 = NULL;
        cal_move_bdays(ana_date, -252, &setup_date_1);
        if (strcmp(setup_date, setup_date_1) < 0)
            setup_date = setup_date_1;
    } else {
        /** Found last setup analysis date in DB. Start analysis from the next
         * business day.
        */
        setup_date = PQgetvalue(res, 0, 0);
        cal_next_bday(cal_ix(setup_date), &setup_date);
    }
 
    
    int ix = cal_ix(start_date), start_ix = cal_ix(s_date), end_ix = cal_ix(e_date);
    while(ix <= end_ix) {
        if (!strcmp(crs_date, exp_bdate)) {
            exp_ix = cal_expiry(ix + 1, &exp_date);
            exp_bix = cal_exp_bday(exp_ix, &exp_bdate);
            ana_expiry_analysis(crs_date, false);
        }
        score_setups(crs_date, stx, tag_name);
        ix = cal_next_bday(ix, &crs_date);
    }


    char *exp_date, *exp_date2;
    int exp_ix = cal_expiry(cal_ix(ana_date) + (eod? 1: 0), &exp_date);
    cal_expiry(exp_ix + 1, &exp_date2);
    cJSON *ldr = NULL, *leaders = stx;
    if (leaders == NULL)
        leaders = ana_get_leaders(exp_date, MAX_ATM_PRICE, MAX_OPT_SPREAD, 0);
    char sql_cmd[256];
    cJSON_ArrayForEach(ldr, leaders) {
        if (cJSON_IsString(ldr) && (ldr->valuestring != NULL))
            score_leader_setups(ldr->valuestring, ana_date, tag_name);
        num++;
        if (num % 100 == 0)
            LOGINFO("%s: scored %4d / %4d leaders\n", ana_date, num, total);
    }
    LOGINFO("%s: scored %4d / %4d leaders\n", ana_date, num, total);
    if (stx == NULL)
       cJSON_Delete(leaders);
}

#endif