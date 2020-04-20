#include <ctype.h>
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
#include "stx_jl.h"
#include "stx_ts.h"

#define JL_200 "200"
#define JL_FACTOR 2.00

typedef struct trade_t {
    char cp;
    char stk[16];
    char und[16];
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
    char triggered;
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


jl_data_ptr trd_get_jl(char *stk, char *dt) {
    char *jl_dt = NULL;
    cal_prev_bday(cal_ix(dt), &jl_dt);
    ht_item_ptr ht_jl = ht_get(trd_jl(JL_200), stk);
    jl_data_ptr jl = NULL;
    if (ht_jl == NULL) {
        stx_data_ptr data = ts_load_stk(stk);
        if (data == NULL) {
            LOGERROR("Could not load data for %s, skipping...\n", stk);
            return NULL;
        }
        jl = jl_jl(data, jl_dt, JL_FACTOR);
        ht_jl = ht_new_data(stk, (void*)jl);
        ht_insert(trd_jl(JL_200), ht_jl);
    } else {
        jl = (jl_data_ptr) ht_jl->val.data;
        if (strcmp(jl->data->data[jl->pos].date, dt) > 0) {
            LOGINFO("%s: skipping %s, as its current date is %s\n",
                    dt, stk, jl->data->data[jl->pos].date);
            return NULL;
        }
        jl_advance(jl, jl_dt);
    }
    return jl;
}


int trd_counter_trend(jl_data_ptr jl) {
    daily_record_ptr dr = &(jl->data->data[jl->pos]);
    daily_record_ptr dr_1 = &(jl->data->data[jl->pos - 1]);
    int sc_dir = ts_strong_close(dr);
    if (sc_dir == 0)
        return 0;
    if ((dr->volume > jl->recs[jl->pos - 1].volume) ||
        (dr->volume > dr_1->volume))
        return sc_dir;
    if (dr_1->volume > jl->recs[jl->pos - 2].volume) {
        if (((sc_dir == 1) && (dr->close > dr_1->high)) ||
            ((sc_dir == -1) && (dr->close < dr_1->low)))
            return sc_dir;
    }
    return 0;
}


int trd_get_option_old(trade_ptr trd, jl_data_ptr jl) {
    char sql_cmd[256];
    sprintf(sql_cmd, "SELECT strike, bid, ask FROM options WHERE und='%s' AND "
            "dt='%s' AND expiry='%s' AND cp='%c' ORDER BY strike", trd->und,
            trd->in_dt, trd->exp_dt, trd->cp);
    PGresult *opt_res = db_query(sql_cmd);
    int rows = PQntuples(opt_res);
    trd->in_spot = jl->data->data[jl->pos].close;
    int dist, dist_1 = 1000000, strike = -1, strike_1, ask = 1000000, ask_1;
    for(int ix = 0; ix < rows; ix++) {
        strike = atoi(PQgetvalue(opt_res, ix, 0));
        ask = atoi(PQgetvalue(opt_res, ix, 2));
        dist = abs(strike - trd->in_spot);
        if (dist > dist_1) {
            trd->strike = strike_1;
            trd->in_ask = ask_1;
            break;
        }
        if (ix == rows - 1) {
            trd->strike = strike;
            trd->in_ask = ask;
        }
        strike_1 = strike;
        dist_1 = dist;
        ask_1 = ask;
    }
    PQclear(opt_res);
    if (strike == -1) {
        LOGERROR("%s: no options data for %s (%s), skipping...\n",
                 trd->in_dt, trd->und, trd->stk);
        return 0;
    }
    if (abs(trd->strike - trd->in_spot) > 2 * jl->recs[jl->pos - 1].rg) {
        LOGERROR("%s: strike %d and spot %d too far apart for %s (rg = %d), "
                 "skipping ...\n", trd->in_dt, trd->strike, trd->in_spot,
                 trd->stk, jl->recs[jl->pos - 1].rg);
        return 0;
    }
    trd->in_range = jl->recs[jl->pos - 1].rg;
    if (trd->in_ask == 0) {
        LOGERROR("%s: %s %s %c %d, ask price = 0, skipping ...\n", trd->in_dt,
            trd->stk, trd->exp_dt, trd->cp, trd->strike);
        return 0;
    }
    return 1;
}


int trd_get_option(trade_ptr trd, jl_data_ptr jl) {
    int sign = (trd->cp == 'c')? 1: -1;
    char sql_cmd[256];
    sprintf(sql_cmd, "SELECT strike, bid, ask FROM options WHERE und='%s' AND "
            "dt='%s' AND expiry='%s' AND cp='%c' ORDER BY strike %s", trd->und,
            trd->in_dt, trd->exp_dt, trd->cp, (trd->cp == 'c'? "": "DESC"));
    PGresult *opt_res = db_query(sql_cmd);
    int rows = PQntuples(opt_res), ix = 0;
    if (rows == 0) {
        LOGERROR("%s: no options data for %s (%s), skipping...\n",
                 trd->in_dt, trd->und, trd->stk);
        PQclear(opt_res);
        return 0;
    }
    trd->in_spot = jl->data->data[jl->pos].close;
    int strike = atoi(PQgetvalue(opt_res, ix, 0));
    trd->strike = strike;
    trd->in_ask = atoi(PQgetvalue(opt_res, ix, 2));
    while((ix++ < rows - 1) && (sign * strike < sign * trd->in_spot)) {
        strike = atoi(PQgetvalue(opt_res, ix, 0));
        if (sign * strike < sign * trd->in_spot) {
            trd->strike = strike;
            trd->in_ask = atoi(PQgetvalue(opt_res, ix, 2));
        }
    }
    PQclear(opt_res);
    if (sign * trd->strike >= sign * trd->in_spot) {
        LOGERROR("%s: no ITM %s strikes for %s: spot = %d, strike = %d), "
                 "skipping ...\n",trd->in_dt, ((sign == 1)? "call": "put"),
                 trd->stk, trd->in_spot, trd->strike);
        return 0;
    }
    if (abs(trd->strike - trd->in_spot) > 4 * jl->recs[jl->pos - 1].rg) {
        LOGERROR("%s: strike %d and spot %d too far apart for %s (rg = %d), "
                 "skipping ...\n", trd->in_dt, trd->strike, trd->in_spot,
                 trd->stk, jl->recs[jl->pos - 1].rg);
        return 0;
    }
    trd->in_range = jl->recs[jl->pos - 1].rg;
    if (trd->in_ask == 0) {
        LOGERROR("%s: %s %s %c %d, ask price = 0, skipping ...\n", trd->in_dt,
            trd->stk, trd->exp_dt, trd->cp, trd->strike);
        return 0;
    }
    return 1;
}

int trd_size_position(trade_ptr trd, int trd_capital) {
    trd->num_contracts = trd_capital / trd->in_ask;
    int sign = (trd->cp == 'c')? 1: -1;
    trd->moneyness = sign * (trd->in_spot - trd->strike) / trd->in_range;
    LOGINFO("%s: open trade: %d contracts %s %s %c %d\n", trd->in_dt,
            trd->num_contracts, trd->stk, trd->exp_dt, trd->cp, trd->strike);
    return sign;
}


int init_trade(trade_ptr trd, int trd_capital) {
    jl_data_ptr jl = trd_get_jl(trd->stk, trd->in_dt);
    if (jl == NULL)
        return 0;
    char *exp_date, *dt_n;
    cal_expiry_next(cal_ix(trd->in_dt), &exp_date);
    strcpy(trd->exp_dt, exp_date);
    char sql_cmd[256];
    cal_move_bdays(trd->in_dt, -8, &dt_n);
    sprintf(sql_cmd, "SELECT dt, direction FROM setups WHERE stk='%s' AND "
            "dt BETWEEN '%s' AND '%s' and setup in ('STRONG_CLOSE', 'GAP_HV')"
            " ORDER BY dt DESC", trd->stk, dt_n, trd->in_dt);
    PGresult *stp_res = db_query(sql_cmd);
    int rows = PQntuples(stp_res), stp_dir = 0;
    for(int ix = 0; ix < rows; ix++) {
        char dir = *(PQgetvalue(stp_res, ix, 1));
        stp_dir += ((dir == 'D')? -1: 1);
        if (abs(stp_dir) >= 3)
            break;
    }
    PQclear(stp_res);
    if (abs(stp_dir) < 3) {
#ifdef DEBUG
        LOGDEBUG("%s: skipping %s, because stp_dir = %d\n", trd->in_dt,
                trd->stk, stp_dir);
#endif
        return 0;
    }
    if ((stp_dir > 0) && (trd->triggered == 'f')) {
        if (trd->cp == 'c') { 
            LOGINFO("%s: skipping %s, up, because not triggered\n", 
                    trd->in_dt, trd->stk);
            return 0;
        } else {
            trd->cp = 'c';
            LOGINFO("%s: flipping the sign on %s, to up\n", trd->in_dt, 
                    trd->stk);
        }
    }
    if ((stp_dir < 0) && (trd->triggered == 'f')) {
        if (trd->cp == 'p') {
            LOGINFO("%s: skipping %s, down, because not triggered\n", 
                    trd->in_dt, trd->stk);
            return 0;
        } else {
            trd->cp = 'p';
            LOGINFO("%s: flipping the sign on %s, to down\n", trd->in_dt, 
                    trd->stk);
        }
    }
    if (trd_get_option(trd, jl) == 0)
        return 0;
    int sign = trd_size_position(trd, trd_capital);
    return sign;
}


int manage_trade(trade_ptr trd) {
    bool exit_trade = false;
    int sign = (trd->cp == 'c')? 1: -1, res = sign;
    ht_item_ptr ht_jl = ht_get(trd_jl(JL_200), trd->stk);
    jl_data_ptr jl = (jl_data_ptr) ht_jl->val.data;
    while (!exit_trade) {
        if (jl_next(jl) == -1) {
            exit_trade = true;
            jl->pos = jl->size - 1;
        } else {
            daily_record_ptr sr = jl->data->data + jl->pos, 
                sr_1 = jl->data->data + jl->pos - 1,
                sr_2 = jl->data->data + jl->pos - 2;
            if (cal_num_busdays(sr->date, trd->exp_dt) < 3)
                exit_trade = true;
            if ((sr->volume > jl->recs[jl->pos - 1].volume) &&  
                ((sign == 1 && sr->close < sr_1->close) ||
                 (sign == -1 && sr->close > sr_1->close)))
                exit_trade = true;
            if (sign * trd_counter_trend(jl) < 0)
                exit_trade = true;
            if (sign * (trd->in_spot - sr->close) > jl->recs[jl->pos - 1].rg)
                exit_trade = true;
        }
    }
    strcpy(trd->out_dt, jl->data->data[jl->pos].date);
    trd->out_spot = jl->data->data[jl->pos].close;
    trd->spot_pnl = (sign * (trd->out_spot - trd->in_spot) / trd->in_range - 1)
        / 2;
    char sql_cmd[128];
    sprintf(sql_cmd, "SELECT bid FROM options WHERE und='%s' AND expiry='%s' "
            "AND dt='%s' AND cp='%c' AND strike=%d", trd->und, trd->exp_dt, 
            trd->out_dt, trd->cp, trd->strike);
    PGresult *opt_res = db_query(sql_cmd);
    int rows = PQntuples(opt_res);
    if (rows == 0) {
        LOGERROR("%s: no out options data found for %s (%s), skipping ...\n",
                 trd->out_dt, trd->und, trd->stk);
        res = 0;
    } else {
        trd->out_bid = atoi(PQgetvalue(opt_res, 0, 0));
        trd->opt_pnl = trd->num_contracts * (trd->out_bid - trd->in_ask);
        trd->opt_pct_pnl = 100 * trd->out_bid / trd->in_ask - 100;
        LOGINFO("%s: closed trade: %d contracts %s %s %c %d\n", trd->out_dt, 
            trd->num_contracts, trd->stk, trd->exp_dt, trd->cp, trd->strike);
        LOGINFO("   in_ask=%d, out_bid=%d, opt_pnl=%d, opt_pct_pnl=%d\n",
                trd->in_ask, trd->out_bid, trd->opt_pnl, trd->opt_pct_pnl);
    }
    PQclear(opt_res);
    return res;
}


int process_trade(trade_ptr trd, int trd_capital) {
    int res = init_trade(trd, trd_capital);
    if (res == 0) 
        return 0;
    return manage_trade(trd);
}


void record_trade(FILE *fp, trade_ptr trd, char* crt_busdate) {
    fprintf(fp, "%s\t%s\t%s\t%s\t%s\t%c\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t"
            "%d\t%d\t%d\t%d\n", crt_busdate, trd->in_dt, trd->out_dt, 
            trd->stk, trd->setup, trd->cp, trd->exp_dt, trd->strike, 
            trd->in_ask, trd->out_bid, trd->opt_pnl, trd->opt_pct_pnl, 
            trd->moneyness, trd->in_spot, trd->in_range, trd->out_spot, 
            trd->spot_pnl, trd->num_contracts);
}


void trd_trade(char *tag, char *start_date, char *end_date, char *stx,
               char *setups, int trd_capital, bool triggered) {
    /**
     v * 1. Tokenize stx, setups
     v * 2. Build query that retrieves all setups
     * 3. For each query result: 
       v  a. Load stock time series, if not already there
          b. Load the option.
          c. Iterate through the time series, check exit rules
          d. When exit rules are triggered, calculate PnLs, insert trade in DB
     **/
    FILE* fp = NULL;
    char *filename = "/tmp/trades.csv";
    if((fp = fopen(filename, "w")) == NULL) {
        LOGERROR("Failed to open file %s for writing\n", filename);
        return;
    }
    char* crt_bd = cal_current_busdate(0);
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
        int ix = 0;
        char* token = strtok(setup_tokens, ",");
        while (token) {
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
        strcpy(trd.und, trd.stk);
        char *und = strchr(trd.und, '.');
        if ((und != NULL) && (strlen(und) == 7) && isdigit(und[1]))
            *und = '\0';
        strcpy(trd.setup, PQgetvalue(setup_recs, ix, 2));
        trd.cp = *(PQgetvalue(setup_recs, ix, 3));
        trd.triggered = *((char *)PQgetvalue(setup_recs, ix, 4));
        if (trd.cp == 'D')
            trd.cp = 'p';
        else
            trd.cp = 'c';
        if (ix % 100 == 0)
            LOGINFO("Analyzed %5d/%5d setups\n", ix, rows);
        if (process_trade(&trd, trd_capital) != 0)
            record_trade(fp, &trd, crt_bd);
    }
    LOGINFO("Analyzed %5d/%5d setups\n", rows, rows);
    PQclear(setup_recs);
    fclose(fp);
    db_upload_file("trades", filename);
}

cJSON* trd_get_stock_list(char *stocks) {
    cJSON *stx = NULL;
    if (stocks != NULL) {
        stx = cJSON_CreateArray();
        if (stx == NULL) {
            LOGERROR("Failed to create stx cJSON Array.\n");
            exit(-1);
        }
        cJSON *stk_name = NULL;
        char* token = strtok(stocks, ",");
        while (token) {
            stk_name = cJSON_CreateString(token);
            if (stk_name == NULL) {
                LOGERROR("Failed to create cJSON string for %s\n", token);
                continue;
            }
            cJSON_AddItemToArray(stx, stk_name);
            token = strtok(NULL, ",");
        }
    }
    return stx;
}


int process_scored_trade(trade_ptr trd, jl_data_ptr jl, int trd_capital) {
    char *exp_date;
    cal_expiry_next(cal_ix(trd->in_dt), &exp_date);
    strcpy(trd->exp_dt, exp_date);
    if (trd_get_option(trd, jl) == 0)
        return 0;
    trd_size_position(trd, trd_capital);
    return manage_trade(trd);

}

int trd_scored_daily(FILE *fp, char *tag, char *trd_date, int daily_num,
                     int max_spread, int min_score, int trd_capital,
                     cJSON *stx) {
    if (stx != NULL) {
        /** TODO: eventually provide custom stock list functionality */
        LOGERROR("Custom list of stocks not supported for now\n");
        return -1;
    }
    char *exp_date = NULL;
    cal_expiry(cal_ix(trd_date), &exp_date);
    char sql_cmd[512];
    // sprintf(sql_cmd, "SELECT * FROM setup_scores WHERE stk in (SELECT stk "
    //         "FROM leaders where expiry='%s' AND opt_spread<=%d) AND dt='%s' "
    //         "AND trigger_score != 0 AND ABS(trigger_score+trend_score)>=%d "
    //         "ORDER BY ABS(trigger_score+trend_score) DESC LIMIT %d", 
    //         exp_date, max_spread, trd_date, min_score, 3 * daily_num);
    // sprintf(sql_cmd, "SELECT * FROM setup_scores WHERE stk in (SELECT stk "
    //         "FROM leaders where expiry='%s' AND opt_spread<=%d) AND dt='%s' "
    //         "AND trigger_score != 0 AND ABS(trend_score)>=%d "
    //         "ORDER BY ABS(trend_score) DESC LIMIT %d", 
    //         exp_date, max_spread, trd_date, min_score, 3 * daily_num);
    sprintf(sql_cmd, "select * from setup_scores where dt='%s' and stk in "
            "(select stk from jl_setups where dt='%s' and setup='JL_P' and factor=100) and "
            "stk in (select stk from leaders where expiry='%s' and "
            "opt_spread < %d) order by abs(trigger_score + trend_score) desc "
            "limit %d",
            trd_date, trd_date, exp_date, max_spread, 3 * daily_num);
    PGresult *res = db_query(sql_cmd);
    int num_setups = 0, rows = PQntuples(res);
    trade trd;
    for (int ix = 0; ix < rows; ix++) {
        if (num_setups >= daily_num)
            break;
        char *stk_name = PQgetvalue(res, ix, 1);
        int trigger_score = atoi(PQgetvalue(res, ix, 2));
        int trend_score = atoi(PQgetvalue(res, ix, 3));
        LOGINFO("%s: %12s %6d %6d\n", trd_date, stk_name, trigger_score,
                trend_score);
        if (trigger_score * trend_score < 0) {
            LOGINFO("%s: skipping %s, because trigger and trend scores point "
                    "in different directions\n", trd_date, stk_name);
            continue;
        }
        jl_data_ptr jl = trd_get_jl(stk_name, trd_date);
        if (jl == NULL)
            continue;
        int daily_trend = trd_counter_trend(jl);
        if (daily_trend * trigger_score < 0)
            continue;
        memset(&trd, 0, sizeof(trade));
        strcpy(trd.in_dt, trd_date);
        strcpy(trd.stk, stk_name);
        strcpy(trd.und, trd.stk);
        char *und = strchr(trd.und, '.');
        if ((und != NULL) && (strlen(und) == 7) && isdigit(und[1]))
            *und = '\0';
        strcpy(trd.setup, "scored");
        trd.cp = (trend_score > 0)? 'c': 'p';
        trd.triggered = 't';
        if (process_scored_trade(&trd, jl, trd_capital) != 0) {
            record_trade(fp, &trd, tag);
            ++num_setups;
        }
    }
    PQclear(res);
    return 0;
}

void trade_one_stock(char* stk_name, char* trd_date, int trd_capital) {
    trade trd;
    jl_data_ptr jl = trd_get_jl(stk_name, trd_date);
    if (jl == NULL) {
        LOGERROR("Could not load JL for stock %s on date %s\n", stk_name,
                 trd_date);
        return;
    }
    memset(&trd, 0, sizeof(trade));
    strcpy(trd.in_dt, trd_date);
    strcpy(trd.stk, stk_name);
    strcpy(trd.und, trd.stk);
    char *und = strchr(trd.und, '.');
    if ((und != NULL) && (strlen(und) == 7) && isdigit(und[1]))
        *und = '\0';
    strcpy(trd.setup, "scored");
    trd.cp = (trd_capital > 0)? 'c': 'p';
    trd.triggered = 't';
    if (process_scored_trade(&trd, jl, abs(trd_capital)) != 0)
        record_trade(stderr, &trd, "");
}

void trd_trade_scored(char *tag, char *start_date, char *end_date,
                      int daily_num, int max_spread, int min_score,
                      int trd_capital, char *stocks) {
    cJSON* stx = trd_get_stock_list(stocks);
    char sql_cmd[1024], *trd_fname = "/tmp/trades.csv";
    sprintf(sql_cmd, "DELETE FROM trades WHERE tag='%s' AND in_dt BETWEEN "
            "'%s' AND '%s'", tag, start_date, end_date);
    db_transaction(sql_cmd);
    char *s_date = cal_move_to_bday(start_date, true);
    char *e_date = cal_move_to_bday(end_date, false);
    int trd_res = 0;
    FILE *trd_fp = fopen(trd_fname, "w");
    if (trd_fp == NULL) {
        LOGERROR("Failed to open %s file\n", trd_fname);
        exit(-1);
    }
    while((trd_res == 0) && (strcmp(s_date, e_date) <= 0)) {
        trd_res = trd_scored_daily(trd_fp, tag, s_date, daily_num, max_spread,
                                   min_score, trd_capital, stx);
        if (trd_res == 0)
            cal_next_bday(cal_ix(s_date), &s_date);
    }
    fclose(trd_fp);
    db_upload_file("trades", trd_fname);
    if (stx != NULL)
        cJSON_Delete(stx);
}