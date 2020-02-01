#ifndef __STX_ANA_H__
#define __STX_ANA_H__

#include <cjson/cJSON.h>
#include <curl/curl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include "stx_core.h"
#include "stx_jl.h"
#include "stx_net.h"
#include "stx_setups.h"
#include "stx_ts.h"

#define AVG_DAYS 50
#define MIN_ACT 8
#define MIN_RCR 15
#define MAX_OPT_SPREAD 33
#define MAX_ATM_PRICE 500
#define UP 'U'
#define DOWN 'D'
#define JL_FACTOR 2.00
#define JLF_050 0.50
#define JLF_100 1.0
#define JLF_150 1.50
#define JLF_200 2.00
#define JL_050 "050"
#define JL_100 "100"
#define JL_150 "150"
#define JL_200 "200"
#define MIN_CHANNEL_LEN 25

#define CANDLESTICK_MARUBOZU_RATIO 80
#define CANDLESTICK_LONG_DAY_AVG_RATIO 90
#define CANDLESTICK_SHORT_DAY_AVG_RATIO 40
#define CANDLESTICK_ENGULFING_RATIO 70
#define CANDLESTICK_HARAMI_RATIO 30
#define CANDLESTICK_HAMMER_BODY_LONG_SHADOW_RATIO 0.5
#define CANDLESTICK_HAMMER_SHORT_SHADOW_RANGE_RATIO 0.15
#define CANDLESTICK_DOJI_BODY_RANGE_RATIO 0.025
#define CANDLESTICK_EQUAL_VALUES_RANGE_RATIO 0.025

#define MAX(a, b) (a > b)? a: b
#define MIN(a, b) (a < b)? a: b

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

/** Check whether a given day intersects a channel built by connecting
 * two pivots.  This applies to both breakout detection, and trend
 * channel break detection.
 */

cJSON* ana_check_for_breaks(jl_data_ptr jl, jl_pivot_ptr pivots, int num) {
    cJSON *res = NULL;
    int i = jl->data->pos - 1;
    daily_record_ptr r = &(jl->data->data[i]), r_1 = &(jl->data->data[i - 1]);
    int len_1 = cal_num_busdays(pivots[num - 4].date, r->date);
    int len_2 = cal_num_busdays(pivots[num - 5].date, r->date);
    if ((len_1 < MIN_CHANNEL_LEN) && (len_2 < MIN_CHANNEL_LEN))
	return NULL;
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
	return NULL;
    /* find the extremes for today's price either the high/low for the
       day, or yesterday's close */
    int ub = (r->high > r_1->close)? r->high: r_1->close;
    int lb = (r->low < r_1->close)? r->low: r_1->close;
    if ((upper_channel_len >= MIN_CHANNEL_LEN) &&
	(px_up > lb) && (px_up < ub)) {
	res = cJSON_CreateObject();
	cJSON_AddStringToObject(res, "stp", "JL_B");
	cJSON_AddNumberToObject(res, "dir", 1);
	cJSON_AddNumberToObject(res, "ipx", px_up);
	cJSON_AddNumberToObject(res, "len", upper_channel_len);
	cJSON_AddNumberToObject(res, "vr", (float)r->volume /
				jl->recs[jl->pos - 2].volume);
	cJSON_AddNumberToObject(res, "f", jl->factor);
    }
    if ((lower_channel_len >= MIN_CHANNEL_LEN) &&
	(px_down > lb) && (px_down < ub)) {
	if (res == NULL)
	    res = cJSON_CreateObject();
	cJSON_AddStringToObject(res, "stp", "JL_B");
	cJSON_AddNumberToObject(res, "dir", -1);
	cJSON_AddNumberToObject(res, "ipx", px_down);
	cJSON_AddNumberToObject(res, "len", lower_channel_len);
	cJSON_AddNumberToObject(res, "vr", (float)r->volume /
				jl->recs[jl->pos - 2].volume);
	cJSON_AddNumberToObject(res, "f", jl->factor);
    }
    return res;
}

/** Check whether a given day creates a new pivot point, and determine
 * if that pivot point represents a change in trend.
 */
void ana_check_for_pullbacks(jl_data_ptr jl_050, jl_pivot_ptr p_050,
			     jl_data_ptr jl_100, jl_pivot_ptr p_100,
			     jl_data_ptr jl_150, jl_pivot_ptr p_150,
			     jl_data_ptr jl_200, jl_pivot_ptr p_200) {

}

/** Check whether the action on a given day stops at a
 * resistance/support point, or whether it pierces that
 * resistance/support (on high volume), and whether it recovers after
 * piercing or not
 */
void ana_check_for_support_resistance(jl_data_ptr jl_050, jl_pivot_ptr p_050,
				      jl_data_ptr jl_100, jl_pivot_ptr p_100,
				      jl_pivot_ptr p_150, jl_pivot_ptr p_200) {

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
    int ix_0 = jl->data->pos - 1;
    for(int ix = 0; ix < 6; ix++)
	r[ix] = &(jl->data->data[ix_0 - ix]);
    int body[6];
    for(int ix = 0; ix < 6; ix++)
	body[ix] = r[ix]->close - r[ix]->open;
    int marubozu[6], engulfing = 0, harami[5], piercing = 0, star = 0, cbs = 0;
    int three = 0, eng_harami = 0;
    for(int ix = 0; ix < 6; ix++) {
	int ratio = 100 * body[ix] / (r[ix]->high - r[ix]->low);
	marubozu[ix] = (abs(ratio) < CANDLESTICK_MARUBOZU_RATIO)? 0: ratio;
	if (ix >= 5)
	    continue;
	if ((100 * abs(body[ix - 1]) > jl->recs[ix_0 - ix - 2].rg *
	     CANDLESTICK_LONG_DAY_AVG_RATIO) &&
	    (100 * body[ix] <= CANDLESTICK_HARAMI_RATIO * body[ix - 1]) &&
	    (MAX(r[ix]->open, r[ix]->close) <
	     MAX(r[ix - 1]->open, r[ix - 1]->close)) &&
	    (MIN(r[ix]->open, r[ix]->close) >
	     MIN(r[ix - 1]->open, r[ix - 1]->close)))
	    harami[ix] = 1;
	else
	    harami[ix] = 0;
    }
    if ((body[0] * body[1] < 0) && (abs(body[0]) > abs(body[1])) &&
	(MAX(r[0]->open, r[0]->close) >= MAX(r[1]->open, r[1]->close)) &&
	(MIN(r[0]->open, r[0]->close) <= MIN(r[1]->open, r[1]->close)))
	engulfing = (body[0] > 0)? 1: -1;
    else
	engulfing = 0;
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
	if ((body[2] < 0) && (MAX(r[1]->open, r[1]->close) < r[2]->close) &&
	    (MAX(r[1]->open, r[1]->close) < r[0]->open) &&
	    (2 * r[0]->close > r[2]->open + r[2]->close))
	    star = 1;
	if ((body[2] > 0) && (MIN(r[1]->open, r[1]->close) > r[2]->close) &&
	    (MIN(r[1]->open, r[1]->close) > r[0]->open) &&
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

/* TODO: redo this */
/*     def engulfingharamifun(r): */
/*     if(r['marubozu'] == 0 or */
/*        (r['marubozu_3'] == 0 and r['marubozu_4'] == 0)): */
/*                 return 0 */
/* 		    if r['marubozu_3'] != 0: */
/*     if r['marubozu'] * r['marubozu_3'] < 0: */
/*                     return 0 */
/* 			if r['harami_2'] == 1: */
/*                     return r['engulfing'] */
/* 		    else: */
/* 			if r['marubozu'] * r['marubozu_4'] < 0: */
/*                     return 0 */
/* 			if(r['harami_3'] == 1 and */
/* 			   r['hi_2'] < max(r['hi_1'], r['hi_3']) and */
/* 			   r['lo_2'] > min(r['lo_1'], r['lo_3'])): */
/*                     return r['engulfing'] */
/*             return 0 */
/* 			ts.df['engulfharami'] = ts.df.apply(engulfingharamifun, axis=1) */

/* ts.df['avg_body'] = ts.df['body'].rolling(self.avg_range_days).mean() */
/* ts.df['avg_v'] = ts.df['volume'].rolling(self.avg_volume_days).mean() */
/* ts.df['upper_shadow'] = ts.df.apply(r['hi'] - max(r['o'], r['c'])) */
/* ts.df['lower_shadow'] = ts.df.apply(min(r['o'], r['c']) - r['lo']) */

}

void ana_jl_setups(char* stk, char* dt, bool eod) {
    jl_data_ptr jl_050 = ana_get_jl(stk, dt, JL_050, JLF_050);
    jl_data_ptr jl_100 = ana_get_jl(stk, dt, JL_100, JLF_100);
    jl_data_ptr jl_150 = ana_get_jl(stk, dt, JL_150, JLF_150);
    jl_data_ptr jl_200 = ana_get_jl(stk, dt, JL_200, JLF_200);
    if ((jl_050 == NULL) || (jl_100 == NULL) ||	(jl_150 == NULL) ||
	(jl_200 == NULL))
	return;
    /** TODO:
	1. Get 4 pivots for JL_150 and JL_200
	2. Find least recent date lrdt for last pivot for JL_150 and JL_200
	3. Find all the JL_050 and JL_100 pivots up to lrdt
	4. Look for 1-2-3 setups for JL_100, JL_150 and JL_200
	5. Look for SR for JL_050, jl_100
	6. Look for breakouts and/or intersecting channels for ...
    */
    cJSON *res = NULL, *setups = cJSON_CreateArray();
    int num_050, num_100, num_150, num_200;
    jl_pivot_ptr pivots_050 = NULL, pivots_100 = NULL, pivots_150 = NULL,
	pivots_200 = NULL;
    pivots_150 = jl_get_pivots(jl_150, 4, &num_150);
    pivots_200 = jl_get_pivots(jl_200, 4, &num_200);
    if ((num_150 < 5) || (num_200 < 5)) {
	LOGERROR("Got %d %s pivots, needed 5\n", num_150, JL_150);
	LOGERROR("Got %d %s pivots, needed 5\n", num_200, JL_200);
	goto end;
    }
    char *lrdt_150 = pivots_150[num_150 - 2].date, 
	*lrdt_200 = pivots_200[num_200 - 2].date;
    char *lrdt = (strcmp(lrdt_150, lrdt_200) >= 0)? lrdt_200: lrdt_150;
    pivots_100 = jl_get_pivots_date(jl_100, lrdt, &num_100);
    if (num_100 < 5) {
	free(pivots_100);
	pivots_100 = NULL;
	pivots_100 = jl_get_pivots(jl_100, 4, &num_100);
    }
    pivots_050 = jl_get_pivots_date(jl_050, lrdt, &num_050);
    if (num_050 < 5) {
	free(pivots_050);
	pivots_050 = NULL;
	pivots_050 = jl_get_pivots(jl_050, 4, &num_050);
    }
    if ((res = ana_check_for_breaks(jl_050, pivots_050, num_050)) != NULL)
	cJSON_AddItemToArray(setups, res);
    if ((res = ana_check_for_breaks(jl_100, pivots_100, num_100)) != NULL)
	cJSON_AddItemToArray(setups, res);
    if ((res = ana_check_for_breaks(jl_150, pivots_150, num_150)) != NULL)
	cJSON_AddItemToArray(setups, res);
    if ((res = ana_check_for_breaks(jl_200, pivots_200, num_200)) != NULL)
	cJSON_AddItemToArray(setups, res);
    int num_setups = cJSON_GetArraySize(setups);
    if (num_setups > 0) {
	LOGINFO("Inserting %d setups for %s on %s\n", num_setups, stk, dt);
	char *setup_string = cJSON_Print(setups);
	char sql_cmd[16384];
	sprintf(sql_cmd, "insert into jl_setups values ('%s','%s','%s')", 
		dt, stk, setup_string);
	db_transaction(sql_cmd);
    }

/*     ana_check_for_pullbacks(fp, jl_050, pivots_050, jl_100, pivots_100, */
/* 			    jl_150, pivots_150, jl_200, pivots_200); */
/*     ana_check_for_support_resistance(fp, jl_050, pivots_050, jl_100, */
/* 				     pivots_100, jl_150, pivots_150, */
/* 				     jl_200, pivots_200); */
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
	if (cJSON_IsString(ldr) && (ldr->valuestring != NULL)) {
	    ana_setups(fp, ldr->valuestring, dt, next_dt, eod);
/* 	    ana_jl_setups(ldr->valuestring, dt, eod); */
	}
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

void stk_analysis(char* stk, char* start_date, char* end_date, bool clear_db) {
    /** calculate setups for a single stock (stk) during the time
     *interval [start_date, end_date]
     */
    char sql_cmd[256];
    if (clear_db) {
	sprintf(sql_cmd, "DELETE FROM jl_setups WHERE stk='%s' AND dt "\
		"BETWEEN '%s' AND '%s'", stk, start_date, end_date);
	db_transaction(sql_cmd);
    }
    char *crs_date = start_date;
    while(strcmp(crs_date, end_date) < 0) {
	ana_jl_setups(stk, crs_date, true);
	cal_next_bday(cal_ix(crs_date), &crs_date);
    }
}

#endif
