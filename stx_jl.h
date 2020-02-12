#ifndef __STX_JL_H__
#define __STX_JL_H__
#include <cjson/cJSON.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
#include "stx_ts.h"

#define PIVOT      0x100
#define NONE       -1
#define S_RALLY    0
#define RALLY      1
#define UPTREND    2
#define DOWNTREND  3
#define REACTION   4
#define S_REACTION 5
#define M_RALLY    6
#define M_REACTION 7

#define RED   "\x1B[31m"
#define GRN   "\x1B[32m"
#define PRED   "\x1B[4;31m"
#define PGRN   "\x1B[4;32m"
#define RESET "\x1B[0m"

#define NO_OBV 1000000000
#define NO_RANGE -1

typedef struct jl_record_t {
    int ix;
    int rg;
    int volume;
    int state;
    int price;
    bool pivot;
    int state2;
    int price2;
    bool pivot2;
    int lns;
    int ls;
    int obv[3];
    int piv_obv;
    int piv_obv2;
} jl_record, *jl_record_ptr;

typedef struct jl_last_t {
    int prim_price;
    int prim_state;
    int price;
    int state;
    int lns_obv;
} jl_last, *jl_last_ptr;

typedef struct jl_pivot_t {
    char date[16];
    int state;
    int price;
    int rg;
    int obv;
    struct jl_pivot_t* next;
    struct jl_pivot_t* prev;
} jl_pivot, *jl_pivot_ptr;

typedef struct jl_data_t {
    jl_record_ptr recs;
    int size;
    int pos;
    float factor;
    int* rgs;
    int* volumes;
    int window;
    int lp[8];
    stx_data_ptr data;
    jl_last_ptr last;
    int num_pivots;
    jl_pivot_ptr pivots;
} jl_data, *jl_data_ptr;

void jl_free(jl_data_ptr jl) {
    if (jl->last != NULL) {
        free(jl->last);
        jl->last = NULL;
    }
    jl_pivot_ptr piv_crs = jl->pivots, piv_next = piv_crs->next;
    free(piv_crs);
    piv_crs = NULL;
    while(piv_next != NULL) {
        piv_crs = piv_next;
        piv_next = piv_crs->next;
        free(piv_crs);
        piv_crs = NULL;
    }
    free(jl->rgs);
    jl->rgs = NULL;
    free(jl->volumes);
    jl->volumes = NULL;
    free(jl->recs);
    jl->recs = NULL;
}

void jl_init_rec(jl_data_ptr jl, int ix) {
    jl_record_ptr jlr = &(jl->recs[ix]), jlr_1 = NULL;
    jlr->ix = ix;
    jlr->state = (jlr->state2 = NONE);
    jlr->price = (jlr->price2 = 0);
    jlr->pivot = (jlr->pivot2 = false);
    if (ix > 0) { 
        jlr_1 = &(jl->recs[ix - 1]);
        jlr->lns = jlr_1->lns;
        jlr->ls = jlr_1->ls;
    } else {
        jlr->lns = -1;
        jlr->ls = -1;
    }
    int rg = ts_true_range(jl->data, ix);
    jl->rgs[ix % jl->window] = rg;
    jl->volumes[ix % jl->window] = jl->data->data[ix].volume;
    if(ix < jl->window - 1) {
        jlr->rg = 0;
        jlr->volume = 0;
    } else {
        int sum_rg = 0, sum_volume = 0;
        for (int ixx = 0; ixx < jl->window; ixx++) {
            sum_rg += jl->rgs[ixx];
            sum_volume += jl->volumes[ixx];
        }
        jlr->rg = sum_rg / jl->window;
        jlr->volume = sum_volume / jl->window;
    }
    jlr->piv_obv = NO_OBV;
    jlr->piv_obv2 = NO_OBV;
}

bool jl_primary(int state) {
    return (state == UPTREND || state == RALLY || state == DOWNTREND || 
            state == REACTION);
}

void jl_update_last(jl_data_ptr jl, int ix) {
    jl_record_ptr jlr = &(jl->recs[ix]);
    if (jlr->state2 == NONE) {
        if (jlr->state != NONE) {
            jl->last->price = jlr->price;
            jl->last->state = jlr->state;
            if (jl_primary(jlr->state)) {
                jl->last->prim_price = jlr->price;
                jl->last->prim_state = jlr->state;
            }
            jl->lp[jlr->state] = jlr->price;
        }
    } else {
        jl->last->price = jlr->price2;
        jl->last->state = jlr->state2;
        jl->lp[jlr->state] = jlr->price;
        jl->lp[jlr->state2] = jlr->price2;
        if (jl_primary(jlr->state2)) {
            jl->last->prim_price = jlr->price2;
            jl->last->prim_state = jlr->state2;
        } else if (jl_primary(jlr->state)) {
            jl->last->prim_price = jlr->price;
            jl->last->prim_state = jlr->state;
        }
    }
    jlr->ls = ix;
}

bool jl_up(int state) {
    return (state == UPTREND || state == RALLY);
}

bool jl_up_all(int state) {
    return (state == UPTREND || state == RALLY) || (state == S_RALLY);
}

bool jl_down(int state) {
    return (state == DOWNTREND || state == REACTION);
}

bool jl_down_all(int state) {
    return (state == DOWNTREND || state == REACTION) || (state == S_REACTION);
}

int jl_calc_obv(jl_data_ptr jl, char* start_date, int start_state, int end) {
    int obv = 0;
    int start = ts_find_date_record(jl->data, start_date, 0);
    jl_record_ptr jls = &(jl->recs[start]), jle = &(jl->recs[end]);
    if (jls->volume == 0)
        return 0;
    daily_record_ptr srs = &(jl->data->data[start]);
    daily_record_ptr sre = &(jl->data->data[end]);
    bool hi_b4_lo = ((2 * srs->close) < (srs->high + srs->low));
    if ((jl_up(start_state) && hi_b4_lo) || 
        (jl_down(start_state) && !hi_b4_lo))
        obv += 100 * jls->obv[1] / jls->volume;
    obv += 100 * jls->obv[2] / jls->volume;
    for(int ix = start + 1; ix <= end; ix++) {
        jl_record_ptr jlr = &(jl->recs[ix]);
        /* Deal with garbage when the average volume is 0 */
        int jlrv = (jlr->volume > 0)? jlr->volume: 1000000;
        obv += (100 * (jlr->obv[0] + jlr->obv[1] + jlr->obv[2]) / jlrv);
    }
    return obv / 10;
}

jl_pivot_ptr jl_get_pivots(jl_data_ptr jl, int num_pivots, int* piv_num) {
    int n = num_pivots;
    jl_pivot_ptr crs = jl->pivots;
    if (crs != NULL)
        n--;
    while((n > 0) && (crs!= NULL) && (crs->next != NULL)) {
        crs = crs->next;
        n--;
    }
    *piv_num = (num_pivots - n + 1);
    jl_pivot_ptr res = (jl_pivot_ptr) calloc(*piv_num, sizeof(jl_pivot));
    jl_pivot_ptr res_crs = res;
    for(int ix = 0; ix < *piv_num - 1; ix++) {
        memcpy(res_crs, crs, sizeof(jl_pivot));
        res_crs++;
        crs = crs->prev;
    }
    int last_lns = jl->recs[jl->pos - 1].lns;
    jl_record_ptr jlr_lns = &(jl->recs[last_lns]);
    strcpy(res_crs->date, jl->data->data[last_lns].date);
    res_crs->state = jl_primary(jlr_lns->state2)? jlr_lns->state2:
        jlr_lns->state;
    res_crs->price = jl_primary(jlr_lns->state2)? jlr_lns->price2:
        jlr_lns->price;
    res_crs->rg = jlr_lns->rg;
    res_crs->obv = jl->last->lns_obv;
    return res;
}


jl_pivot_ptr jl_get_pivots_date(jl_data_ptr jl, char* dt, int* piv_num) {
    int n = 0;
    jl_pivot_ptr crs = jl->pivots;
    while((strcmp(dt, crs->date) <= 0) && (crs!= NULL) &&
          (crs->next != NULL)) {
        crs = crs->next;
        n++;
    }
    *piv_num = n + 1;
    jl_pivot_ptr res = (jl_pivot_ptr) calloc(n + 1, sizeof(jl_pivot));
    jl_pivot_ptr res_crs = res;
    crs = crs->prev;
    for(int ix = 0; ix < n; ix++) {
        memcpy(res_crs, crs, sizeof(jl_pivot));
        res_crs++;
        crs = crs->prev;
    }
    int last_lns = jl->recs[jl->pos - 1].lns;
    jl_record_ptr jlr_lns = &(jl->recs[last_lns]);
    strcpy(res_crs->date, jl->data->data[last_lns].date);
    res_crs->state = jl_primary(jlr_lns->state2)? jlr_lns->state2:
        jlr_lns->state;
    res_crs->price = jl_primary(jlr_lns->state2)? jlr_lns->price2:
        jlr_lns->price;
    res_crs->rg = jlr_lns->rg;
    res_crs->obv = jl->last->lns_obv;
    return res;
}

int jl_prev_ns(jl_data_ptr jl) {
        jl_record_ptr jlr = &(jl->recs[jl->pos - 1]);
        if (jl_primary(jlr->state2) && jl_primary(jlr->state))
                return jlr->state;
        jl_record_ptr jlr_1 = &(jl->recs[jl->pos - 2]);
        jl_record_ptr jlr_pns = &(jl->recs[jlr_1->lns]);
    return jl_primary(jlr_pns->state2)? jlr_pns->state2: jlr_pns->state;
}

cJSON* jl_pivots_json(jl_data_ptr jl, int num_pivots) {
    int num_pivs;
    jl_pivot_ptr pivots = jl_get_pivots(jl, num_pivots, &num_pivs);
    cJSON *json_jl = cJSON_CreateObject();
    if (cJSON_AddNumberToObject(json_jl, "f", jl->factor) == NULL)
        goto end;
    cJSON *pivs = NULL;
    if ((pivs = cJSON_AddArrayToObject(json_jl, "pivs")) == NULL)
        goto end;
    for(int ix = 0; ix < num_pivs; ix++) {
        cJSON * pivot = cJSON_CreateObject();
        if (cJSON_AddStringToObject(pivot, "d", pivots[ix].date) == NULL)
            goto end;
        if (cJSON_AddNumberToObject(pivot, "x", pivots[ix].price) == NULL)
            goto end;
        if (cJSON_AddNumberToObject(pivot, "s", pivots[ix].state) == NULL)
            goto end;
        if (cJSON_AddNumberToObject(pivot, "r", pivots[ix].rg) == NULL)
            goto end;
        if (cJSON_AddNumberToObject(pivot, "v", pivots[ix].obv) == NULL)
            goto end;
        if (cJSON_AddBoolToObject(pivot, "p", (ix != (num_pivs - 1))) == NULL)
            goto end;
        if (ix == (num_pivs - 1)) {
            if (cJSON_AddNumberToObject(pivot, "s_1", jl_prev_ns(jl)) == NULL)
                goto end;
        }
        cJSON_AddItemToArray(pivs, pivot);
    }
 end:
    free(pivots);
    return json_jl;
}

jl_pivot_ptr jl_add_pivot(jl_pivot_ptr pivots, char* piv_date, int piv_state, 
                          int piv_price, int piv_rg) {
    jl_pivot_ptr piv = (jl_pivot_ptr) malloc(sizeof(jl_pivot));
    strcpy(piv->date, piv_date);
    piv->state = piv_state;
    piv->price = piv_price;
    piv->rg = piv_rg;
    if (pivots == NULL)
        piv->next = NULL;
    else {
        piv->next = pivots;
        pivots->prev = piv;
    }
    return piv;
}

bool jl_is_pivot(int prev_state, int crt_state) {
    return (((prev_state == REACTION || prev_state == DOWNTREND) &&
             (crt_state == RALLY || crt_state == UPTREND)) ||
            ((crt_state == REACTION || crt_state == DOWNTREND) &&
             (prev_state == RALLY || prev_state == UPTREND)));
}

/* TODO: handle the case when there are two pivots in the same day */

void jl_update_lns_and_pivots(jl_data_ptr jl, int ix) {
    jl_record_ptr jlr = &(jl->recs[ix]);
    jl_record_ptr jlns = (jlr->lns > -1)? &(jl->recs[jlr->lns]): NULL;
    int crt_s = jl_primary(jlr->state)? jlr->state: jlr->state2;
    if (jlns != NULL) {
        bool p2 = jl_primary(jlns->state2);
        if (p2)
            jlns->piv_obv2 = NO_OBV;
        else
            jlns->piv_obv = NO_OBV;
        int lns_s = p2? jlns->state2: jlns->state;
        if (jl_is_pivot(lns_s, crt_s)) {
            if (p2)
                jlns->pivot2 = true;
            else
                jlns->pivot = true;
            jl->pivots = jl_add_pivot(jl->pivots, 
                                      jl->data->data[jlr->lns].date, 
                                      p2? jlns->state2: jlns->state,
                                      p2? jlns->price2: jlns->price, jlns->rg);
            jl->pivots->obv = jl_calc_obv(jl, jl->pivots->date,
                                          jl->pivots->state, ix);
        }
    }
    if (jl_is_pivot(jlr->state, jlr->state2)) {
        jlr->pivot = true;
        jl_pivot_ptr piv = (jl_pivot_ptr) malloc(sizeof(jl_pivot));
        strcpy(piv->date, jl->data->data[ix].date);
        piv->state = jlr->state;
        piv->price = jlr->price;
        piv->rg = jlr->rg;
        jl->pivots = jl_add_pivot(jl->pivots, jl->data->data[ix].date, 
                                  jlr->state, jlr->price, jlr->rg);
        jl->pivots->obv = jl_calc_obv(jl, jl->pivots->date,
                                      jl->pivots->state, ix);
    }
    jlr->lns = ix;
    jl->last->lns_obv = jl_calc_obv(jl, jl->data->data[ix].date,
                                    (jl_primary(jlr->state2)? jlr->state2:
                                     jlr->state), ix);
}

char* jl_state_to_string(int state) {
    static char _retval[4];
    switch(state) {
    case S_RALLY:
        strcpy(_retval, "SRa");
        break;
    case RALLY:
        strcpy(_retval, "NRa");
        break;
    case UPTREND:
        strcpy(_retval, "UT");
        break;
    case DOWNTREND:
        strcpy(_retval, "DT");
        break;
    case REACTION:
        strcpy(_retval, "NRe");
        break;
    case S_REACTION:
        strcpy(_retval, "SRe");
        break;
    case M_RALLY:
        strcpy(_retval, "MRa");
        break;
    case M_REACTION:
        strcpy(_retval, "MRe");
        break;
    default:
        strcpy(_retval, "Nil");
        break;
    }
    return _retval;
}

void jl_set_obv(jl_data_ptr jl, int ix) {
    daily_record_ptr sr = &(jl->data->data[ix]);
    int prev_close = (ix > 0)? jl->data->data[ix - 1].close: sr->open;
    jl_record_ptr jlr = &(jl->recs[ix]);
    bool hi_b4_lo = ((2 * sr->close) < (sr->high + sr->low));
    int e1 = hi_b4_lo? sr->high: sr->low, e2 = hi_b4_lo? sr->low: sr->high;
    int diff1 = e1 - prev_close, diff2 = e2 - e1, diff3 = sr->close - e2;
    int sum = abs(diff1) + abs(diff2) + abs(diff3);
    /* Deal with garbage like first days with the same open/high/low/close */
    /* or same previous close/high/low/close */
    if (sum == 0)
        sum = 1;
    jlr->obv[0] = diff1 * sr->volume / sum;
    jlr->obv[1] = diff2 * sr->volume / sum;
    jlr->obv[2] = diff3 * sr->volume / sum;
}

void jl_rec_day(jl_data_ptr jl, int ix, int upstate, int downstate) {
    jl_init_rec(jl, ix);
    daily_record_ptr sr = &(jl->data->data[ix]);
    jl_record_ptr jlr = &(jl->recs[ix]);
#ifdef DDEBUGG
    fprintf(stderr, "%s: upstate = %d, downstate = %d\n", 
            jl->data->data[ix].date, upstate, downstate);
#endif
    if (upstate != NONE && downstate != NONE) {
        if (2 * sr->close < sr->high + sr->low) {
            jlr->state = upstate;
            jlr->price = sr->high;
            jlr->state2 = downstate;
            jlr->price2 = sr->low;
        } else {
            jlr->state = downstate;
            jlr->price = sr->low;
            jlr->state2 = upstate;
            jlr->price2 = sr->high;
        }
    } else if (upstate != NONE) {
        jlr->state = upstate;
        jlr->price = sr->high;
    } else if (downstate != NONE) {
        jlr->state = downstate;
        jlr->price = sr->low;
    }
    jl_set_obv(jl, ix);
    if (jlr->volume > 0) {
        int jl_obv = 10 * (jlr->obv[0] + jlr->obv[1] + jlr->obv[2]) /
            jlr->volume;
        jl->last->lns_obv += jl_obv;
        jl_pivot_ptr piv_crs = jl->pivots;
        jl_pivot_ptr piv_next = (piv_crs == NULL)? NULL: piv_crs->next;
        if (piv_crs != NULL)
            piv_crs->obv += jl_obv;
        while(piv_next != NULL) {
            piv_crs = piv_next;
            piv_crs->obv += jl_obv;
            piv_next = piv_crs->next;
        }
    }
    if (jlr->state != NONE) {
        jl_update_last(jl, ix);
        if (jl_primary(upstate) || jl_primary(downstate))
            jl_update_lns_and_pivots(jl, ix);
    }
    jl->pos++;
#ifdef DDEBUGG
        fprintf(stderr, "%8d lns = %5d, ls = %5d, rg = %6d\n", ix, 
                jlr->lns, jlr->ls, jlr->rg);
        for(int ixxx = 0; ixxx < jl->window; ixxx++)
            fprintf(stderr, "%6d ", jl->rgs[ixxx]);
        fprintf(stderr, "\n");
        fprintf(stderr, "  last: prim_px =%6d, prim_s = %s, px =%6d, s = %s\n",
                jl->last->prim_price, 
                jl_state_to_string(jl->last->prim_state), 
                jl->last->price, jl_state_to_string(jl->last->state));
        fprintf(stderr, "  lp[%s] =%6d lp[%s] =%6d lp[%s] =%6d lp[%s] =%6d \n",
                jl_state_to_string(S_RALLY), jl->lp[S_RALLY], 
                jl_state_to_string(RALLY), jl->lp[RALLY],
                jl_state_to_string(UPTREND), jl->lp[UPTREND],
                jl_state_to_string(DOWNTREND), jl->lp[DOWNTREND]);
        fprintf(stderr, "  lp[%s] =%6d lp[%s] =%6d lp[%s] =%6d lp[%s] =%6d \n",
                jl_state_to_string(REACTION), jl->lp[REACTION],
                jl_state_to_string(S_REACTION), jl->lp[S_REACTION],
                jl_state_to_string(M_RALLY), jl->lp[M_RALLY],
                jl_state_to_string(M_REACTION), jl->lp[M_REACTION]);
#endif
}

jl_data_ptr jl_init(stx_data_ptr data, float factor, int window) {
    if(data->num_recs < window)
        return NULL;
    jl_data_ptr jl = (jl_data_ptr) malloc(sizeof(jl_data));
    jl->data = data;
    jl->recs = (jl_record_ptr) calloc(data->num_recs, sizeof(jl_record));
    jl->size = data->num_recs;
    jl->factor = factor;
    jl->pos = -1;
    jl->window = window;
    jl->last = (jl_last_ptr) malloc(sizeof(jl_last));
    jl->last->price = (jl->last->prim_price = -1);
    jl->last->state = (jl->last->prim_state = NONE);
    jl->rgs = (int *) calloc(window, sizeof(int));
    jl->volumes = (int *) calloc(window, sizeof(int));
    jl->num_pivots = 0;
    jl->pivots = NULL;
    int max = 0, max_ix, min = 2000000000, min_ix;
    ts_set_day(data, data->data[window - 1].date, 0);
    for(int ix = 0; ix < window; ix++) {
        if(data->data[ix].high > max) {
            max = data->data[ix].high;
            max_ix = ix;
        }
        if(data->data[ix].low < min) {
            min = data->data[ix].low;
            min_ix = ix;
        }
    }
    for(int ix = 0; ix < window; ix++)
        jl_rec_day(jl, ix, (ix == max_ix)? RALLY: NONE,
                   (ix == min_ix)? REACTION: NONE);
    jl->lp[S_RALLY] = (jl->lp[RALLY] = 
                       (jl->lp[UPTREND] = 
                        (jl->lp[M_RALLY] = max)));
    jl->lp[DOWNTREND] = (jl->lp[REACTION] = 
                         (jl->lp[S_REACTION] = 
                          (jl->lp[M_REACTION] = min)));
    return jl;
}

jl_data_ptr jl_init20(stx_data_ptr data, float factor) {
    return jl_init(data, factor, 20);
}

void jl_split_adjust(jl_data_ptr jl, ht_item_ptr split) {
    float ratio = split->val.ratio;
    for(int ix = 0; ix < jl->pos; ix++) {
        jl->recs[ix].rg = (int) (jl->recs[ix].rg * ratio);
        jl->recs[ix].volume = (int) (jl->recs[ix].volume / ratio);
        jl->recs[ix].price = (int) (jl->recs[ix].price * ratio);
        jl->recs[ix].price2 = (int) (jl->recs[ix].price2 * ratio);
    }
    for(int ix = 0; ix < jl->window; ix++) {
        jl->rgs[ix] = (int) (jl->rgs[ix] * ratio);
        jl->volumes[ix] = (int) (jl->volumes[ix] / ratio);
    }
    jl->last->prim_price = (int) (jl->last->prim_price * ratio);
    jl->last->price = (int) (jl->last->price * ratio);
}

void jl_sra(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if (jl->lp[UPTREND] < sr->high)
        sh = UPTREND;
    else if (jl->lp[M_RALLY] + factor < sr->high) {
        if (jl->last->prim_state == RALLY || jl->last->prim_state == UPTREND)
            sh = (sr->high > jl->last->prim_price)? UPTREND: S_RALLY;
        else
            sh = UPTREND;
    } else if ((jl->lp[RALLY] < sr->high) && 
               (jl->last->prim_state != UPTREND))
        sh = RALLY;
    else if (jl->lp[S_RALLY] < sr->high)
        sh = S_RALLY;
    if (jl_up(sh) && jl_down(jl->last->prim_state))
        jl->lp[M_REACTION] = jl->last->prim_price;
    if (sr->low < jl->lp[S_RALLY] - 2 * factor) {
        if (jl->lp[REACTION] < sr->low)
            sl = S_REACTION;
        else {
            sl = ((sr->low < jl->lp[DOWNTREND]) || 
                  (sr->low < jl->lp[M_REACTION] - factor))? DOWNTREND: 
                REACTION;
            if (jl_up(jl->last->prim_state))
                jl->lp[M_RALLY] = jl->last->prim_price;
        }
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_nra(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if ((jl->lp[UPTREND] < sr->high) || (jl->lp[M_RALLY] + factor < sr->high))
        sh = UPTREND;
    else if (jl->lp[RALLY] < sr->high)
        sh = RALLY;
    if (sr->low < jl->lp[RALLY] - 2 * factor) {
        if (jl->lp[REACTION] < sr->low)
            sl = S_REACTION;
        else if ((sr->low < jl->lp[DOWNTREND]) || 
                 (sr->low < jl->lp[M_REACTION] - factor))
            sl = DOWNTREND;
        else
            sl = REACTION;
        if (sl != S_REACTION)
            jl->lp[M_RALLY] = jl->lp[RALLY];
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_ut(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if (jl->lp[UPTREND] < sr->high)
        sh = UPTREND;
    if (sr->low <= jl->lp[UPTREND] - 2 * factor) {
        sl = ((sr->low < jl->lp[DOWNTREND]) || 
              (sr->low < jl->lp[M_REACTION] - factor))? DOWNTREND: REACTION;
        jl->lp[M_RALLY] = jl->lp[UPTREND];
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_sre(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if (sr->low < jl->lp[DOWNTREND])
        sl = DOWNTREND;
    else if (jl->lp[M_REACTION] - factor > sr->low) {
        if ((jl->last->prim_state == REACTION) ||
            (jl->last->prim_state == DOWNTREND))
            sl = (sr->low < jl->last->prim_price)? DOWNTREND: S_REACTION;
        else
            sl = DOWNTREND;
    } else if ((jl->lp[REACTION] > sr->low) &&
               (jl->last->prim_state != DOWNTREND))
        sl = REACTION;
    else if (jl->lp[S_REACTION] > sr->low)
        sl = S_REACTION;
    if (jl_down(sl) && jl_up(jl->last->prim_state))
        jl->lp[M_RALLY] = jl->last->prim_price;
    if (sr->high > jl->lp[S_REACTION] + 2 * factor) {
        if (jl->lp[RALLY] > sr->high)
            sh = S_RALLY;
        else {
            sh = ((sr->high > jl->lp[UPTREND]) ||
                  (sr->high > jl->lp[M_RALLY] + factor))? UPTREND: RALLY;
            if (jl_down(jl->last->prim_state))
                jl->lp[M_REACTION] = jl->last->prim_price;
        }
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_dt(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if (jl->lp[DOWNTREND] > sr->low)
        sl = DOWNTREND;
    if (sr->high >= jl->lp[DOWNTREND] + 2 * factor) {
        sh = ((sr->high > jl->lp[UPTREND]) ||
              (sr->high > jl->lp[M_RALLY] + factor))? UPTREND: RALLY;
        jl->lp[M_REACTION] = jl->lp[DOWNTREND];
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_nre(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if ((jl->lp[DOWNTREND] > sr->low) || 
        (jl->lp[M_REACTION] - factor > sr->low))
        sl = DOWNTREND;
    else if (jl->lp[REACTION] > sr->low)
        sl = REACTION;
    if (sr->high > jl->lp[REACTION] + 2 * factor) {
        if (jl->lp[RALLY] > sr->high)
            sh = S_RALLY;
        else if ((sr->high > jl->lp[UPTREND]) ||
                 (sr->high > jl->lp[M_RALLY] + factor))
            sh = UPTREND;
        else
            sh = RALLY;
        if (sh != S_RALLY)
            jl->lp[M_REACTION] = jl->lp[REACTION];
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

int jl_next(jl_data_ptr jl) {
    if (jl->pos >= jl->size)
        return -1;
    ht_item_ptr split = ht_get(jl->data->splits, jl->data->data[jl->pos].date);
    if (split != NULL) 
        jl_split_adjust(jl, split);
    int factor = (int) (jl->factor * jl->recs[jl->pos - 1].rg);
    switch(jl->last->state) {
    case S_RALLY:
        jl_sra(jl, factor);
        break;
    case RALLY:
        jl_nra(jl, factor);
        break;
    case UPTREND:
        jl_ut(jl, factor);
        break;
    case DOWNTREND:
        jl_dt(jl, factor);
        break;
    case REACTION:
        jl_nre(jl, factor);
        break;
    case S_REACTION:
        jl_sre(jl, factor);
        break;
    default:
        LOGWARN("%s: unknown state %d found for date %s", jl->data->stk,
                jl->last->state, jl->data->data[jl->pos].date);
        break;
    }
    if (ts_next(jl->data) == -1)
        return -1;
    return 0;
} 

void jl_print_rec(char* date, int state, int price, bool pivot, int rg, 
                  int obv) {
    if (obv == NO_OBV)
        fprintf(stderr, "%6s", " ");
    else
        fprintf(stderr, "%6d", obv);
    fprintf(stderr, " %s ", date);
    switch(state) {
    case S_RALLY:
        fprintf(stderr, "|%7d|%7s|%7s|%7s|%7s|%7s|", price, "", "", "",
                "", "");
        break;
    case S_REACTION:
        fprintf(stderr, "|%7s|%7s|%7s|%7s|%7s|%7d|", "", "", "", "", "",
                price);
        break;
    case RALLY:
        if (pivot)
            fprintf(stderr, "|%7s|%s%7d%s|%7s|%7s|%7s|%7s|", "", PRED,
                    price, RESET, "", "", "", "");
        else
            fprintf(stderr, "|%7s|%7d|%7s|%7s|%7s|%7s|", "", price, "",
                    "", "", "");
        break;
    case REACTION:
        if (pivot)
            fprintf(stderr, "|%7s|%7s|%7s|%7s|%s%7d%s|%7s|", "", "", "", "",
                    PGRN, price, RESET, "");
        else
            fprintf(stderr, "|%7s|%7s|%7s|%7s|%7d|%7s|", "", "", "", "",
                    price, "");
        break;
    case UPTREND:
        if (pivot)
            fprintf(stderr, "|%7s|%7s|%s%7d%s|%7s|%7s|%7s|", "", "", PGRN,
                    price, RESET, "", "", "");
        else
            fprintf(stderr, "|%7s|%7s|%7d|%7s|%7s|%7s|", "", "", price,
                    "", "", "");
        break;
    case DOWNTREND:
        if (pivot)
            fprintf(stderr, "|%7s|%7s|%7s|%s%7d%s|%7s|%7s|", "", "", "",
                    PRED, price, RESET, "", "");
        else
            fprintf(stderr, "|%7s|%7s|%7s|%7d|%7s|%7s|", "", "", "", price,
                    "", "");
        break;
    default:
        fprintf(stderr, "|%7s|%7s|%7s|%7s|%7s|%7s|", "", "", "", "", "", "");
        break;
    }
    if (rg == NO_RANGE)
        fprintf(stderr, "%6s\n", " ");
    else
        fprintf(stderr, "%6d\n", rg);

}

jl_data_ptr jl_jl(stx_data_ptr data, char* end_date, float factor) {
    jl_data_ptr jl = jl_init20(data, factor);
    int res = 0;
/*     jl->pos++; */
    while((strcmp(jl->data->data[jl->pos].date, end_date) <= 0) && (res != -1))
        res = jl_next(jl);
    return jl;
}

int jl_advance(jl_data_ptr jl, char* end_date) {
    int res = 0, num_days = 0;
    while((strcmp(jl->data->data[jl->pos].date, end_date) <= 0) &&
          (res != -1)) {
        res = jl_next(jl);
        num_days++;
    }
    return num_days;
}

/* jl_pivot_ptr jl_pivots(jl_data_ptr jl, int num_pivs, int* piv_num) { */
void jl_print_pivots(jl_data_ptr jl, int num_pivs, int* piv_num) {
    int n = num_pivs;
    jl_pivot_ptr crs = jl->pivots;
    while((n > 0) && (crs!= NULL) && (crs->next != NULL)) {
        jl_print_rec(crs->date, crs->state, crs->price, true, NO_RANGE, 
                     crs->obv);
        crs = crs->next;
        n--;
    }
    *piv_num = (num_pivs - n);
}

void jl_print(jl_data_ptr jl, bool print_pivots_only, bool print_nils) {
    int last_piv = ts_find_date_record(jl->data, jl->pivots->date, 0);
    for(int ix = 0; ix < jl->pos; ix++) {
        jl_record_ptr jlr = &(jl->recs[ix]);
        if (jlr->state == NONE && (!print_nils))
            continue;
        if (ix < last_piv && !jlr->pivot && !jlr->pivot2 && print_pivots_only)
            continue;
        if (!print_pivots_only || jlr->pivot || (ix > last_piv))
            jl_print_rec(jl->data->data[ix].date, jlr->state, jlr->price, 
                         jlr->pivot, jlr->rg, jlr->piv_obv);
        if (jlr->state2 != NONE && (!print_pivots_only || jlr->pivot2 || 
                                    (ix > last_piv)))
            jl_print_rec(jl->data->data[ix].date, jlr->state2, jlr->price2, 
                         jlr->pivot2, jlr->rg, jlr->piv_obv2);
    }
}
#endif
