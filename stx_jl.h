#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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


typedef struct jl_record_t {
    int ix;
    int rg;
    int state;
    int price;
    bool pivot;
    int state2;
    int price2;
    bool pivot2;
    int lns;
    int ls;
} jl_record, *jl_record_ptr;

typedef struct jl_last_t {
    int prim_price;
    int prim_state;
    int price;
    int state;
} jl_last, *jl_last_ptr;

typedef struct jl_pivot_t {
    char date[16];
    int state;
    int price;
    int rg;
    struct jl_pivot_t* next;
} jl_pivot, *jl_pivot_ptr;

typedef struct jl_data_t {
    jl_record_ptr recs;
    int size;
    int pos;
    float factor;
    int* rgs;
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
    free(jl->recs);
    jl_recs = NULL;
}

void jl_init_rec(jl_data_ptr jl, int ix) {
    jl_record_ptr jlr = &(jl.recs[ix]), jlr_1 = NULL;
    jlr->ix = ix;
    jlr->state = (jlr->state2 = NONE);
    jlr->price = (jlr->price2 = 0);
    jlr->pivot = (jlr->pivot2 = false);
    if (ix > 0) { 
	jlr_1 = &(jl.recs[ix - 1]);
	jlr->lns = jlr_1->lns;
	jlr->ls = jlr_1->ls;
    } else {
	jlr->lns = -1;
	jlr->ls = -1;
    }
    int rg = ts_true_range(jl->data, ix);
    if(ix < jl->window - 1) {
	jlr->rg = 0;
	jl->rgs[ix % jl->window] = rg;
    } else if(ix == jl->window - 1) {
	jl->rgs[ix % jl->window] = rg;
	int sum_rg = 0;
	for (int ixx = 0; ixx < jl->window; ixx++)
	    sum_rg += jl->rgs[ixx];
	jlr->rg = sum_rg / jl->window;
    } else { /* ix >= jl->window */
	jlr->rg = jl->window * jlr_1->rg + rg - jl->rgs[ix % jl->window];
	jl->rgs[ix % jl->window] = rg;
    }
    jl->num_pivots = 0;
    jl->pivots = NULL;
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

bool jl_down(int state) {
    return (state == DOWNTREND || state == REACTION);
}

void jl_update_pivot_diff_day(jl_data_ptr jl, int ix) {
    jl_record_ptr jlr = &(jl->recs[ix]);
    jl_record_ptr piv_rec = &(jl->recs[jlr->lns]);
    bool piv_s2 = jl_primary(piv_rec->state2);
    if (piv_s2)
	piv_rec->pivot2 = true;
    else
	piv_rec->pivot = true;
    jl_pivot_ptr piv = (jl_pivot_ptr) malloc(sizeof(jl_pivot));
    strcpy(piv->date, jl->data[jlr->lns].date);
    piv->state = piv_s2? piv_rec->state2: piv_rec->state;
    piv->price = piv_s2? piv_rec->price2: piv_rec->price;
    piv->rg = piv_rec->rg;
    if (jl->pivots == NULL)
	piv->next = NULL;
    else
	piv->next = jl->pivots;
    jl->pivots = piv;
}

/* TODO: handle the case when there are two pivots in the same day */

void jl_update_lns_and_pivots(jl_data_ptr jl, int ix) {
    jl_record_ptr jlr = &(jl->recs[ix]);
    jl_record_ptr jlns = &(jl->recs[jlr->lns]);
    int crt_s = jl_primary(jlr->state)? jlr->state: jlr->state2;
    int lns_s = lns_s2? jlns->state2: jlns->state;
    bool lns_s2 = jl_primary(jlns->state2);
    if ((jl_up(crt_s) && jl_down(lns_s)) || (jl_down(crt_s) && jl_up(lns_s))) {
	if (lns_s2)
	    jlns->pivot2 = true;
	else
	    jlns->pivot = true;
    }
    jlr->lns = ix;
}

void jl_rec_day(jl_data_ptr jl, int ix, int upstate, int downstate) {
    jl_init_rec(jl, ix);
    daily_record_ptr sr = &(jl->data[ix]);
    jl_record_ptr jlr = &(jl->recs[ix]);
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
    if (jlr->state != NONE) {
	jl_update_last(jl, ix);
	if (jl_primary(upstate) || jl_primary(downstate))
	    jl_update_lns_and_pivots(jl, ix);
    }
    jl->pos++;
}

jl_data_ptr jl_init20(stx_data_ptr data, float factor) {
    return jl_init(data, factor, 20);
}

jl_data_ptr jl_init(stx_data_ptr data, float factor, int window) {
    if(data->num_recs < window)
	return NULL;
    jl_data_ptr jl = (jl_data_ptr) malloc(sizeof(jl_data));
    jl->data = data;
    jl->recs = (jl_record_ptr) calloc(data->num_recs, sizeof(jl_record));
    jl->size = data->num_recs;
    jl->factor = factor;
    jl->pos = 0;
    jl->last = (jl_last_ptr) malloc(sizeof(jl_last));
    jl->last->price = (jl->last->prim_price = -1);
    jl->last->state = (jl->last->prim_state = NONE);
    jl->rgs = (int *) calloc(window, sizeof(int));
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
	jl_rec_day(jld, ix, (ix == max_ix)? RALLY: NONE,
		   (ix == min_ix)? REACTION: NONE);
    jl->lp[S_RALLY] = (jl->lp[RALLY] = 
		       (jl->lp[UPTREND] = 
			(jl->lp[M_RALLY] = max)));
    jl->lp[DOWNTREND] = (jl->lp[REACTION] = 
			 (jl->lp[S_REACTION] = 
			  (jl->lp[M_REACTION] = min)));
}

void jl_split_adjust(jl_data_ptr jl, ht_item_ptr split) {
    float ratio = split->val.ratio;
    for(int ix = 0; ix < jl->pos; ix++) {
	jl->recs[ix].rg = (int) (jl->recs[ix].rg * ratio);
	jl->recs[ix].price = (int) (jl->recs[ix].price * ratio);
	jl->recs[ix].price2 = (int) (jl->recs[ix].price2 * ratio);
    }
    for(int ix = 0; ix < jl->window; ix++)
	jl->rgs[ix] = (int) (jl->rgs[ix] * ratio);
    for(int ix = 0; ix < 8; ix++)
	jl->rgs[ix] = (int) (jl->rgs[ix] * ratio);
    jl->last->prim_price = (int) (jl->last->prim_price * ratio);
    jl->last->price = (int) (jl->last->price * ratio);
}

void jl_sra(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if (jl->lp[UPTREND] < sr->high)
	sh = UPTREND;
    else if (jl->lp[M_RALLY] + factor < sr->high) {
	if (jl->last->prim_state == N_RALLY || jl->last->prim_state == UPTREND)
	    sh = (sr->high > jl->last->prim_price)? UPTREND: S_RALLY;
	else
	    sh = UPTREND;
    } else if ((jl->lp[N_RALLY] < sr->high) && 
	       (jl->last->prim_state != UPTREND))
	sh = N_RALLY;
    else if (jl->lp[S_RALLY] < sr->high)
	sh = S_RALLY;
    if (jl_up(sh) && jl_down(jl->last->prim_state))
	jl->lp[M_REACTION] = jl->last->prim_price;
    if (sr->low < jl->lp[S_RALLY] - 2 * factor) {
	if (jl->lp[N_REACTION] < sr->low)
	    sl = S_REACTION;
	else {
	    sl = ((sr->low < jl->lp[DOWNTREND]) || 
		  (sr->low < jl->lp[M_REACTION] - factor))? DOWNTREND: 
		N_REACTION;
	    if (jl_up(jl->last->prim_state))
		jl->lp[M_RALLY] = jl->last->prim_price;
	}
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_nra(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if ((jl->lp[UPTREND] < sr->high) || (jl->lp[M_RALLY] + factor < sr->high))
	sh = UPTREND;
    else if (jl->lp[N_RALLY] < sr->high)
	sh = N_RALLY;
    if (sr->low < jl->lp[N_RALLY] - 2 * factor) {
	if (jl->lp[N_REACTION] < sr->low)
	    sl = S_REACTION;
	else if ((sr->low < jl->lp[DOWNTREND]) || 
		 (sr->low < jl->lp[M_REACTION] - factor))
	    sl = DOWNTREND;
	else
	    sl = N_REACTION;
	if (sl != S_REACTION)
	    jl->lp[M_RALLY] = jl->lp[N_RALLY];
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_ut(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if (jl->lp[UPTREND] < sr->high)
	sh = UPTREND;
    if (sr->low <= jl->lp[UPTREND] - 2 * factor) {
	sl = ((sr->low < jl->lp[DOWNTREND]) || 
	      (sr->low < jl->lp[M_REACTION] - factor))? DOWNTREND: N_REACTION;
	jl->lp[M_RALLY] = jl->lp[UPTREND];
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_sre(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if (sr->low < jl->lp[DOWNTREND])
	sl = DOWNTREND;
    else if (jl->lp[M_REACTION] - factor > sr->low) {
	if ((jl->last->prim_state == N_REACTION) ||
	    (jl->last->prim_state == DOWNTREND))
	    sl = (sr->low < jl->last->prim_price)? DOWNTREND: S_REACTION;
	else
	    sl = DOWNTREND;
    } else if ((jl->lp[N_REACTION] > sr->low) &&
	       (jl->last->prim_state != DOWNTREND))
	sl = N_REACTION;
    else if (jl->lp[S_REACTION] > sr->low)
	sl = S_REACTION;
    if (jl_down(sl) && jl_up(jl->last->prim_state))
	jl->lp[M_RALLY] = jl->last_prim_price;
    if (sr->high > jl->lp[S_REACTION] + 2 * factor) {
	if (jl->lp[N_RALLY] > sr->high)
	    sh = S_RALLY;
	else {
	    sh = ((sr->high > jl->lp[UPTREND]) ||
		  (sr->high > jl->lp[M_RALLY] + factor))? UPTREND: N_RALLY;
	    if (jl_down(jl->last->prim_state))
		jl->lp[M_REACTION] = jl->last->prim_price;
	}
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_dt(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if (jl->lp[DOWNTREND] > sr->low)
	sl = DOWNTREND;
    if (sr->high >= jl->lp[DOWNTREND] + 2 * factor) {
	sh = ((sr->high > jl->lp[UPTREND]) ||
	      (sr->high > jl->lp[M_RALLY] + factor))? UPTREND: N_RALLY;
	jl->lp[M_REACTION] = jl->lp[DOWNTREND];
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_nre(jl_data_ptr jl, int factor) {
    daily_record_ptr sr = &(jl->data[jl->pos]);
    int sh = NONE, sl = NONE;
    if ((jl->lp[DOWNTREND] > sr->low) || 
	(jl->lp[M_REACTION] - factor > sr->low))
	sl = DOWNTREND;
    else if (jl->lp[N_REACTION] > sr->low)
	sl = N_REACTION;
    if (sr->high > jl->lp[N_REACTION] + 2 * factor) {
	if (jl->lp[N_RALLY] > sr->high)
	    sh = S_RALLY;
	else if ((sr->high > jl->lp[UPTREND]) ||
		 (sr->high > jl->lp[M_RALLY] + factor))
	    sh = UPTREND;
	else
	    sh = N_RALLY;
	if (sh != S_RALLY)
	    jl->lp[M_REACTION] = jl->lp[N_REACTION];
    }
    jl_rec_day(jl, jl->pos, sh, sl);
}

void jl_next(jl_data_ptr jl) {
    if (jl->pos >= jl->size)
	return;
    ht_item_ptr split = ht_get(jl->data->splits, jl->data[jl->pos].date);
    if (split != NULL) 
	jl_split_adjust(jl, split);
    int factor = (int) (jl->factor * jl->recs[jl->pos].rg);
    switch(jl->last->state) {
    case S_RALLY:
	jl_sra(jl, factor);
	break;
    case N_RALLY:
	jl_nra(jl, factor);
	break;
    case UPTREND:
	jl_ut(jl, factor);
	break;
    case DOWNTREND:
	jl_dt(jl, factor);
	break;
    case N_REACTION:
	jl_nre(jl, factor);
	break;
    case S_REACTION:
	jl_sre(jl, factor);
	break;
    default:
	LOGWARN("%s: unknown state %d found for date %s", jl->data->stk,
		jl->last->state, jl->data[jl->pos].date);
	break;
    }
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

void jl_print_rec(int state, int price, bool pivot) {
    switch(state) {
    case S_RALLY:
	fprintf(stderr, "%10d\n", price);
	break;
    case S_REACTION:
	fprintf(stderr, "%50d\n", price);
	break;
    case RALLY:
	fprintf(stderr, "%10s", " ");
	if (pivot)
	    fprintf(stderr, PRED, "%10d%s\n", price, RESET);
	else
	    fprintf(stderr, "%10d\n", price);
	break;
    case REACTION:
	fprintf(stderr, "%40s", " ");
	if (pivot)
	    fprintf(stderr, PGRN, "%10d%s\n", price, RESET);
	else
	    fprintf(stderr, "%10d\n", price);
	break;
    case UPTREND:
	fprintf(stderr, "%20s", " ");
	if (pivot)
	    fprintf(stderr, PGRN, "%10d%s\n", price, RESET);
	else
	    fprintf(stderr, GRN, "%10d%s\n", price, RESET);
	break;
    case DOWNTREND:
	fprintf(stderr, "%30s", " ");
	if (pivot)
	    fprintf(stderr, PRED, "%10d%s\n", price, RESET);
	else
	    fprintf(stderr, RED, "%10d%s\n", price, RESET);
	break;
    default:
	fprintf(stderr, "\n");
	break;
    }
}

void jl_print_record(jl_data_ptr jl, bool print_pivots_only, bool print_nils) {
    int last_pivot = ts_find_date_record(jl->data, jl->pivots->date);
    for(int ix = 0; ix <= jl->pos; ix++) {
	jl_record_ptr jlr = &(jl->recs[ix]);
	if (jlr->state == NONE && (!print_nils))
	    continue;
	if (ix < last_piv && !jlr->pivot && !jlr->pivot2 && print_pivots_only)
	    continue;	
	if (!print_pivots_only || jlr->pivot) {
	    fprintf(stderr, "%s", jl->data[ix].date);
	    jl_print_rec(jlr->state, jlr->price, jlr->pivot);
	}
	if (jlr->state2 != NONE && (!print_pivots_only || jlr->pivot2)) {
	    fprintf(stderr, "%s", jl->data[ix].date);
	    jl_print_rec(jlr->state2, jlr->price2, jlr->pivot2);
	}
#ifdef DEBUG
	fprintf(stderr, "%8d lns = %5d, ls = %5d\n", ix, jlr->lns, jlr->ls);
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
}

    def get_num_pivots(self, num_pivs):
        ixx = -1
        end = -len(self.jl_recs)
        pivs = []
        while len(pivs) < num_pivs and ixx >= end:
            jlr = self.jl_recs[ixx]
            if jlr[self.col_ix['pivot2']] == 1:
                pivs.append(JLPivot(jlr[self.col_ix['dt']],
                                    jlr[self.col_ix['state2']],
                                    jlr[self.col_ix['price2']],
                                    jlr[self.col_ix['rg']]))
            if len(pivs) < num_pivs and jlr[self.col_ix['pivot']] == 1:
                pivs.append(JLPivot(jlr[self.col_ix['dt']],
                                    jlr[self.col_ix['state']],
                                    jlr[self.col_ix['price']],
                                    jlr[self.col_ix['rg']]))
            ixx -= 1
        pivs.reverse()
        return pivs

    def get_pivots_in_days(self, num_days):
        ixx = -1
        end = -len(self.jl_recs)
        pivs = []
        if end < -num_days:
            end = -num_days
        while ixx > end:
            jlr = self.jl_recs[ixx]
            if jlr[self.col_ix['pivot2']] == 1:
                pivs.append(JLPivot(jlr[self.col_ix['dt']],
                                    jlr[self.col_ix['state2']],
                                    jlr[self.col_ix['price2']],
                                    jlr[self.col_ix['rg']]))
            if jlr[self.col_ix['pivot']] == 1:
                pivs.append(JLPivot(jlr[self.col_ix['dt']],
                                    jlr[self.col_ix['state']],
                                    jlr[self.col_ix['price']],
                                    jlr[self.col_ix['rg']]))
            ixx -= 1
        pivs.reverse()
        return pivs

    def print_pivs(self, pivs):
        output = ''
        for piv in pivs:
            px_str = self.get_formatted_price(piv.state, 1, piv.price)
            output += '{0:s}{1:s}{2:6.2f}\n'.format(piv.dt, px_str, piv.rg)
        print(output)

    def last_rec(self, col_name, ixx=1):
        if ixx > len(self.jl_recs):
            ixx = len(self.jl_recs)
        jlr = self.jl_recs[-ixx]
        if col_name in ['state', 'price', 'pivot']:
            col_name2 = '{0:s}2'.format(col_name)
            if jlr[self.col_ix['state2']] != StxJL.Nil:
                return jlr[self.col_ix[col_name2]]
        return jlr[self.col_ix[col_name]]


if __name__ == '__main__':
    stk = sys.argv[1]
    sd = sys.argv[2]
    ed = sys.argv[3]
    dt = sys.argv[4]
    factor = float(sys.argv[5])
    ts = StxTS(stk, sd, ed)
    jl = StxJL(ts, factor)
    jlres = jl.jl(dt)
    jl.jl_print()
    pivs = jl.get_pivots_in_days(100)
    print("Pivs in 100 days:")
    jl.print_pivs(pivs)
    pivs = jl.get_num_pivots(4)
    print("4 pivs:")
    jl.print_pivs(pivs)
    # jl.jl_print(print_pivots_only = True)
    # pd.set_option('display.max_rows', 2000)
    # pd.set_option('display.max_columns', 1500)
    # pd.set_option('display.width', 1500)
    # print(jlres)
