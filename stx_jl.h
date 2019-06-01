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
} jl_data, *jl_data_ptr;

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
    if (jl_primary(piv_rec->state2))
	piv_rec->pivot2 = true;
    else
	piv_rec->pivot = true;
}

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

class StxJL:
    # static variables
    UT_fmt = '\x1b[1;32;40m'
    DT_fmt = '\x1b[1;31;40m'
    UP_piv_fmt = '\x1b[4;32;40m'  # '4;30;42'
    DN_piv_fmt = '\x1b[4;31;40m'  # '4;37;41'

    def __init__(self, ts, f, w=20):
        self.ts = ts
        self.f = f
        self.w = w
        self.cols = ['dt', 'rg', 'state', 'price', 'pivot', 'state2',
                     'price2', 'pivot2', 'p1_dt', 'p1_px', 'p1_s',
                     'lns_dt', 'lns_px', 'lns_s', 'lns', 'ls_s', 'ls']
        self.col_ix = dict(zip(self.cols, range(0, len(self.cols))))
        self.jl_recs = []
        self.jl_recs.append(self.cols)
        self.jlix = {}  # map dates to indices in JL records list (jl_rec)
        self.jl_ix = 0  # current index in the jlix table
        self.last = {'prim_px': 0, 'prim_state': StxJL.Nil, 'px': 0,
                     'state': StxJL.Nil}
        self.ts.df['hb4l'] = self.ts.df.apply(lambda r: 1 if
                                              2*r['c'] < r['hi']+r['lo'] else 0,
                                              axis=1)

    def jl(self, dt):
        self.ts.set_day(dt, -1)
        end = self.ts.pos
        start_w = self.initjl()
        # print('jl(): start_w={0:d}'.format(start_w))
        for ixx in range(start_w, end + 1):
            self.ts.next_day()
            self.nextjl()
        return self.jl_recs

    def initjl(self):
        ss = self.ts.start
        w1 = self.ts.pos - ss + 1
        win = self.w if w1 >= self.w else w1
        self.ts.set_day(str(self.ts.df.index[ss+win-1].date()))
        # print('initjl: ss={0:d} w1={1:d} win={2:d} set_day:{3:s}'.
        #       format(ss, w1, win, str(self.ts.df.index[ss+win-1].date())))
        df_0 = self.ts.df[ss: ss + win]
        max_dt = df_0['hi'].idxmax()
        min_dt = df_0['lo'].idxmin()
        hi = df_0.loc[max_dt].hi
        lo = df_0.loc[min_dt].lo
        self.trs = []
        self.trs.append(df_0.ix[0].hi - df_0.ix[0].lo)
        for h, l, c_1 in zip(df_0['hi'].values[1:], df_0['lo'].values[1:],
                             df_0['c'].values[:-1]):
            self.trs.append(max(h, c_1) - min(l, c_1))
        self.avg_rg = np.mean(self.trs)
        # assign hi to SRa, NRa, UT, m_NRa, and lo to SRe, NRe, DT, m_NRe
        self.lp = [hi, hi, hi, lo, lo, lo, hi, lo]
        for ixx in range(0, len(df_0)):
            dtc = df_0.index[ixx]
            dtcs = str(dtc.date())
            self.jlix[dtcs] = ixx + 1
            if dtc == max_dt and dtc == min_dt:
                self.rec_day(StxJL.NRa, StxJL.NRe, self.ts.start + ixx)
            elif dtc == max_dt:
                self.rec_day(StxJL.NRa, StxJL.Nil, self.ts.start + ixx)
            elif dtc == min_dt:
                self.rec_day(StxJL.Nil, StxJL.NRe, self.ts.start + ixx)
            else:
                self.rec_day(StxJL.Nil, StxJL.Nil, self.ts.start + ixx)
        return self.ts.start + win

    def rec_day(self, sh, sl, ixx=-1):
        if ixx == -1:
            ixx = self.ts.pos
        sr = self.ts.df.ix[ixx]
        dtc = str(self.ts.df.index[ixx].date())
        lix = ixx - self.ts.start
        # print("lix = %d" % lix)
        dd = self.init_first_rec(dtc) if lix == 0 else self.init_rec(dtc, lix)
        if sh != StxJL.Nil and sl != StxJL.Nil:
            if sr.hb4l == 1:
                dd.update({'state': sh, 'price': sr['hi'], 'state2': sl,
                           'price2': sr['lo']})
            else:
                dd.update({'state': sl, 'price': sr['lo'], 'state2': sh,
                           'price2': sr['hi']})
        elif sh != StxJL.Nil:
            dd.update({'state': sh, 'price': sr['hi']})
        elif sl != StxJL.Nil:
            dd.update({'state': sl, 'price': sr['lo']})
        else:
            pass  # nothing to do, the record is already initialized
        if dd['state'] != StxJL.Nil:
            self.update_last(dd)
            self.update_lns_pivots(dd, lix)
        lst = [dd[col] for col in self.cols]
        self.jl_recs.append(lst)
        self.jlix[dtc] = lix + 1

    def update_last(self, dd):
        if dd['state2'] == StxJL.Nil:
            if dd['state'] != StxJL.Nil:
                self.last['px'] = dd['price']
                self.last['state'] = dd['state']
                if self.primary(dd['state']):
                    self.last['prim_px'] = dd['price']
                    self.last['prim_state'] = dd['state']
                self.lp[dd['state']] = dd['price']
        else:
            self.last['px'] = dd['price2']
            self.last['state'] = dd['state2']
            self.lp[dd['state2']] = dd['price2']
            self.lp[dd['state']] = dd['price']
            if self.primary(dd['state2']):
                self.last['prim_px'] = dd['price2']
                self.last['prim_state'] = dd['state2']
            elif self.primary(dd['state']):
                self.last['prim_px'] = dd['price']
                self.last['prim_state'] = dd['state']

    def update_lns_pivots(self, dd, list_ix):
        if (self.up(dd['state']) and self.dn(dd['lns'])) or \
           (self.dn(dd['state']) and self.up(dd['lns'])):
            self.update_pivot_diff_day(dd)
        if dd['state'] != StxJL.Nil:
            dd['ls_s'] = dd['ls']
            dd['ls'] = dd['state']
        if self.primary(dd['state']):
            dd['lns_dt'] = dd['dt']
            dd['lns_px'] = dd['price']
            dd['lns_s'] = dd['lns']
            dd['lns'] = dd['state']
        if (self.up(dd['state2']) and self.dn(dd['lns'])) or \
           (self.dn(dd['state2']) and self.up(dd['lns'])):
            if dd['lns_dt'] == dd['dt']:
                dd['pivot'] = 1
                dd['p1_dt'] = dd['dt']
                dd['p1_px'] = dd['price']
                dd['p1_s'] = dd['state']
            else:
                self.update_pivot_diff_day(dd)
        if dd['state2'] != StxJL.Nil:
            dd['ls_s'] = dd['ls']
            dd['ls'] = dd['state2']
        if self.primary(dd['state2']):
            dd['lns_dt'] = dd['dt']
            dd['lns_px'] = dd['price2']
            dd['lns_s'] = dd['lns']
            dd['lns'] = dd['state2']

    def update_pivot_diff_day(self, dd):
        # print(self.jlix)
        piv_rec = self.jl_recs[self.jlix[dd['lns_dt']]]
        if self.primary(piv_rec[self.col_ix['state2']]):
            piv_rec[self.col_ix['pivot2']] = 1
            dd['p1_px'] = piv_rec[self.col_ix['price2']]
            dd['p1_s'] = piv_rec[self.col_ix['state2']]
        else:
            piv_rec[self.col_ix['pivot']] = 1
            dd['p1_px'] = piv_rec[self.col_ix['price']]
            dd['p1_s'] = piv_rec[self.col_ix['state']]
        dd['p1_dt'] = dd['lns_dt']

    def nextjl(self):
        dtc = self.ts.current_date()
        split_info = self.ts.splits.get(pd.Timestamp(dtc))
        if split_info is not None:
            self.adjust_for_splits(split_info[0])
        fctr = self.f * self.avg_rg
        if self.last['state'] == StxJL.SRa:
            self.sRa(fctr)
        elif self.last['state'] == StxJL.NRa:
            self.nRa(fctr)
        elif self.last['state'] == StxJL.UT:
            self.uT(fctr)
        elif self.last['state'] == StxJL.DT:
            self.dT(fctr)
        elif self.last['state'] == StxJL.NRe:
            self.nRe(fctr)
        elif self.last['state'] == StxJL.SRe:
            self.sRe(fctr)
        self.trs.pop(0)
        sr = self.ts.df.ix[self.ts.pos]
        sr_1 = self.ts.df.ix[self.ts.pos - 1]
        self.trs.append(max(sr.hi, sr_1.c) - min(sr.lo, sr_1.c))
        self.avg_rg = np.mean(self.trs)

    def adjust_for_splits(self, ratio):
        for ixx in range(0, len(self.lp)):
            self.lp[ixx] = self.lp[ixx] * ratio
        for jlr in self.jl_recs[1:]:
            jlr[self.col_ix['rg']] = jlr[self.col_ix['rg']] * ratio
            jlr[self.col_ix['price']] = jlr[self.col_ix['price']] * ratio
            jlr[self.col_ix['price2']] = jlr[self.col_ix['price2']] * ratio
            jlr[self.col_ix['p1_px']] = jlr[self.col_ix['p1_px']] * ratio
            jlr[self.col_ix['lns_px']] = jlr[self.col_ix['lns_px']] * ratio
        self.last['prim_px'] = self.last['prim_px'] * ratio
        self.last['px'] = self.last['px'] * ratio
        self.trs[:] = [x * ratio for x in self.trs]

    def sRa(self, fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = StxJL.Nil, StxJL.Nil
        if self.lp[StxJL.UT] < r.hi:
            sh = StxJL.UT
        elif self.lp[StxJL.m_NRa] + fctr < r.hi:
            if self.last['prim_state'] in [StxJL.NRa, StxJL.UT]:
                sh = StxJL.UT if r.hi > self.last['prim_px'] else StxJL.SRa
            else:
                sh = StxJL.UT
        elif self.lp[StxJL.NRa] < r.hi and self.last['prim_state'] != StxJL.UT:
            sh = StxJL.NRa
        elif self.lp[StxJL.SRa] < r.hi:
            sh = StxJL.SRa
        if self.up(sh) and self.dn(self.last['prim_state']):
            self.lp[StxJL.m_NRe] = self.last['prim_px']
        if r.lo < self.lp[StxJL.SRa] - 2 * fctr:
            if self.lp[StxJL.NRe] < r.lo:
                sl = StxJL.SRe
            else:
                sl = StxJL.DT if(r.lo < self.lp[StxJL.DT] or
                                 r.lo < self.lp[StxJL.m_NRe] - fctr) \
                    else StxJL.NRe
                if self.up(self.last['prim_state']):
                    self.lp[StxJL.m_NRa] = self.last['prim_px']
        self.rec_day(sh, sl)

    def nRa(self, fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = StxJL.Nil, StxJL.Nil
        if self.lp[StxJL.UT] < r.hi or self.lp[StxJL.m_NRa] + fctr < r.hi:
            sh = StxJL.UT
        elif self.lp[StxJL.NRa] < r.hi:
            sh = StxJL.NRa
        if r.lo < self.lp[StxJL.NRa] - 2 * fctr:
            if self.lp[StxJL.NRe] < r.lo:
                sl = StxJL.SRe
            elif r.lo < self.lp[StxJL.DT] or r.lo < self.lp[StxJL.m_NRe] - fctr:
                sl = StxJL.DT
            else:
                sl = StxJL.NRe
            if sl != StxJL.SRe:
                self.lp[StxJL.m_NRa] = self.lp[StxJL.NRa]
        self.rec_day(sh, sl)

    def uT(self, fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = StxJL.Nil, StxJL.Nil
        if self.lp[StxJL.UT] < r.hi:
            sh = StxJL.UT
        if r.lo <= self.lp[StxJL.UT] - 2 * fctr:
            sl = StxJL.DT if (r.lo < self.lp[StxJL.DT] or
                              r.lo < self.lp[StxJL.m_NRe] - fctr) \
                else StxJL.NRe
            self.lp[StxJL.m_NRa] = self.lp[StxJL.UT]
        self.rec_day(sh, sl)

    def sRe(self, fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = StxJL.Nil, StxJL.Nil
        if self.lp[StxJL.DT] > r.lo:
            sl = StxJL.DT
        elif self.lp[StxJL.m_NRe] - fctr > r.lo:
            if self.last['prim_state'] in [StxJL.NRe, StxJL.DT]:
                sl = StxJL.DT if r.lo < self.last['prim_px'] else StxJL.SRe
            else:
                sl = StxJL.DT
        elif self.lp[StxJL.NRe] > r.lo and self.last['prim_state'] != StxJL.DT:
            sl = StxJL.NRe
        elif self.lp[StxJL.SRe] > r.lo:
            sl = StxJL.SRe
        if self.dn(sl) and self.up(self.last['prim_state']):
            self.lp[StxJL.m_NRa] = self.last['prim_px']
        if r.hi > self.lp[StxJL.SRe] + 2 * fctr:
            if self.lp[StxJL.NRa] > r.hi:
                sh = StxJL.SRa
            else:
                sh = StxJL.UT if(r.hi > self.lp[StxJL.UT] or
                                 r.hi > self.lp[StxJL.m_NRa] + fctr) \
                                 else StxJL.NRa
                if self.dn(self.last['prim_state']):
                    self.lp[StxJL.m_NRe] = self.last['prim_px']
        self.rec_day(sh, sl)

    def dT(self, fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = StxJL.Nil, StxJL.Nil
        if self.lp[StxJL.DT] > r.lo:
            sl = StxJL.DT
        if r.hi >= self.lp[StxJL.DT] + 2 * fctr:
            sh = StxJL.UT if (r.hi > self.lp[StxJL.UT] or
                              r.hi > self.lp[StxJL.m_NRa] + fctr) \
                else StxJL.NRa
            self.lp[StxJL.m_NRe] = self.lp[StxJL.DT]
        self.rec_day(sh, sl)

    def nRe(self, fctr):
        r = self.ts.df.ix[self.ts.pos]
        sh, sl = StxJL.Nil, StxJL.Nil
        if self.lp[StxJL.DT] > r.lo or self.lp[StxJL.m_NRe] - fctr > r.lo:
            sl = StxJL.DT
        elif self.lp[StxJL.NRe] > r.lo:
            sl = StxJL.NRe
        if r.hi > self.lp[StxJL.NRe] + 2 * fctr:
            if self.lp[StxJL.NRa] > r.hi:
                sh = StxJL.SRa
            elif r.hi > self.lp[StxJL.UT] or r.hi > self.lp[StxJL.m_NRa] + fctr:
                sh = StxJL.UT
            else:
                sh = StxJL.NRa
            if sh != StxJL.SRa:
                self.lp[StxJL.m_NRe] = self.lp[StxJL.NRe]
        self.rec_day(sh, sl)

    def up(self, state):
        return state in [StxJL.NRa, StxJL.UT]

    def dn(self, state):
        return state in [StxJL.NRe, StxJL.DT]

    def up_all(self, state):
        return state in [StxJL.SRa, StxJL.NRa, StxJL.UT]

    def dn_all(self, state):
        return state in [StxJL.SRe, StxJL.NRe, StxJL.DT]

    def primary(self, state):
        return state in [StxJL.NRa, StxJL.UT, StxJL.NRe, StxJL.DT]

    def secondary(self, state):
        return state in [StxJL.SRa, StxJL.SRe]

    def jlr_print(self, jlr):
        return 'dt:{0:s} rg:{1:.2f} s:{2:d} px:{3:.2f} p:{4:d} s2:{5:d} ' \
            'px2:{6:.2f} p2:{7:d} p1dt:{8:s} p1px:{9:.2f} p1s:{10:d} ' \
            'ldt:{11:s} lpx:{12:.2f} lns_s:{13:d} lns:{14:d} ls_s:{15:d} ' \
            'ls:{16:d}'. \
            format(jlr[self.col_ix['dt']], jlr[self.col_ix['rg']],
                   jlr[self.col_ix['state']], jlr[self.col_ix['price']],
                   jlr[self.col_ix['pivot']], jlr[self.col_ix['state2']],
                   jlr[self.col_ix['price2']], jlr[self.col_ix['pivot2']],
                   jlr[self.col_ix['p1_dt']], jlr[self.col_ix['p1_px']],
                   jlr[self.col_ix['p1_s']], jlr[self.col_ix['lns_dt']],
                   jlr[self.col_ix['lns_px']], jlr[self.col_ix['lns_s']],
                   jlr[self.col_ix['lns']], jlr[self.col_ix['ls_s']],
                   jlr[self.col_ix['ls']])

    def jlr_print2(self, jlr):
        return 's:{0:d} px:{1:.2f} p:{2:d} s2:{3:d} px2:{4:.2f} p2:{5:d} ' \
            'p1dt:{6:s} p1px:{7:.2f} p1s:{8:d} ldt:{9:s} lpx:{10:.2f} ' \
            'lns:{11:d} ls_s:{12:d} ls:{13:d}'. \
            format(jlr[self.col_ix['state']], jlr[self.col_ix['price']],
                   jlr[self.col_ix['pivot']], jlr[self.col_ix['state2']],
                   jlr[self.col_ix['price2']], jlr[self.col_ix['pivot2']],
                   jlr[self.col_ix['p1_dt']], jlr[self.col_ix['p1_px']],
                   jlr[self.col_ix['p1_s']], jlr[self.col_ix['lns_dt']],
                   jlr[self.col_ix['lns_px']], jlr[self.col_ix['lns']],
                   jlr[self.col_ix['ls_s']], jlr[self.col_ix['ls']])

    def get_formatted_price(self, state, pivot, price):
        s_fmt = ''
        e_fmt = '\x1b[0m'
        if state == StxJL.UT:
            s_fmt = StxJL.UT_fmt if pivot == 0 else StxJL.UP_piv_fmt
        elif state == StxJL.DT:
            s_fmt = StxJL.DT_fmt if pivot == 0 else StxJL.DN_piv_fmt
        elif pivot == 1:
            s_fmt = StxJL.UP_piv_fmt if state == StxJL.NRe else \
                    StxJL.DN_piv_fmt
        else:
            e_fmt = ''
        s_price = '{0:s}{1:9.2f}{2:s}'.format(s_fmt, price, e_fmt)
        return '{0:s}'.format(54 * ' ') if state == StxJL.Nil else \
            '{0:s}{1:s}{2:s}'.format((9 * state) * ' ', s_price,
                                     (9 * (5 - state)) * ' ')

    def jl_print(self, print_pivots_only=False, print_nils=False,
                 print_dbg=False):
        output = ''
        for jlr in self.jl_recs[1:]:
            state = jlr[self.col_ix['state']]
            pivot = jlr[self.col_ix['pivot']]
            price = jlr[self.col_ix['price']]
            if print_pivots_only and pivot == 0:
                continue
            if not print_nils and state == StxJL.Nil:
                continue
            px_str = self.get_formatted_price(state, pivot, price)
            output += '{0:s}{1:s}{2:6.2f} {3:s}\n'. \
                format(jlr[self.col_ix['dt']], px_str, jlr[self.col_ix['rg']],
                       '' if not print_dbg else self.jlr_print2(jlr))
            state2 = jlr[self.col_ix['state2']]
            if state2 == StxJL.Nil:
                continue
            pivot2 = jlr[self.col_ix['pivot2']]
            if print_pivots_only and pivot2 == 0:
                continue
            price2 = jlr[self.col_ix['price2']]
            px_str = self.get_formatted_price(state2, pivot2, price2)
            output += '{0:s}{1:s}{2:6.2f} {3:s}\n'.\
                format(jlr[self.col_ix['dt']], px_str, jlr[self.col_ix['rg']],
                       '' if not print_dbg else self.jlr_print2(jlr))
        print(output)

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
