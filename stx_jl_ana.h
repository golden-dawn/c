#ifndef __STX_ANA_H__
#define __STX_ANA_H__

#include <cjson/cJSON.h>
#include <curl/curl.h>
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

#define JL_060 "60"
#define JL_100 "100"
#define JL_150 "150"

static float jl_factors[4] = { 0.6, 1.0, 1.5, 2.0 };
static const char *jl_labels[] = { JL_060, JL_100, JL_150, JL_200 };

/* void ana_gaps_8(FILE* fp, char* stk, char* dt, jl_data_ptr jl_recs) { */
/*     daily_record_ptr dr = jl_recs->data->data; */
/*     int ix = jl_recs->data->pos; */
/*     bool res; */
/*     char setup_name[16]; */
/*     /\* Find gaps *\/ */
/*     if ((dr[ix].open > dr[ix - 1].high) || (dr[ix].open < dr[ix - 1].low)) { */
/* 	strcpy(setup_name,  (dr[ix].volume > 1.1 * jl_recs->recs[ix].volume)?  */
/* 	       "GAP_HV": "GAP"); */
/* 	fprintf(fp, "%s\t%s\t%s\t%c\t1\n", dt, stk, setup_name,  */
/* 		(dr[ix].open > dr[ix - 1].high)? UP: DOWN); */
/*     } */
/*     /\* Find strong closes on significant range and volume *\/ */
/*     if ((ts_true_range(jl_recs->data, ix) >= jl_recs->recs[ix].rg) &&  */
/* 	(dr[ix].volume >= jl_recs->recs[ix].volume)) { */
/* 	if (4 * dr[ix].close >= 3 * dr[ix].high + dr[ix].low) */
/* 	    fprintf(fp, "%s\t%s\tSTRONG_CLOSE\t%c\t1\n", dt, stk, UP); */
/* 	if (4 * dr[ix].close <= dr[ix].high + 3 * dr[ix].low) */
/* 	    fprintf(fp, "%s\t%s\tSTRONG_CLOSE\t%c\t1\n", dt, stk, DOWN); */
/*     }      */
/* } */

void ana_jl_setups(FILE* fp, char* stk, char* dt) {
    jl_data_ptr jl_recs[4];
    for (int ix = 0; ix < 4; ix++) {
	ht_item_ptr ht_jl = ht_get(ana_jl(jl_labels[ix]), stk);
	if (ht_jl == NULL) {
	    stx_data_ptr data = ts_load_stk(stk);
	    if (data == NULL) {
		LOGERROR("Could not load %s, skipping...\n", stk);
		return;
	    }
	    jl_recs[ix] = jl_jl(data, dt, JL_FACTOR);
	    ht_jl = ht_new_data(stk, (void*)jl_recs[ix]);
	    ht_insert(ana_jl(jl_labels[ix]), ht_jl);
	} else {
	    jl_recs[ix] = (jl_data_ptr) ht_jl->val.data;
	    jl_advance(jl_recs[ix], dt);
	}
    }
    ana_jl(fp, stk, dt, jl_recs);
}


void ana_jl_analysis(char* dt, bool eod) {
    /** This analysis runa intraday, or at eod. Prices are already downloaded.
     * 1. Get the leaders
     * 2. Run 4 JL calculations
     * 3. Detect double bottoms
     **/
    char *exp_date, sql_cmd[256], *filename = "/tmp/setups.csv";
    int exp_ix = cal_expiry(cal_ix(dt) + (eod? 1: 0), &exp_date);
    cJSON *ldr = NULL, *leaders = ana_get_leaders(exp_date, MAX_ATM_PRICE,
						  MAX_OPT_SPREAD, 0);
    sprintf(sql_cmd, "DELETE FROM setups WHERE dt='%s' AND setup IN "
	    "('JL2B', 'JLBREAKOUT')", dt);
    db_transaction(sql_cmd);
    int num = 0, total = cJSON_GetArraySize(leaders);
    FILE *fp = NULL;
    if ((fp = fopen(filename, "w")) == NULL) {
	LOGERROR("Failed to open file %s for writing\n", filename);
	fp = stderr;
    }
    num = 0;
    cJSON_ArrayForEach(ldr, leaders) {
	if (cJSON_IsString(ldr) && (ldr->valuestring != NULL))
	    ana_jl_setups(fp, ldr->valuestring, dt);
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
	fclose(fp);
    }
    cJSON_Delete(leaders);
}

#endif
