#include <libpq-fe.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_ana.h"
#include "stx_core.h"


int main(int argc, char** argv) {
    bool rt_analysis = false;
    if (argc == 2) {
	rt_analysis = true;
	char* crt_busdate = cal_current_busdate(5);
	if (!strcmp(argv[1], "-expiry"))
	    ana_expiry_analysis(crt_busdate, rt_analysis);
	else if (!strcmp(argv[1], "-intraday"))
	    ana_intraday_analysis(crt_busdate, rt_analysis);
	else if (!strcmp(argv[1], "-eod"))
	    ana_eod_analysis(crt_busdate, leaders, ana_name);
    }
    char *crs_date = "2002-02-15";
    char *exp_date = "2002-02-16";
    char *exp_bdate;
    char *prev_exp_date = "2002-01-19";
    char *end_date = (char *) calloc((size_t)16, sizeof(char));
    char sql_cmd[128];
/*     strcpy(sql_cmd, "explain analyze select * from eods"); */
/*     PGresult *all_res = db_query(sql_cmd); */
/*     PQclear(all_res); */
/*     LOGINFO("got all records from eods\n"); */
    strcpy(sql_cmd, "select max(dt) from eods");
    PGresult *res = db_query(sql_cmd);
    int num = PQntuples(res);
    if (num < 1) {
	LOGERROR("No data in the 'eods' table. Exiting!\n");
	exit(-1);
    }
    strcpy(end_date, PQgetvalue(res, 0, 0));
    PQclear(res);
    printf("end_date = %s\n", end_date);
    int ix = cal_ix(crs_date), end_ix = cal_ix(end_date);
    int exp_ix = ix, exp_bix = cal_exp_bday(ix, &exp_bdate);
    cJSON *leaders = NULL;
    while(ix <= end_ix) {
	if (!strcmp(crs_date, exp_bdate)) {
	    if (leaders != NULL)
		cJSON_Delete(leaders);
	    exp_ix = cal_expiry(ix + 1, &exp_date);
	    exp_bix = cal_exp_bday(exp_ix, &exp_bdate);
/* 	    LOGINFO("%s: ana_expiry(%s)\n", crs_date, exp_date); */
	    ana_expiry_analysis(crs_date);
	    leaders = ana_get_leaders(exp_date, 500, 33, 0);
	}
	/** TODO: pass the setup type as a parameter **/
	ana_eod_analysis(crs_date, leaders, "JC_Pullback");
	ix = cal_next_bday(ix, &crs_date);
    }
    if (leaders != NULL)
	cJSON_Delete(leaders);
    /*
      v 1. Implement cal_exp_bday() function
      v 2. Retrieve max(dt) from eods.  This is the end_date
      v 3. Get start_ix and end_ix indices.
      v 4. while loop with stop condition start_ix <= end_ix
      5. if !strcmp(start_date, exp_bdate
             exp_ix = cal_expiry(ix + 1, &exp_date)
             exp_bix = cal_exp_bday(exp_ix, &exp_bdate)
	     ana_expiry()
       6. ana_eod()
       v 7. ix = cal_next_bday(ix, &start_date)
     */
    free(end_date);
}
