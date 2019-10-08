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
	if (!strcmp(argv[1], "-expiry")) {
	    curl_global_init(CURL_GLOBAL_ALL);
	    ana_expiry_analysis(crt_busdate, rt_analysis);
	    curl_global_cleanup();
	    return 0;
	}
	else if (!strcmp(argv[1], "-intraday")) {
	    curl_global_init(CURL_GLOBAL_ALL);
	    ana_intraday_analysis(crt_busdate, false);
	    curl_global_cleanup();
	    return 0;
	}
	else if (!strcmp(argv[1], "-eod")) {
	    curl_global_init(CURL_GLOBAL_ALL);
	    ana_intraday_analysis(crt_busdate, true);
	    curl_global_cleanup();
	    return 0;
	}
    }
    char ana_name[64];
    memset(ana_name, 0, 64);
    strcpy(ana_name, "JC_Pullback");
/*     char *crs_date = "2002-02-15",  *exp_date = "2002-02-16", *exp_bdate; */
    char *crs_date = (char *) calloc((size_t)16, sizeof(char));
    char *exp_date = (char *) calloc((size_t)16, sizeof(char)), *exp_bdate;
    char *end_date = (char *) calloc((size_t)16, sizeof(char)), sql_cmd[128];

    strcpy(sql_cmd, "select max(dt) from eods");
    PGresult *res = db_query(sql_cmd);
    int num = PQntuples(res);
    if (num < 1) {
	LOGERROR("No data in the 'eods' table. Exiting!\n");
	exit(-1);
    }
    strcpy(end_date, PQgetvalue(res, 0, 0));
    PQclear(res);
    strcpy(crs_date, "2002-02-15");
    strcpy(exp_date, "2002-02-16");

    printf("argc = %d\n", argc);
    for (int ix = 1; ix < argc; ix++) {
	printf("argv[%d] = %s\n", ix, argv[ix]);
	if (!strcmp(argv[ix], "--ana-name") && (ix++ < argc - 1)) {
	    memset(ana_name, 0, 64);
	    strcpy(ana_name, argv[ix]);
	    printf("ana_name = %s\n", ana_name);
	} else if (!strcmp(argv[ix], "--start-date") && (ix++ < argc - 1)) {
	    fprintf(stderr, "ix = %d\n", ix);
	    fprintf(stderr, "ix = %d, crs_date = %s\n", ix, argv[ix]);
	    strcpy(crs_date, argv[ix]);
	    fprintf(stderr, "crs_date = %s\n", crs_date);
	}
	else if (!strcmp(argv[ix], "--end-date") && (ix++ < argc - 1)) {
	    strcpy(end_date, argv[ix]);
	    printf("end_date = %s\n", end_date);
	}

    }
    LOGINFO("crs_date = %s\n", crs_date);
    LOGINFO("end_date = %s\n", end_date);
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
	    ana_expiry_analysis(crs_date, false);
	    leaders = ana_get_leaders(exp_date, MAX_ATM_PRICE,
				      MAX_OPT_SPREAD, 0);
	}
	ana_eod_analysis(crs_date, leaders, ana_name);
	ix = cal_next_bday(ix, &crs_date);
    }
    if (leaders != NULL)
	cJSON_Delete(leaders);
    free(end_date);
    free(crs_date);
    free(exp_date);
}
