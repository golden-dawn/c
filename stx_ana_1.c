#include <cjson/cJSON.h>
#include <libpq-fe.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_ana.h"
#include "stx_core.h"


int main(int argc, char** argv) {
    bool rt_analysis = true, eod = true, expiry_only = false, hist_analysis = false;
    char ana_name[32];
    char *start_date = cal_current_busdate(5);
    char *end_date = cal_current_busdate(5);
    cJSON *stx = NULL;

    for (int ix = 1; ix < argc; ix++) {
        if (!strcmp(argv[ix], "--ana") && (ix++ < argc - 1)) {
            strcpy(ana_name, argv[ix]);
            rt_analysis = false;
            LOGINFO("ana_name = %s\n", ana_name);
        } else if (!strcmp(argv[ix], "--start-date") && (ix++ < argc - 1)) {
            cal_move_bdays(argv[ix], 0, &start_date);
            LOGINFO("start_date = %s\n", start_date);
        } else if (!strcmp(argv[ix], "--end-date") && (ix++ < argc - 1)) {
            cal_move_bdays(argv[ix], 0, &end_date);
            LOGINFO("end_date = %s\n", end_date);
        } else if (!strcmp(argv[ix], '--stx') && (ix++ < argc - 1)) {
            stx = cJSON_CreateArray();
            if (stx == NULL) {
                LOGERROR("Failed to create stx cJSON Array.\n");
                exit(-1);
            }
            cJSON *stk_name = NULL;
            char* token = strtok(argv[ix], ",");
            while (token) {
                stk_name = cJSON_CreateString(token);
                if (stk_name == NULL) {
                    LOGERROR("Failed to create cJSON string for %s\n", token);
                    continue;
                }
                cJSON_AddItemToArray(stx, stk_name);
                token = strtok(NULL, ",");
            }
        } else if (!strcmp(argv[ix], "--expiry")) {
            expiry_only = true;
            /** TODO: how do we identify realtime expiry analysis? */
            rt_analysis = true;
        } else if (!strcmp(argv[ix], "--intraday")) {
            eod = false;
            rt_analysis = true;
        }
        else if (!strcmp(argv[ix], "--eod")) {

        }

    }



    if (argc == 2) {
        rt_analysis = true;
        char* crt_busdate = cal_current_busdate(5);
        if (!strcmp(argv[1], "-expiry")) {
            curl_global_init(CURL_GLOBAL_ALL);
            ana_expiry_analysis(crt_busdate, rt_analysis);
            curl_global_cleanup();
            return 0;
        } else if (!strcmp(argv[1], "-intraday")) {
            curl_global_init(CURL_GLOBAL_ALL);
            ana_daily_analysis(crt_busdate, false, true);
            curl_global_cleanup();
            return 0;
        } else if (!strcmp(argv[1], "-eod")) {
            curl_global_init(CURL_GLOBAL_ALL);
            char *exp_date;
            cal_expiry(cal_ix(crt_busdate), &exp_date);
            if (!strcmp(crt_busdate, exp_date))
                ana_expiry_analysis(crt_busdate, true);
            ana_daily_analysis(crt_busdate, true, true);
            curl_global_cleanup();
            return 0;
        } else if (!strcmp(argv[1], "-ana")) {
            ana_daily_analysis(crt_busdate, true, false);
            return 0;
        }
    }

    char *ana_name = "eod", *crs_date = "2002-02-15";
    char *exp_date = "2002-02-16", *exp_bdate;
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
    for (int ix = 1; ix < argc; ix++) {
        if (!strcmp(argv[ix], "--ana-name") && (ix++ < argc - 1)) {
            ana_name = argv[ix];
            LOGINFO("ana_name = %s\n", ana_name);
        } else if (!strcmp(argv[ix], "--start-date") && (ix++ < argc - 1)) {
            crs_date = argv[ix];
            LOGINFO("start_date = %s\n", crs_date);
        }
        else if (!strcmp(argv[ix], "--end-date") && (ix++ < argc - 1)) {
            strcpy(end_date, argv[ix]);
            LOGINFO("end_date = %s\n", end_date);
        }
    }
    LOGINFO("start_date = %s\n", crs_date);
    LOGINFO("end_date = %s\n", end_date);
    int ix = cal_ix(crs_date), end_ix = cal_ix(end_date);
    int exp_ix = ix, exp_bix = cal_exp_bday(ix, &exp_bdate);
    while(ix <= end_ix) {
        if (!strcmp(ana_name, "eod"))
            ana_daily_analysis(crs_date, true, false);
        else if (!strcmp(ana_name, "expiry")) {
            if (!strcmp(crs_date, exp_bdate)) {
                exp_ix = cal_expiry(ix + 1, &exp_date);
                exp_bix = cal_exp_bday(exp_ix, &exp_bdate);
                ana_expiry_analysis(crs_date, false);
            }
        }
        ix = cal_next_bday(ix, &crs_date);
    }
    free(end_date);
    LOGINFO("All Done!!!\n");
}
