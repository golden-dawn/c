#include <cjson/cJSON.h>
#include <libpq-fe.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_ana.h"
#include "stx_core.h"


int main(int argc, char** argv) {
    /**
     * TODO: 
     * 1. Why this dont pickup the right database(stx, instead of stx_test)
     * 2. Why there are no setups for today, only for yesterday.
     * 3. Uncomment SR setup retrievals
    */
    bool download_spots = false, download_options = false, rt_ana = false,
        eod = false, no_rt = false;
    char ana_name[32], *ana_type = NULL, *start_date = cal_current_busdate(5),
        *end_date = cal_current_busdate(5);
    cJSON *stx = NULL;
    strcpy(ana_name, "JL_Analysis");
    for (int ix = 1; ix < argc; ix++) {
        if (!strcmp(argv[ix], "--ana") && (ix++ < argc - 1))
            strcpy(ana_name, argv[ix]);
        else if (!strcmp(argv[ix], "--start-date") && (ix++ < argc - 1))
            start_date = cal_move_to_bday(argv[ix], true);
        else if (!strcmp(argv[ix], "--end-date") && (ix++ < argc - 1))
            end_date = cal_move_to_bday(argv[ix], false);
        else if (!strcmp(argv[ix], "--stx") && (ix++ < argc - 1)) {
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
        } else if (!strcmp(argv[ix], "--intraday")) {
            eod = false;
            download_spots = true;
            download_options = false;
            rt_ana = true;
            ana_type = argv[ix] + 2;
        } else if (!strcmp(argv[ix], "--eod")) {
            eod = true;
            download_spots = true;
            download_options = true;
            rt_ana = true;
            ana_type = argv[ix] + 2;
        } else if (!strcmp(argv[ix], "--intraday-expiry")) {
            eod = false;
            download_spots = true;
            download_options = true;
            rt_ana = true;
            ana_type = argv[ix] + 2;
        }  else if (!strcmp(argv[ix], "--no-rt"))
            no_rt = true;
        else if (!strcmp(argv[ix], "--cron"))
            if (!cal_is_today_busday()) {
                LOGINFO("Will not run in batch mode on a holiday\n");
                return 0;
            }
    }
    char* crt_busdate = cal_current_busdate(5);
    if (!strcmp(ana_type, "intraday-expiry")) {
        char *exp_date;
        cal_expiry(cal_ix(crt_busdate), &exp_date);
        if (!strcmp(crt_busdate, exp_date)) {
            ana_stx_analysis(crt_busdate, stx, download_spots,
                             download_options, eod, false);
        } else
            LOGINFO("%s not an expiration date.  Skip option download\n",
                    crt_busdate );
        if (stx != NULL)
            cJSON_Delete(stx);
        return 0;
    }
    if (!strcmp(start_date, crt_busdate) && !strcmp(end_date, crt_busdate) &&
        !no_rt) {
        rt_ana = true;
        download_spots = true;
        if (ana_type == NULL)
            ana_type = eod? "eod": "intraday";
    }
    if (rt_ana) {
        LOGINFO("Running realtime %s analysis for %s\n", ana_type, crt_busdate);
        curl_global_init(CURL_GLOBAL_ALL);
        if (eod) {
            char *exp_date;
            cal_expiry(cal_ix(crt_busdate), &exp_date);
            if (!strcmp(crt_busdate, exp_date))
                ana_expiry_analysis(crt_busdate, download_options);
        }
        ana_stx_analysis(crt_busdate, stx, download_spots, download_options,
                         eod, true);
        curl_global_cleanup();
        if (stx != NULL)
            cJSON_Delete(stx);
        return 0;
    }
    download_options = false;
    download_spots = false;
    eod = true;
    char *crs_date = start_date, *exp_date = "2002-02-16", *exp_bdate;
    if (strcmp(crs_date, "2002-02-15") < 0)
        crs_date = cal_move_to_bday("2002-02-15", false);
    // char sql_cmd[128];
    // strcpy(sql_cmd, "select max(dt) from eods");
    // PGresult *res = db_query(sql_cmd);
    // int num = PQntuples(res);
    // if (num < 1) {
    //     LOGERROR("No data in the 'eods' table. Exiting!\n");
    //     exit(-1);
    // }
    // end_date = cal_move_to_bday(PQgetvalue(res, 0, 0), false);
    // PQclear(res);
    LOGINFO("Running historical analysis %s from %s to %s\n", ana_name,
            start_date, end_date);
    if (stx == NULL) 
        LOGINFO(" for all leaders\n");
    else
        LOGINFO(" for %d stocks\n", cJSON_GetArraySize(stx));
    int ix = cal_ix(crs_date), end_ix = cal_ix(end_date);
    int exp_ix = cal_expiry(ix, &exp_date);
    int exp_bix = cal_exp_bday(exp_ix, &exp_bdate);
    while(ix <= end_ix) {
        if (!strcmp(crs_date, exp_bdate)) {
            exp_ix = cal_expiry(ix + 1, &exp_date);
            exp_bix = cal_exp_bday(exp_ix, &exp_bdate);
            ana_expiry_analysis(crs_date, false);
        }
        ana_stx_analysis(crs_date, stx, download_spots, download_options, eod,
                         true);
        ix = cal_next_bday(ix, &crs_date);
    }
    if (stx != NULL)
        cJSON_Delete(stx);
    LOGINFO("All Done!!!\n");
}
