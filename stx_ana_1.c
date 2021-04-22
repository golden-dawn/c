#include <cjson/cJSON.h>
#include <libpq-fe.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_ana.h"
#include "stx_core.h"

/**
 * This program can run in several modes:
 * 1. Cron - launched from a cron job at specific times during the business
 *    days. If launched on a holiday, the program will return immediately,
 *    otherwise, it will run real-time, in 'intraday', 'intraday-expiry', or
 *    'eod' modes.  First, code will download the spots (in 'intraday' mode),
 *    or the spots and the options (in 'intraday-expiry' or 'eod' modes).
 *    Then, it will run the daily analysis on the downloaded data.
 *    To launch the code in cron mode, specify the --cron parameter.  In 
 *    addition, one of these parameters should be specified: --intraday,
 *    --intraday-expiry, --eod.
 * 2. Real-time - same as cron, except that it will always run; if launched
 *    on a holiday, code will run for the previous business day.  It can run
 *    in intraday mode, where it will only download the spots and perform the
 *    analysis for today, in intraday-expiry mode, it will download the spots
 *    and the options.  In the eod mode, it will download the spots and the
 *    options, and it will run the analysis for today, and tomorrow.  To run
 *    the code without downloading any data, --no-rt parameter should be
 *    specified.
 * 3. Analysis - this will not download any data, but it will run the analysis
 *    for each date, in the interval between --start-date and --end-date
 *    parameters.  In addition, the --stx parameter allows running the analysis
 *    for a specific set of stocks (by default, the analysis runs for all the
 *    leaders, as returned by the get_leaders() function). The --stx parameter
 *    is a comma-separated list of stock names.
*/


int main(int argc, char** argv) {
    bool download_spots = false, download_options = false, rt_ana = false,
      eod = false, no_rt = false, run_expiry = false;
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
	else if (!strcmp(argv[ix], "--run-expiry"))
            run_expiry = true;
        else if (!strcmp(argv[ix], "--cron"))
            if (!cal_is_today_busday()) {
                LOGINFO("Will not run in batch mode on a holiday\n");
                return 0;
            }
    }
    char* crt_busdate = cal_current_busdate(5);
    LOGINFO("Current business date is: %s\n", crt_busdate);
    if ((ana_type != NULL) && !strcmp(ana_type, "intraday-expiry")) {
        LOGINFO("Running intraday expiry for %s\n", crt_busdate);
        ana_intraday_expiry(crt_busdate);
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
        LOGINFO("Running realtime %s analysis for %s\n", ana_type,
                crt_busdate);
        curl_global_init(CURL_GLOBAL_ALL);
        if (no_rt) {
            download_spots = false;
            download_options = false;
        }
        if (eod) {
            char *exp_date;
            cal_expiry(cal_ix(crt_busdate), &exp_date);
            if (!strcmp(crt_busdate, exp_date) || run_expiry)
                ana_expiry_analysis(crt_busdate, rt_ana, download_spots,
                                    download_options);
        }
        ana_stx_analysis(crt_busdate, stx, download_spots, download_options,
                         eod);
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
            ana_expiry_analysis(crs_date, rt_ana, download_spots,
                                download_options);
        }
        ana_stx_analysis(crs_date, stx, download_spots, download_options, eod);
        ix = cal_next_bday(ix, &crs_date);
    }
    if (stx != NULL)
        cJSON_Delete(stx);
    LOGINFO("All Done!!!\n");
}
