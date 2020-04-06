#include <cjson/cJSON.h>
#include <libpq-fe.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
#include "stx_score.h"


int main(int argc, char** argv) {
    char *tag_name = "S0", *start_date = cal_current_busdate(5),
        *end_date = cal_current_busdate(5);
    cJSON *stx = NULL;
    for (int ix = 1; ix < argc; ix++) {
        if (!strcmp(argv[ix], "--tag") && (ix++ < argc - 1))
            tag_name = argv[ix];
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
        }
    }
    LOGINFO("Scoring setups with tag %s from %s to %s\n", tag_name, start_date,
            end_date);
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
        score_setups(crs_date, stx, tag_name);
        ix = cal_next_bday(ix, &crs_date);
    }
    if (stx != NULL)
        cJSON_Delete(stx);
    LOGINFO("All Done!!!\n");
}
