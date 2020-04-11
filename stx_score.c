#include <cjson/cJSON.h>
#include <libpq-fe.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
#include "stx_score.h"


int main(int argc, char** argv) {

    char *tag_name = "S0", *start_date = "2002-02-01", *end_date = NULL;
    cJSON *stx = NULL;
    end_date = cal_current_busdate(5);
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

    char sql_cmd[2048], tf[128], *stk = NULL, *s_date = NULL, *e_date = NULL;
    sprintf(sql_cmd, "SELECT stk, min(dt), max(dt) FROM setup_scores WHERE "
            "dt BETWEEN '%s' AND '%s'", start_date, end_date);
    if (stx != NULL) {
        sprintf(sql_cmd, "%s AND stk IN (", sql_cmd);
        cJSON *stock = NULL;
        cJSON_ArrayForEach(stock, stx) {
        if (cJSON_IsString(stock) && (stock->valuestring != NULL))
            sprintf(sql_cmd, "%s'%s',", sql_cmd, stock->valuestring);
        sql_cmd[strlen(sql_cmd) - 1] = ')';
    }
    sprintf(sql_cmd, "%s GROUP BY stk", sql_cmd);

    PGresult* res = db_query(sql_cmd);
    int rows = PQntuples(res);
    LOGINFO("Scoring setups with tag %s from %s to %s for %d stocks\n",
            tag_name, start_date, end_date, rows);
    for(int ix = 0; ix < rows; ix++) {
        stk = PQgetvalue(res, ix, 0);
        s_date = PQgetvalue(res, ix, 1);
        e_date = PQgetvalue(res, ix, 2);
        score_leader_setups(stk, s_date, e_date, tag_name);
        if (ix > 0 && ix % 100 == 0)
            LOGINFO("Analyzed %5d/%5d leaders\n", ix, rows);
    }
    LOGINFO("Analyzed %5d/%5d leaders\n", rows, rows);
    PQclear(res);
    if (stx != NULL)
        cJSON_Delete(stx);
    LOGINFO("All Done!!!\n");
}
