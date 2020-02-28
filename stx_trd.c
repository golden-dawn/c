#include <libpq-fe.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
#include "stx_trd.h"


/**
 * 1. Define a trade structure, and a corresponding database table:

 * 2. Define stop_loss rules. Exit a trade if:
      a. date reaches expiry
      b. a day opposite the trade direction on higher than average volume
      c. two days opposite the trade direction with one strong close
   
 * 3. Load a stock, and iterate through the EOD records.

 * 4. When a setup is found:
      a. Set the time series day to the setup date
      b. Open a new trade record
      c. Iterate through the time series, and check the exit rules daily
      d. When an exit rule is triggered, 
          i. log the trade 
         ii. free the trade structure
**/

int main(int argc, char** argv) {
    char *stx = "", *setups = "JC_1234,JC_5DAYS", *start_date = "2002-02-15";
    char *end_date = (char *) calloc((size_t)16, sizeof(char)), sql_cmd[128];
    bool triggered = false;
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
        if (!strcmp(argv[ix], "--stocks") && (ix++ < argc - 1)) {
            stx = argv[ix];
            LOGINFO("stx = %s\n", stx);
        } else if (!strcmp(argv[ix], "--setups") && (ix++ < argc - 1)) {
            setups = argv[ix];
            LOGINFO("setups = %s\n", setups);
        } else if (!strcmp(argv[ix], "--start-date") && (ix++ < argc - 1)) {
            start_date = argv[ix];
            LOGINFO("start_date = %s\n", start_date);
        }
        else if (!strcmp(argv[ix], "--end-date") && (ix++ < argc - 1)) {
            strcpy(end_date, argv[ix]);
            LOGINFO("end_date = %s\n", end_date);
        }
        else if (!strcmp(argv[ix], "--triggered-only"))
            triggered = true;
    }
    LOGINFO("start_date = %s\n", start_date);
    LOGINFO("end_date = %s\n", end_date);
    LOGINFO("stx = %s\n", stx);
    LOGINFO("setups = %s\n", setups);
    trd_trade(start_date, end_date, stx, setups, triggered);
}
