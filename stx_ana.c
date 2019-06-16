#include "stx_ana.h"
#include "stx_core.h"

int main(int argc, char** argv) {
    char *start_date = "2002-02-17";
    /*
      1. Implement cal_exp_bday() function
      2. Retrieve max(dt) from eods.  This is the end_date
      3. Get start_ix and end_ix indices.
      4. while loop with stop condition start_ix <= end_ix
      5. if !strcmp(start_date, exp_bdate
             exp_ix = cal_expiry(ix + 1, &exp_date)
             exp_bix = cal_exp_bday(exp_ix, &exp_bdate)
	     ana_expiry()
       6. ana_eod()
       7. ix = cal_next_bday(ix, &start_date)
     */
}
