#include "stx_core.h"


/**
 * 1. Define a trade structure:

 typedef trade_t {
   char cp;
   char stk[16];
   char in_dt[16];
   char out_dt[16];
   char exp_dt[16];
   int in_spot;
   int in_range;
   int out_spot;
   float spot_pnl;
   int num_contracts;
   int strike;
   int in_ask;
   int out_bid;
   float opt_pnl;
   int moneyness;
 } trade, *trade_ptr;

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
}
