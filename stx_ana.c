void expiry_analysis(char* dt) {
    /** 
     * special case when the date is an option expiry date
     * if the data is NULL, only run for the most recent business day
     * 1. wait until eoddata is downloaded. 
     * 2. calculate liquidity leaders
     * 3. download options for all liquidity leaders
     * 4. calculate option spread leaders
     * 5. populate leaders table
     **/
    /* 1. Get all the stocks as of a given date */
    /* 2. For each stock, get the data */
    /* 3. Set the time series to a specific date (dt) */
    /* 4. Get all the splits up to dt */
    /* 5. Adjust the data accordingly */
    /* 6. Run JL on the adjusted data */
    /* 7. Run the setups */
}


void eod_analysis(char* dt) {
    /** this runs at the end of the trading day.
     * 1. Get prices and options for hte leaders
     * 2. calculate eod setups
     * 3. email the results
     **/
}


void intraday_analysis() {
    /** this runs during the trading day
     * 1. download price data only for option spread leaders
     * 2. determine which EOD setups were triggered today
     * 3. Calculate intraday setups (?)
     * 4. email the results
     **/
}
