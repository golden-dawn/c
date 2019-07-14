#ifndef __STX_SETUP_H__
#define __STX_SETUP_H__

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
#include "stx_ts.h"

#define JC_1234      0x1
#define JC_5DAYS     0x2

#define JC_5D_DAYS 8
#define JC_5D_LB 30
#define JC_5D_UB 70

bool stp_jc_1234(daily_record_ptr data, int ix, int trend) {
    bool result = false;
    int inside_days = 0, ixx;
    if (trend > 0) {
	for(ixx = ix; ixx > ix - 2; ixx--) {
	    if (ixx < 0)
		break;
	    if (data[ixx].low > data[ixx - 1].low) {
		if (data[ixx].high < data[ixx - 1].high)
		    inside_days++;
		else
		    ixx = -1;
	    }
	}
	if ((ixx > 0) && (inside_days < 2))
	    result = true;
    } else {
	for(ixx = ix; ixx > ix - 2; ixx--) {
	    if (ixx < 0)
		break;
	    if (data[ixx].high < data[ixx - 1].high) {
		if (data[ixx].low > data[ixx - 1].low)
		    inside_days++;
		else
		    ixx = -1;
	    }
	}
	if ((ixx > 0) && (inside_days < 2))
	    result = true;
    }
    return result;
}

bool stp_jc_5days(daily_record_ptr data, int ix, int trend) {
    float min = 0, max = 0;
    if (ix < JC_5D_DAYS - 1)
	return false;
    for(int ixx = ix; ixx > ix - nb_days; ixx--) {
	if (max < data[ixx].high)
	    max = data[ixx].high;
	if (min > data[ixx].low)
	    min = data[ixx].low;
    }
    if (max == min)
	return false;
    float fs = 100 * (data[ix].close - min) / (max - min);
    if (((trend > 0) && (fs < JC_5D_LB)) || ((trend < 0) && (fs > JC_5D_UB)))
	return true;
    return false;
}
#endif
