#ifndef __STX_SETUP_H__
#define __STX_SETUP_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_core.h"
#include "stx_ts.h"

#define JC_1234      0x1
#define JC_5DAYS     0x2

int stp_jc_1234(daily_record_ptr data, int ix, int trend) {
    int result = 0, inside_days = 0, ixx;
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
	    result = JC_1234;
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
	    result= -JC_1234;
    }
    return result;
}


#endif
