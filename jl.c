#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_jl.h"
#include "stx_ts.h"

int main(int argc, char** argv) {
    char stk[16], ed[16];
    strcpy(stk, "NFLX");
    strcpy(ed, "2019-05-24");
    float factor = 1.5;
    for (int ix = 1; ix < argc; ix++) {
	if (!strcmp(argv[ix], "-n") && (++ix < argc))
	    strcpy(stk, argv[ix]);
	else if (!strcmp(argv[ix], "-e") && (++ix < argc))
	    strcpy(ed, argv[ix]);
	else if (!strcmp(argv[ix], "-f") && (++ix < argc))
	    factor = atof(argv[ix]);
    }
    stx_data_ptr data = ts_load_stk(stk);
    jl_data_ptr jl = jl_jl(data, "2019-05-24", factor);
    jl_print(jl, false, false);
/*     pivs = jl.get_num_pivots(4) */
/*     print("4 pivs:") */
/*     jl.print_pivs(pivs) */
}
