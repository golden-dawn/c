#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stx_jl.h"
#include "stx_ts.h"

int main(int argc, char** argv) {
    char stk[16], ed[16];
    strcpy(stk, "NFLX");
    strcpy(ed, "2019-05-17");
    float factor = 1.5;
    int num_pivots = 0, num_lines = 3;
    bool print_pivots_only = false, print_nils = false;

    for (int ix = 1; ix < argc; ix++) {
	if (!strcmp(argv[ix], "-s") && (++ix < argc))
	    strcpy(stk, argv[ix]);
	else if (!strcmp(argv[ix], "-e") && (++ix < argc))
	    strcpy(ed, argv[ix]);
	else if (!strcmp(argv[ix], "-f") && (++ix < argc))
	    factor = atof(argv[ix]);
	else if (!strcmp(argv[ix], "-n") && (++ix < argc))
	    num_pivots = atoi(argv[ix]);
	else if (!strcmp(argv[ix], "-l") && (++ix < argc))
	    num_lines = atoi(argv[ix]);
	else if (!strcmp(argv[ix], "-ppo"))
	    print_pivots_only = true;
	else if (!strcmp(argv[ix], "-pnils"))
	    print_nils = true;
    }
    stx_data_ptr data = ts_load_stk(stk);
    jl_data_ptr jl = jl_jl(data, ed, factor);
    jl_print(jl, print_pivots_only, print_nils);
    for(int ix = jl->pos - num_lines; ix < jl->pos; ix++) {
	ts_print_record(&(jl->data->data[ix]));
	fprintf(stderr, "\n");
    }
    int num, ixx;
    jl_print_pivots(jl, num_pivots, &num);

    jl_pivot_ptr pivots = jl_get_pivots(jl, 4, &num);
    for(ixx = 0; ixx < num - 1; ixx++)
	jl_print_rec(pivots[ixx].date, pivots[ixx].state, pivots[ixx].price, 
		     true, pivots[ixx].rg, pivots[ixx].obv);
    jl_print_rec(pivots[ixx].date, pivots[ixx].state, pivots[ixx].price, 
		 false, pivots[ixx].rg, pivots[ixx].obv);
}
