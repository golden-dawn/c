#include "stx_jl.h"

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
    jl_data_ptr jl = jl_init20(data, factor);
    
    stk = sys.argv[1]
    sd = sys.argv[2]
    ed = sys.argv[3]
    dt = sys.argv[4]
    factor = float(sys.argv[5])
    ts = StxTS(stk, sd, ed)
    jl = StxJL(ts, factor)
    jlres = jl.jl(dt)
    jl.jl_print()
    pivs = jl.get_pivots_in_days(100)
    print("Pivs in 100 days:")
    jl.print_pivs(pivs)
    pivs = jl.get_num_pivots(4)
    print("4 pivs:")
    jl.print_pivs(pivs)
    # jl.jl_print(print_pivots_only = True)
    # pd.set_option('display.max_rows', 2000)
    # pd.set_option('display.max_columns', 1500)
    # pd.set_option('display.width', 1500)
    # print(jlres)
)
