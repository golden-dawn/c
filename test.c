#include <assert.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "stx_core.h"
#include "stx_ts.h"

#define RED   "\x1B[31m"
#define GRN   "\x1B[32m"
#define PRED   "\x1B[4;31m"
#define PGRN   "\x1B[4;32m"
#define RESET "\x1B[0m"


int print_num_busdays(char* sd, char* ed) {
    int num_busdays = cal_num_busdays(sd, ed);
    LOGINFO("The number of business days between %s and %s is: %d\n",
	    sd, ed, num_busdays);
    return num_busdays;
}

char* print_next_busday(char* date) {
    char* res;
    int crt_ix = cal_ix(date);
    crt_ix = cal_next_bday(crt_ix, &res);
    LOGINFO("Next business day after %s is %s\n", date, res);
    return res;
}

char* print_prev_busday(char* date) {
    char* res;
    int crt_ix = cal_ix(date);
    crt_ix = cal_prev_bday(crt_ix, &res);
    LOGINFO("Previous business day before %s is %s\n", date, res);
    return res;
}

int find_record_date(stx_data_ptr data, char* date, int rel_pos) {
    int ix = ts_find_date_record(data, date, rel_pos);
    if (ix == -1) 
	LOGINFO("Date %s not found\n", date);
    else
	LOGINFO("The index for date %s is %d and the date is %s\n", 
		date, ix, data->data[ix].date);
    return ix;
}

int check_split_sequence(stx_data_ptr data, char* date) {
    int ress = ht_seq_index(data->splits, date);
    LOGINFO("ress = %d\n", ress);
    return ress;
}

int find_true_range(stx_data_ptr data, char* date) { 
    int ix_tr = ts_find_date_record(data, date, 0);
    int tr = ts_true_range(data, ix_tr);
    LOGINFO("%6s: %s: true_range is %d\n", data->stk, 
	    data->data[ix_tr].date, tr);
    return tr;
}

int main(int argc, char** argv) {
    LOGINFO("sizeof(int) = %lu\n", sizeof(int));
    LOGINFO("sizeof(short) = %lu\n", sizeof(short));
    LOGWARN("sizeof(long) = %lu\n", sizeof(long));

    assert(print_num_busdays("2019-05-17", "2019-05-19") == 1);
    assert(print_num_busdays("2019-05-17", "2019-05-20") == 2);
    assert(print_num_busdays("2019-05-17", "2019-05-23") == 5);
    assert(print_num_busdays("2019-05-17", "2019-05-26") == 6);

    assert(print_num_busdays("2019-05-18", "2019-05-19") == 0);
    assert(print_num_busdays("2019-05-18", "2019-05-20") == 1);
    assert(print_num_busdays("2019-05-18", "2019-05-23") == 4);
    assert(print_num_busdays("2019-05-18", "2019-05-26") == 5);

    assert(print_num_busdays("2019-05-19", "2019-05-18") == 0);
    assert(print_num_busdays("2019-05-19", "2019-05-17") == -1);
    assert(print_num_busdays("2019-05-19", "2019-05-15") == -3);
    assert(print_num_busdays("2019-05-19", "2019-05-12") == -5);

    assert(print_num_busdays("2019-05-20", "2019-05-18") == -1);
    assert(print_num_busdays("2019-05-20", "2019-05-17") == -2);
    assert(print_num_busdays("2019-05-20", "2019-05-15") == -4);
    assert(print_num_busdays("2019-05-20", "2019-05-12") == -6);

    assert(print_num_busdays("2019-05-17", "2019-05-17") == 1);
    assert(print_num_busdays("2019-05-18", "2019-05-18") == 0);
    assert(print_num_busdays("2018-01-01", "2019-01-01") == 251);

    assert(strcmp(print_next_busday("2019-05-15"), "2019-05-16") == 0);
    assert(strcmp(print_next_busday("2019-05-16"), "2019-05-17") == 0);
    assert(strcmp(print_next_busday("2019-05-17"), "2019-05-20") == 0);
    assert(strcmp(print_next_busday("2019-05-18"), "2019-05-20") == 0);
    assert(strcmp(print_next_busday("2019-05-19"), "2019-05-20") == 0);
    assert(strcmp(print_next_busday("2019-05-20"), "2019-05-21") == 0);
    assert(strcmp(print_next_busday("2019-05-21"), "2019-05-22") == 0);
    assert(strcmp(print_next_busday("2019-05-22"), "2019-05-23") == 0);

    assert(strcmp(print_prev_busday("2019-05-15"), "2019-05-14") == 0);
    assert(strcmp(print_prev_busday("2019-05-16"), "2019-05-15") == 0);
    assert(strcmp(print_prev_busday("2019-05-17"), "2019-05-16") == 0);
    assert(strcmp(print_prev_busday("2019-05-18"), "2019-05-17") == 0);
    assert(strcmp(print_prev_busday("2019-05-19"), "2019-05-17") == 0);
    assert(strcmp(print_prev_busday("2019-05-20"), "2019-05-17") == 0);
    assert(strcmp(print_prev_busday("2019-05-21"), "2019-05-20") == 0);
    assert(strcmp(print_prev_busday("2019-05-22"), "2019-05-21") == 0);
    assert(strcmp(print_prev_busday("1985-01-02"), "1984-12-31") == 0);

    stx_data_ptr data = ts_load_stk("NFLX");
    assert(find_record_date(data, "2002-05-24", 0) == 1);

    char *sd = "2019-05-17";
    char *ed = "2019-05-17";
    if (sd == ed)
	LOGDEBUG("sd == ed\n");
    else
	LOGDEBUG("False");

    fprintf(stderr, "\x1b[1;32;40m This is green \x1b[0m  \n");
    fprintf(stderr, "\x1b[1;31;40m This is red \x1b[0m  \n");
    fprintf(stderr, "\x1b[4;32;40m This is underline green \x1b[0m  \n");
    fprintf(stderr, "\x1b[4;31;40m This is underline red \x1b[0m  \n");

    ts_free_data(data);

    data = ts_load_stk("MSFT");
    assert(check_split_sequence(data, "1986-09-09") == -1);
    assert(check_split_sequence(data, "1987-09-17") == -1);
    assert(check_split_sequence(data, "1987-09-18") == 0);
    assert(check_split_sequence(data, "1987-09-19") == 0);
    assert(check_split_sequence(data, "1990-04-12") == 1);
    assert(check_split_sequence(data, "1991-06-26") == 2);
    assert(check_split_sequence(data, "1992-06-12") == 3);
    assert(check_split_sequence(data, "1994-05-20") == 4);
    assert(check_split_sequence(data, "1996-12-06") == 5);
    assert(check_split_sequence(data, "1998-02-20") == 6);
    assert(check_split_sequence(data, "1999-03-26") == 7);
    assert(check_split_sequence(data, "1999-09-09") == 7);
    assert(check_split_sequence(data, "2003-02-14") == 8);
    assert(check_split_sequence(data, "2004-11-11") == 8);
    assert(check_split_sequence(data, "2004-11-12") == 9);
    assert(check_split_sequence(data, "2004-11-13") == 9);
    assert(check_split_sequence(data, "2019-02-14") == 9);

    assert(find_true_range(data, "1986-03-13") == 320);
    assert(find_true_range(data, "1986-10-27") == 177);
    assert(find_true_range(data, "1987-01-13") == 320);
    assert(find_true_range(data, "1986-11-24") == 640);
/* stx=# select * from eods where stk='MSFT' and dt='1987-09-18'; */
/*  stk  |     dt     |   o   |  hi   |  lo   |   c   |  v  | oi  */
/* ------+------------+-------+-------+-------+-------+-----+---- */
/*  MSFT | 1987-09-18 | 11521 | 11521 | 11379 | 11521 | 110 |  0 */

    ts_print(data, "1987-09-18", "1987-09-18");
    ts_print(data, "1990-04-12", "1990-04-12");
    ts_set_day(data, "1987-09-18", 0);
    ts_print(data, "1987-09-18", "1987-09-18");
    ts_print(data, "1990-04-12", "1990-04-12");
    ts_set_day(data, "1990-04-12", 0);
    ts_print(data, "1987-09-18", "1987-09-18");
    ts_print(data, "1990-04-12", "1990-04-12");
    ts_set_day(data, "1994-05-20", 0);
    ts_print(data, "1987-09-18", "1987-09-18");
    ts_print(data, "1990-04-12", "1990-04-12");

    ts_free_data(data);

    data = ts_load_stk("X");
    assert(check_split_sequence(data, "1986-09-09") == -2);
    assert(check_split_sequence(data, "1998-02-20") == -2);
    assert(check_split_sequence(data, "2003-02-14") == -2);
    assert(check_split_sequence(data, "2019-02-14") == -2);

    ts_free_data(data);

    int* rgs = (int *) calloc(20, sizeof(int));
    for(int ix = 0; ix < 20; ix++)
	rgs[ix] = ix;
    int avg_rg = 0;
    for(int ix = 0; ix < 20; ix++)
	avg_rg += rgs[ix];
    avg_rg = (int) (avg_rg / 20.0);
    LOGINFO("avg_rg = %d\n", avg_rg);
    /* This doesn't work: */
/*     int lp[8]; */
/*     lp = [1000, 1000, 1000, 900, 900, 900, 1000, 900]; */
    int lp[8];
    lp[0] = (lp[1] = (lp[2] = (lp[6] = 1000)));
    lp[3] = (lp[4] = (lp[5] = (lp[7] = 900)));
    for(int ix = 0; ix < 8; ix++)
	fprintf(stderr, "%5d", lp[ix]);

    char* ut_fmt = "\x1b[1;32;40m";
    char* e_fmt = "\x1b[0m";
    int color_price = 4000;

    fprintf(stderr, "%s%6d%s\n", ut_fmt, color_price, e_fmt);
    fprintf(stderr, "%10s", " ");
    fprintf(stderr, RED "%10d%s\n", color_price, RESET);
    printf(GRN "green\n" RESET);
    printf(PRED "pivot-red\n" RESET);
    printf(PGRN "pivot-green\n" RESET);

    char* dt = "2019-06-14";
    char* exp_date;
    cal_expiry(cal_ix(dt), &exp_date);
    printf("%s: exp_date = %s\n", dt, exp_date);
    assert(!strcmp(exp_date, "2019-06-21"));
    dt = "2019-06-21";
    cal_expiry(cal_ix(dt), &exp_date);
    printf("%s: exp_date = %s\n", dt, exp_date);
    assert(!strcmp(exp_date, "2019-06-21"));
    dt = "2019-06-22";
    cal_expiry(cal_ix(dt), &exp_date);
    printf("%s: exp_date = %s\n", dt, exp_date);
    assert(!strcmp(exp_date, "2019-07-19"));
    dt = "2019-04-01";
    cal_expiry(cal_ix(dt), &exp_date);
    printf("%s: exp_date = %s\n", dt, exp_date);
    assert(!strcmp(exp_date, "2019-04-18"));
    dt = "2015-01-01";
    cal_expiry(cal_ix(dt), &exp_date);
    printf("%s: exp_date = %s\n", dt, exp_date);
    assert(!strcmp(exp_date, "2015-01-17"));
    dt = "2015-02-01";
    cal_expiry(cal_ix(dt), &exp_date);
    printf("%s: exp_date = %s\n", dt, exp_date);
    assert(!strcmp(exp_date, "2015-02-20"));
    dt = "2002-01-01";
    cal_expiry(cal_ix(dt), &exp_date);
    printf("%s: exp_date = %s\n", dt, exp_date);
    assert(!strcmp(exp_date, "2002-01-19"));
    dt = "2002-02-01";
    cal_expiry(cal_ix(dt), &exp_date);
    printf("%s: exp_date = %s\n", dt, exp_date);
    assert(!strcmp(exp_date, "2002-02-16"));
    dt = "2002-03-01";
    cal_expiry(cal_ix(dt), &exp_date);
    printf("%s: exp_date = %s\n", dt, exp_date);
    assert(!strcmp(exp_date, "2002-03-16"));

}
