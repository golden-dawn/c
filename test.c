#include <assert.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "stx_core.h"
#include "stx_ts.h"

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

    ts_free_data(data);
}
