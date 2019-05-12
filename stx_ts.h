#ifndef __STX_TS_H__
#define __STX_TS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <time.h>
#include "stx_db.h"
#include "stx_ht.h"

/** BEGIN: macros */
#define module( x) (( x>  0)? x: -x)
#define sign( x) (( x> 0)? 1: -1)
/** END: macros */

typedef struct daily_record_t {
    int open;
    int high;
    int low;
    int close;
    int volume;
    char date[ 16];
} daily_record, *daily_record_ptr;


typedef struct stx_data_t {
    daily_record_ptr data;
    int num_recs;
    hashtable_ptr splits;
} stx_data, *stx_data_ptr;


void print_timestamp() {
    long milliseconds;
    time_t seconds;
    struct timespec spec;
    char buff[20];
    clock_gettime(CLOCK_REALTIME, &spec);
    seconds = spec.tv_sec;
    milliseconds = round(spec.tv_nsec / 1.0e6);
    if (milliseconds > 999) {
	seconds++;
	milliseconds = 0;
    }
    strftime(buff, 20, "%Y-%m-%d %H:%M:%S", localtime(&seconds));
    fprintf(stderr, "%s.%ld [] ", buff, milliseconds);
}


float wprice( daily_record_ptr data, int ix) {
    return ( data[ ix].close+ data[ ix].high+ data[ ix].low)/ 3;
}


hashtable_ptr load_splits(char* stk) {
    char sql_cmd[80];
    sprintf(sql_cmd, "select ratio, dt from dividends where stk='%s' "
	    "order by dt", stk);
    PGresult *res = db_query(sql_cmd);
#ifdef DEBUG
    fprintf(stderr, "Found %d splits for %s\n", PQntuples(res), stk);
#endif
    hashtable_ptr result = ht_divis(res);
#ifdef DEBUG
    fprintf(stderr, "Loaded splits for %s\n", stk);
#endif
    PQclear(res);
    return result;
}


stx_data_ptr load_stk(char* stk) {
#ifdef DEBUG
    fprintf(stderr, "Loading data for %s\n", stk);
#endif
    stx_data_ptr data = (stx_data_ptr) malloc(sizeof(stx_data));
    data->data = NULL;
    data->num_recs = 0;
    char sql_cmd[80];
    sprintf(sql_cmd, "select o, hi, lo, c, v, dt from eods where stk='%s' "
	    "order by dt", stk);
    PGresult *res = db_query(sql_cmd);
    if((data->num_recs = PQntuples(res)) <= 0) 
	return data;
    int num = data->num_recs;
#ifdef DEBUG
    fprintf(stderr, "Found %d records for %s\n", num, stk);
#endif
    data->data = (daily_record_ptr) calloc(num, sizeof(daily_record));
    for(int ix = 0; ix < num; ix++) {
	data->data[ix].open = atoi(PQgetvalue(res, ix, 0));
	data->data[ix].high = atoi(PQgetvalue(res, ix, 1));
	data->data[ix].low = atoi(PQgetvalue(res, ix, 2));
	data->data[ix].close = atoi(PQgetvalue(res, ix, 3));
	data->data[ix].volume = atoi(PQgetvalue(res, ix, 4));
	strcpy(data->data[ix].date, PQgetvalue(res, ix, 5)); 
    }
    PQclear(res);
#ifdef DEBUG
    fprintf(stderr, "Loading the splits for %s\n", stk);
#endif
    data->splits = load_splits(stk);
#ifdef DEBUG
    fprintf(stderr, "Done loading %s\n", stk);
#endif
    return data;
}


int find_date_record(stx_data_ptr data, char* date) {
    char* first_date = data->data[0].date;
    return 0;
}
/*     int num_days = stx_time_num_bus_days(first_date, date); */

/*     time_t date_time; */
/*     time_t crs_time; */
/*     int i = 0, j = lines- 1, mid = (i + j) / 2; */

/*     if( !strcmp( data[ j].date, date)) */
/* 	return j; */

/*     date_time= get_time_from_date(date); */
/*     while( i<= j) { */
/* 	crs_time= get_time_from_date( data[ mid].date); */
/* 	if( crs_time< date_time) */
/* 	    i= mid+ 1; */
/* 	else if( crs_time> date_time) */
/* 	    j= mid- 1; */
/* 	else */
/* 	    return mid; */
/* 	mid= ( i+ j)/ 2; */
/*     } */
/*     return -1; */
/* } */

/* data_fragment_ptr get_fragment( daily_record_ptr data, int nb_lines, */
/* 				time_frame_ptr tf) { */

/*     data_fragment_ptr dtf=       NULL; */
/*     int               start_ix; */
/*     int               end_ix; */
  
/*     start_ix= find_date_record( data, nb_lines, tf->start_date); */
/*     end_ix= find_date_record( data, nb_lines, tf->end_date); */

/*     if(( start_ix> end_ix)|| ( start_ix== -1)|| ( end_ix== -1)) */
/* 	return dtf; */
/*     else { */
/* 	dtf= ( data_fragment_ptr) malloc( sizeof( data_fragment)); */
/* 	memset( dtf, 0, sizeof( data_fragment)); */
/* 	dtf->start= start_ix; */
/* 	dtf->end= end_ix; */
/*     } */
/*     return dtf; */
/* } */

/* daily_record_ptr load_fragment( char* fname, char* start, char* end, */
/* 				int* s_ix, int* lines) { */

/*     daily_record_ptr data= NULL; */
/*     int              res= -1; */

/*     if(( data= load_file_into_memory( fname, lines))!= NULL) { */
/* 	res= *lines; */
/* 	while( order( start, data[ res- 1].date)>= 0) */
/* 	    res--; */
/* 	if( res== *lines) { */
/* 	    free( data); */
/* 	    data= NULL; */
/* 	    res= -1; */
/* 	    return NULL; */
/* 	} */
/*     } */
/*     *s_ix= res; */
/*     return data; */
/* } */
#endif
