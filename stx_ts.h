#ifndef __STX_TS_H__
#define __STX_TS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include "stx_db.h"

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

typedef struct divi_t {
    float ratio;
    char date[ 16];
} divi, *divi_ptr;

float wprice( daily_record_ptr data, int ix) {

    return ( data[ ix].close+ data[ ix].high+ data[ ix].low)/ 3;
}

daily_record_ptr load_stk(char* stk, int* num_recs) {

    daily_record_ptr result = NULL;
    /* daily_record_ptr cursor = NULL; */
    char sql_cmd[80];
    sprintf(sql_cmd, "select o, hi, lo, c, v, dt from eods where stk='%s' "
	    "order by dt", stk);
    PGresult *res = db_query(sql_cmd);
    if((*num_recs = PQntuples(res)) <= 0) 
	return result;
    int num = *num_recs;
    result = (daily_record_ptr) malloc(num * sizeof(daily_record));
    memset(result, 0, num * sizeof(daily_record));
    for(int ix = 0; ix < num; ix++) {
	result[ix].open = atoi(PQgetvalue(res, ix, 0));
	result[ix].high = atoi(PQgetvalue(res, ix, 1));
	result[ix].low = atoi(PQgetvalue(res, ix, 2));
	result[ix].close = atoi(PQgetvalue(res, ix, 3));
	result[ix].volume = atoi(PQgetvalue(res, ix, 4));
	strcpy(result[ix].date, PQgetvalue(res, ix, 5)); 
    }
    PQclear(res);
    return result;
    /* fp= fopen( filename, "r"); */
    /* memset( line, 0, 80); */
    /* cursor= result; */
    /* while( fgets( line, 80, fp)) */
    /* 	read_file_line( line, cursor++); */
    /* fclose( fp); */
    /* return result; */
}

/* int find_date_record( daily_record_ptr data, int lines, char* date) { */

/*     time_t date_time; */
/*     time_t crs_time; */
/*     int    i= 0, j= lines- 1, mid= ( i+ j)/ 2; */

/*     if( !strcmp( data[ j].date, date)) */
/* 	return j; */

/*     date_time= get_time_from_date( date); */
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
