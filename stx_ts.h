#ifndef __STX_TS_H__
#define __STX_TS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <time.h>
#include "stx_core.h"

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
    int pos;
} stx_data, *stx_data_ptr;


float wprice( daily_record_ptr data, int ix) {
    return ( data[ ix].close+ data[ ix].high+ data[ ix].low)/ 3;
}


hashtable_ptr load_splits(char* stk) {
    char sql_cmd[80];
    sprintf(sql_cmd, "select ratio, dt from dividends where stk='%s' "
	    "order by dt", stk);
    PGresult *res = db_query(sql_cmd);
#ifdef DEBUG
    LOGDEBUG("Found %d splits for %s\n", PQntuples(res), stk);
#endif
    hashtable_ptr result = ht_divis(res);
#ifdef DEBUG
    LOGDEBUG("Loaded splits for %s\n", stk);
#endif
    PQclear(res);
    return result;
}


stx_data_ptr load_stk(char* stk) {
#ifdef DEBUG
    LOGDEBUG("Loading data for %s\n", stk);
#endif
    stx_data_ptr data = (stx_data_ptr) malloc(sizeof(stx_data));
    data->data = NULL;
    data->num_recs = 0;
    data->pos = 0;
    char sql_cmd[128];
    sprintf(sql_cmd, "select o, hi, lo, c, v, dt from eods where stk='%s' "
	    "and dt>'1985-01-01'order by dt", stk);
    PGresult *res = db_query(sql_cmd);
    if((data->num_recs = PQntuples(res)) <= 0) 
	return data;
    int num = data->num_recs;
    char sd[16], ed[16];
    strcpy(sd, PQgetvalue(res, 0, 5));
    strcpy(ed, PQgetvalue(res, num - 1, 5));
    int b_days = cal_num_busdays(sd, ed);
#ifdef DEBUG
    LOGDEBUG("Found %d records for %s\n", num, stk);
#endif
    data->data = (daily_record_ptr) calloc(b_days, sizeof(daily_record));
    int ts_idx = 0;
    int calix = cal_ix(sd);
    char* dt;
    calix = cal_prev_bday(calix, &dt);
    for(int ix = 0; ix < num; ix++) {
	calix = cal_next_bday(calix, &dt);
	char* db_date = PQgetvalue(res, ix, 5);
	if (strcmp(dt, db_date) > 0) {
	    LOGERROR("Something is very wrong: dt = %s, db_date = %s\n",
		     dt, db_date);
	    exit(-1);
	}
	while (strcmp(dt, db_date) < 0) {
#ifdef DEBUG
	    LOGDEBUG("Adding data for %s, not found in %s data\n", dt, stk);
#endif
	    data->data[ts_idx].open = data->data[ts_idx - 1].close;
	    data->data[ts_idx].high = data->data[ts_idx - 1].close;
	    data->data[ts_idx].low = data->data[ts_idx - 1].close;
	    data->data[ts_idx].close = data->data[ts_idx - 1].close;
	    data->data[ts_idx].volume = 0;
	    strcpy(data->data[ts_idx].date, dt); 
	    calix = cal_next_bday(calix, &dt);
	    ts_idx++;
	}   
	data->data[ts_idx].open = atoi(PQgetvalue(res, ix, 0));
	data->data[ts_idx].high = atoi(PQgetvalue(res, ix, 1));
	data->data[ts_idx].low = atoi(PQgetvalue(res, ix, 2));
	data->data[ts_idx].close = atoi(PQgetvalue(res, ix, 3));
	data->data[ts_idx].volume = atoi(PQgetvalue(res, ix, 4));
	strcpy(data->data[ts_idx].date, PQgetvalue(res, ix, 5)); 
	ts_idx++;
    }
    data->pos = b_days - 1;
    PQclear(res);
#ifdef DEBUG
    LOGDEBUG("Loading the splits for %s\n", stk);
#endif
    data->splits = load_splits(stk);
#ifdef DEBUG
    LOGDEBUG("Done loading %s\n", stk);
#endif
    return data;
}


int find_date_record(stx_data_ptr data, char* date, int rel_pos) {
    /** rel_pos is a parameter that can take the following values:
     *  0 - do an exact search
     *  1 - return date, or next business day, if date not found
     * -1 - return date, or previous business day, if date not found
     **/
    char* first_date = data->data[0].date;
    int n = cal_num_busdays(first_date, date);
/*     if (n < 0) { */
/* 	if (rel_pos > 0) */
/* 	    return 0; */
/*     } else if (n >= data->num_recs) { */
/* 	if (rel_pos < 0) */
/* 	    return data->num_recs - 1;; */
	
/* 	return -1; */

/*         n = stxcal.num_busdays(self.sd_str, dt) */
/*         if n < 0: */
/*             if c > 0: */
/*                 return 0 */
/*         elif n >= self.l: */
/*             if c < 0: */
/*                 return self.l - 1 */
/*         else: */
/*             df_dt = str(self.df.index[n].date()) */
/*             if df_dt == dt: */
/*                 return n */
/*             # df_dt will always be less than dt, if dt is not a business day */
/*             if c < 0: */
/*                 return n - 1 */
/*             if c > 0: */
/*                 return n */
/*         return -1 */
    
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
