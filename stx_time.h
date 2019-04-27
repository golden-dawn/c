#ifndef __STOCKS_TIME_H__
#define __STOCKS_TIME_H__

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#pragma warning( disable: 4047)

#define START_DATE "4-May-00"
#define ONE_DAY    86400

typedef struct time_frame_t {
  char  start_date[ 16];
  char  end_date[ 16];
  int   nb_pivots;
  int   nb_days;
} time_frame, *time_frame_ptr;


static char* month_name[]= {
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
};

static char daytab[ 2][ 12] = {
  { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
  { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};

char* get_month_name( int month_number) {

  return month_name[ month_number];
}

int get_month_number( char* month_name) {

  int month= -1;

  if( !strcmp( month_name, "Jan"))      month=  0;
  else if( !strcmp( month_name, "Feb")) month=  1;
  else if( !strcmp( month_name, "Mar")) month=  2;
  else if( !strcmp( month_name, "Apr")) month=  3;
  else if( !strcmp( month_name, "May")) month=  4;
  else if( !strcmp( month_name, "Jun")) month=  5;
  else if( !strcmp( month_name, "Jul")) month=  6;
  else if( !strcmp( month_name, "Aug")) month=  7;
  else if( !strcmp( month_name, "Sep")) month=  8;
  else if( !strcmp( month_name, "Oct")) month=  9;
  else if( !strcmp( month_name, "Nov")) month= 10;
  else if( !strcmp( month_name, "Dec")) month= 11;

  return month;
}


int is_valid_date( char* date) {

  char *month_str= NULL, *year_str=  NULL;
  int   month, day, year, leap;
  char  day_str[ 32];

  if(( date== NULL)|| !strcmp( date, ""))
    return 0;

  strcpy( day_str, date);
  if((( month_str= strchr( day_str, '-'))== NULL)||
     (( year_str= strrchr( day_str, '-'))== NULL))
    return 0;
  else {
    *month_str++= '\0';
    *year_str++= '\0';
  }

  if(( month= get_month_number( month_str))== -1)
    return 0;

  if(( day= atoi( day_str))< 0)
    return 0;

  year= atoi( year_str);
  year+= ( year> 70)? 1900: 2000;
  leap= year% 4== 0&& year% 100!= 0|| year% 400== 0;

  return day<= daytab[ leap][ month];
}

struct tm* get_time_struct_from_date( char* date) {

  struct tm* result;
  char*      month= NULL;
  char*      year=  NULL;
  time_t     tt;
  int        year_number;
  char       day[ 32];

  if( is_valid_date( date)== 0)
    return NULL;

  time( &tt);
  result= localtime( &tt);
  strcpy( day, date);
  month= strchr( day, '-');
  *month++= '\0';
  year= strchr( month, '-');
  *year++= '\0';
  
  year_number= atoi( year);
  if( year_number< 70)
    year_number+= 100;

  result->tm_mday= atoi( day);
  result->tm_mon= get_month_number( month);
  result->tm_year= year_number;
  tt= mktime( result);
  result= localtime( &tt);

  return result;
}

void ts_to_date( struct tm* ts, char* date) {

  int year= ts->tm_year% 100;

  if( date)
    sprintf( date, "%d-%s-%s%d", ts->tm_mday, get_month_name( ts->tm_mon),
	     ( year< 10)? "0": "", year);
  else
    printf( "ts_to_date: date is NULL\n");
}

void time_to_date( time_t tt, char* date) {

  ts_to_date( localtime( &tt), date);
}

time_t get_time_from_date( char* date) {

  struct tm* ts;

  if(( ts= get_time_struct_from_date( date))== NULL)
    return 0;
  ts->tm_hour= 12;
  ts->tm_min= 0;
  ts->tm_sec= 0;
  return mktime( ts);
}

void get_last_trading_day( char* date, int hour) {

  struct tm *ts;
  time_t    tt;
  int       wday;

  time( &tt);
  ts= localtime( &tt);

  wday= ts->tm_wday;
  if( ts->tm_hour>= hour) {
    wday= ( wday+ 1)% 7;
    tt+= ( unsigned long) ONE_DAY;
  }
  switch( wday) {
  case 1:
      tt-= ( unsigned long) ONE_DAY;
  case 0:
      tt-= ( unsigned long) ONE_DAY;
  default:
      tt-= ( unsigned long) ONE_DAY;
    break;
  };
  ts= localtime( &tt);
  ts_to_date( ts, date);
}

void get_previous_trading_day( char* date, char* res) {

  struct tm *ts;
  time_t    tt;
  int       wday;

  if(( ts= get_time_struct_from_date( date))== NULL)
    return;
  tt= mktime( ts);
  switch( ts->tm_wday) {
  case 1:
      tt-= ( unsigned long) ONE_DAY;        
  case 0:
      tt-= ( unsigned long) ONE_DAY;    
  default:
      tt-= ( unsigned long) ONE_DAY;    
    break;
  };
  ts= localtime( &tt);
  ts_to_date( ts, res);
}

void get_n_previous_trading_days( char* date, int n, char* res) {

  struct tm *ts;
  time_t    tt;

  if(( ts= get_time_struct_from_date( date))== NULL)
    return;

  tt= mktime( ts);
  while( n--> 0) {
    switch( ts->tm_wday) {
    case 1:
      tt-= ( unsigned long) ONE_DAY;        
    case 0:
      tt-= ( unsigned long) ONE_DAY;    
    default:
      tt-= ( unsigned long) ONE_DAY;    
      break;
    };
    ts= localtime( &tt);
  }
  ts_to_date( ts, res);
}

void get_next_trading_day( char* date, char* res) {

  struct tm *ts;
  time_t    tt;

  if(( ts= get_time_struct_from_date( date))== NULL)
    return;
  tt= mktime( ts);
  switch( ts->tm_wday) {
  case 5:
    tt+= ( unsigned long) ONE_DAY;
  case 6:
    tt+= ( unsigned long) ONE_DAY;
  default:
    tt+= ( unsigned long) ONE_DAY;
    break;
  };
  ts= localtime( &tt);
  ts_to_date( ts, res);
}

int is_trading_day( char* date) {

  struct tm *ts;

  if(( ts= get_time_struct_from_date( date))== NULL)
    return -1;
  return ts->tm_wday% 6;
}

char** generate_date_range( char* start_date, char* end_date,
			    int* nb_dates) {

  char**     dates=   NULL;
  struct tm* ts;
  time_t     start, end, tt;
  int        nb_days;
  int        we_days= 0;
  int        ix;

  if( !is_valid_date( start_date)|| !is_valid_date( end_date))
    return dates;

  start= get_time_from_date( start_date);
  end= get_time_from_date( end_date);
  nb_days= 1+ ( end- start)/ ONE_DAY;
  tt= start;
  ts= get_time_struct_from_date( start_date);
  while( ts->tm_wday< 6) {
    tt+= ONE_DAY;
    ts= localtime( &tt);
  }
  for( ; tt<= end; tt+= 6* ONE_DAY) {
    we_days++;
    tt+= ONE_DAY;
    if( tt<= end)
      we_days++;
  }
  *nb_dates= nb_days- we_days;
  if( *nb_dates> 0) {
    dates= ( char**) malloc( ( *nb_dates)* sizeof( char *));
    memset( dates, 0, ( *nb_dates)* sizeof( char *));
    for( ix= 0; start<= end; start+= ONE_DAY) {
      ts= localtime( &start);
      if( ts->tm_wday% 6) {
	dates[ ix]= ( char *) malloc( 16);
	memset( dates[ ix], 0, 16);
	ts_to_date( ts, dates[ ix++]);
      }
    }
  } else
    *nb_dates= 0;
  return dates;
}

void process_time_frame( time_frame_ptr tf) {

  int   fine= 1;
  char  tmp[ 16];

  if( !is_valid_date( tf->end_date)) {
    printf( "Invalid end date specified: %s\n<p>", tf->end_date);
    fine= 0;
  }
  if(( fine== 1)&& ( !is_valid_date( tf->start_date))&&
     ( tf->nb_pivots== 0)&& ( tf->nb_days== 0)) {
    printf( "No start date, pivotal points or range specified.\n<p>");
    fine= 0;
  }

  if(( fine== 1)&& ( is_valid_date( tf->start_date))&&
     ( is_trading_day( tf->start_date)== 0)) {
    get_next_trading_day( tf->start_date, tmp);
    strcpy( tf->start_date, tmp);
  }
  if(( fine== 1)&& ( is_trading_day( tf->end_date)== 0)) {
    get_previous_trading_day( tf->end_date, tmp);
    strcpy( tf->end_date, tmp);
  }
  if(( fine== 1)&& ( tf->nb_days> 0)) {
    get_n_previous_trading_days( tf->end_date, tf->nb_days, tmp);
    strcpy( tf->start_date, tmp);
  }
  if( fine== 0) {
    free( tf);
    tf= NULL;
  }
}

int id_time( int delay) {

  struct tm* ts;
  time_t     tt;

  time( &tt);
  ts= localtime( &tt);

  if(( ts->tm_hour< 9)|| ( ts->tm_hour> 15)||
     (( ts->tm_hour== 9)&& ( ts->tm_min< 30)))
    return 390;

  return (( ts->tm_hour- 9)* 60+ ts->tm_min- 30- delay);
}

void get_stock_time( char* s_time) {

  struct tm* ts;
  time_t     tt;

  time( &tt);
  ts= localtime( &tt);
  if((( ts->tm_hour< 16)&& ( ts->tm_hour> 9))||
     (( ts->tm_hour== 9)&& ( ts->tm_min> 30)))
    strftime( s_time, 10, "%H:%M:%S", ts);
  else
    strcpy( s_time, "EOD");
}

int order( char* date_1, char* date_2) {

  time_t t1, t2;

  if( !strcmp( date_1, date_2))
    return 0;
  t1= get_time_from_date( date_1);
  t2= get_time_from_date( date_2);
  if( t1> t2)
    return -1;
  return 1;
}
#endif
