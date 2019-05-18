#ifndef __STX_CORE_H__
#define __STX_CORE_H__

#include <libpq-fe.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


/** LOGGING Used this: https://stackoverflow.com/questions/7411301/ **/
/* Get current time in format YYYY-MM-DD HH:MM:SS.mms */
char* crt_timestamp() {
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
    static char _retval[24];
    strftime(buff, 20, "%Y-%m-%d %H:%M:%S", localtime(&seconds));
    sprintf(_retval, "%s.%03ld", buff, milliseconds);
    return _retval;
}

/* Remove path from filename */
#define __SHORT_FILE__ (strrchr(__FILE__, '/') ? \
			strrchr(__FILE__, '/') + 1 : __FILE__)

/* Main log macro */
#define __LOG__(format, loglevel, ...) \
    fprintf(stderr, "%s %-5s [%s] [%s:%d] " format , crt_timestamp(),	\
	   loglevel, __func__, __SHORT_FILE__, __LINE__, ## __VA_ARGS__)

/* Specific log macros with  */
#define LOGDEBUG(format, ...) __LOG__(format, "DEBUG", ## __VA_ARGS__)
#define LOGWARN(format, ...) __LOG__(format, "WARN", ## __VA_ARGS__)
#define LOGERROR(format, ...) __LOG__(format, "ERROR", ## __VA_ARGS__)
#define LOGINFO(format, ...) __LOG__(format, "INFO", ## __VA_ARGS__)


/** DATABASE **/
static PGconn *conn = NULL;

void do_exit(PGconn *conn) {
    PQfinish(conn);
    exit(1);
}

void db_connect() {
    if(conn == NULL)
	conn = PQconnectdb(getenv("POSTGRES_CNX"));
    if (PQstatus(conn) == CONNECTION_BAD) {
        LOGERROR("Connection to database failed: %s\n", PQerrorMessage(conn));
        do_exit(conn);
    }
}

void db_disconnect() {
    PQfinish(conn);
}

PGresult* db_query(char* sql_cmd) {
    db_connect();
    PGresult *res = PQexec(conn, sql_cmd);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        LOGERROR("Failed to get data for command %s\n", sql_cmd);        
        PQclear(res);
        do_exit(conn);
    }
    return res;
}

/** HASHTABLE 
This is based on : https://github.com/jamesroutley/write-a-hash-table/

Need this:
1. Number of elements is known at creation time.
2. Add an internal array which will contain only the keys.
3. I don't need resizing.
4. I don't need delete.
5. Need a function to insert all elements.
6. Need a function to return a list of indices between two dates.
7. Need a function to return the key values for the indices returned in 6.
**/

typedef struct cal_info_t {
    int day_number;
    int busday_number;
    bool is_busday;
} cal_info, *cal_info_ptr;

typedef enum { DIVI_HT, CAL_HT } ht_type;

union item_value {
    float ratio;
    cal_info_ptr cal;
};

typedef struct ht_item_t {
    char key[16];
    ht_type item_type;
    union item_value val;
} ht_item, *ht_item_ptr;

typedef struct hashtable_t {
    int size;
    int count;
    ht_item_ptr* items;
    ht_item_ptr list;
} hashtable, *hashtable_ptr;

static int HT_PRIME_1 = 151;
static int HT_PRIME_2 = 163;

/*
 * Return whether x is prime or not
 *   1  - prime
 *   0  - not prime
 *   -1 - undefined (i.e. x < 2)
 */
int is_prime(const int x) {
    if (x < 2) { return -1; }
    if (x < 4) { return 1; }
    if ((x % 2) == 0) { return 0; }
    for (int i = 3; i <= floor(sqrt((double) x)); i += 2) {
        if ((x % i) == 0) {
            return 0;
        }
    }
    return 1;
}

/*
 * Return the next prime after x, or x if x is prime
 */
int next_prime(int x) {
    while (is_prime(x) != 1) {
        x++;
    }
    return x;
}

void ht_new_divi(ht_item_ptr i, const char* k, float v) {
    strcpy(i->key, k);
    i->item_type = DIVI_HT;
    i->val.ratio = v;
}

void ht_new_cal(ht_item_ptr i, const char* k, int dt_info) {
    strcpy(i->key, k);
    i->item_type = CAL_HT;
    i->val.cal = (cal_info_ptr) malloc(sizeof(cal_info));
    i->val.cal->day_number = abs(dt_info) & 0xffff;
    i->val.cal->busday_number = (abs(dt_info) >> 16) & 0x7fff;
    i->val.cal->is_busday = (dt_info > 0);
}

int ht_hash(const char* s, const int a, const int m) {
    long hash = 0;
    const int len_s = strlen(s);
    for (int i = 0; i < len_s; i++) {
        hash += (long)pow(a, len_s - (i+1)) * s[i];
        hash = hash % m;
    }
    return (int)hash;
}

int ht_get_hash(const char* s, const int num_buckets, const int attempt) {
    int hash_a = ht_hash(s, HT_PRIME_1, num_buckets);
    int hash_b = ht_hash(s, HT_PRIME_2, num_buckets);
#ifdef DEBUG
    if (attempt <= 999) 
	LOGINFO("hash_a = %d, hash_b = %d, num_buckets = %d, attempt = %d\n",
		hash_a, hash_b, num_buckets, attempt);
#endif
    if (hash_b % num_buckets == 0)
	hash_b = 1;
    return (hash_a + attempt * hash_b) % num_buckets;
}

void ht_insert(hashtable_ptr ht, ht_item_ptr item) {
    char* key = item->key;
    int index = ht_get_hash(key, ht->size, 0);
    ht_item_ptr crt_item = ht->items[index];
    int i = 1;
    while (crt_item != NULL) {
        index = ht_get_hash(item->key, ht->size, i);
#ifdef DEBUG
	LOGDEBUG("i= %d, index = %d\n", i, index);
#endif
        crt_item = ht->items[index];
        i++;
    } 
    ht->items[index] = item;
    ht->count++;
}

ht_item_ptr ht_get(hashtable_ptr ht, const char* key) {
    int index = ht_get_hash(key, ht->size, 0);
    ht_item_ptr item = ht->items[index];
    int i = 1;
    while (item != NULL) {
	if (strcmp(item->key, key) == 0)
	    return item;
        index = ht_get_hash(key, ht->size, i);
        item = ht->items[index];
        i++;
    } 
    return NULL;
}

hashtable_ptr ht_new(ht_item_ptr list, int num_elts) {
    hashtable_ptr ht = malloc(sizeof(hashtable));
    ht->count = 0;
    ht->list = list;
    ht->size = next_prime(2 * num_elts);
    ht->items = calloc((size_t)ht->size, sizeof(ht_item_ptr));
    for (int ix = 0; ix < num_elts; ++ix)
	ht_insert(ht, &list[ix]);
    return ht;
}

hashtable_ptr ht_divis(PGresult* res) {
    int num = PQntuples(res);
#ifdef DEBUG
    LOGDEBUG("Found %d records\n", num);
#endif
    ht_item_ptr list = NULL;
    if (num > 0) {
	list = (ht_item_ptr) calloc((size_t)num, sizeof(ht_item));
	for(int ix = 0; ix < num; ix++) {
#ifdef DEBUG
	    LOGDEBUG("ix = %d\n", ix);
#endif
	    float ratio = atof(PQgetvalue(res, ix, 0));
	    char* dt = PQgetvalue(res, ix, 1);
	    ht_new_divi(list + ix, dt, ratio);
#ifdef DEBUG
	    LOGDEBUG("value = %12.6f\n", ratio);
#endif
	}
    }
    return ht_new(list, num);
}


hashtable_ptr ht_calendar(PGresult* res) {
    int num = PQntuples(res);
#ifdef DEBUG
    LOGDEBUG("Calendar: found %d records\n", num);
#endif
    ht_item_ptr list = NULL;
    if (num > 0) {
	list = (ht_item_ptr) calloc((size_t)num, sizeof(ht_item));
	for(int ix = 0; ix < num; ix++) {
#ifdef DEBUG
	    LOGDEBUG("ix = %d\n", ix);
#endif
	    char* dt = PQgetvalue(res, ix, 0);
	    int dt_info = atoi(PQgetvalue(res, ix, 1));
	    ht_new_cal(list + ix, dt, dt_info);
#ifdef DEBUG
	    LOGDEBUG("dt=%s, is_busday=%5s, num_day=%5d, num_busday=%5d\n",
		     dt, (dt_info > 0)? "true": "false", 
		     abs(dt_info) & 0xffff, (abs(dt_info) >> 16) & 0x7fff);
#endif
	}
    }
    return ht_new(list, num);
}


void ht_print(hashtable_ptr ht) {
    LOGINFO("Hashtable: \n");
    if(ht->size == 0)
	return;
    for(int ix = 0; ix < ht->count; ix++) {
	ht_item_ptr crs = ht->list + ix;
	if (crs->item_type == DIVI_HT)
	    LOGINFO("  %s, %12.6f\n", crs->key, crs->val.ratio);
	else
	    LOGINFO("  %s, %5d %5d %d\n", crs->key, crs->val.cal->day_number, 
		    crs->val.cal->busday_number, crs->val.cal->is_busday);
    }
}


void ht_free(hashtable_ptr ht) {
    for(int ix = 0; ix < ht->count; ix++) {
	ht_item_ptr crs = ht->list + ix;
	if (crs->item_type == CAL_HT)
	    free(crs->val.cal);
    }
    free(ht->items);
    free(ht->list);
    free(ht);
}


/** CALENDAR **/
static hashtable_ptr cal = NULL;

hashtable_ptr get_cal() {
    if (cal == NULL) {
	char sql_cmd[80];
	LOGINFO("getting calendar from database\n");
	strcpy(sql_cmd, "select * from calendar");    
	PGresult *sql_res = db_query(sql_cmd);
	LOGINFO("got calendar fron database\n");
	cal = ht_calendar(sql_res);
	PQclear(sql_res);
	LOGINFO("populated hashtable with calendar dates\n");
    }
    return cal;
}

int cal_num_busdays(char* start_date, char* end_date) {
    hashtable_ptr calendar = get_cal();
    ht_item_ptr d1 = ht_get(calendar, start_date);
    ht_item_ptr d2 = ht_get(calendar, end_date);
    int num_days = d2->val.cal->busday_number - d1->val.cal->busday_number;
    int adj = 0;
    if (strcmp(start_date, end_date) <= 0) {
	if (d1->val.cal->is_busday)
	    adj = 1;
    } else {
	if (d2->val.cal->is_busday)
	    adj = -1;
    }
    return (num_days + adj);
}
#endif