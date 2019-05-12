#ifndef __STX_HT_H__
#define __STX_HT_H__

/**
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

#include <libpq-fe.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
 *
 * Returns:
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


void ht_new_cal(ht_item_ptr i, const char* k, int num_day, int num_busday, 
		bool is_busday) {
    strcpy(i->key, k);
    i->item_type = CAL_HT;
    i->val.cal = (cal_info_ptr) malloc(sizeof(cal_info));
    i->val.cal->day_number = num_day;
    i->val.cal->busday_number = num_busday;
    i->val.cal->is_busday = is_busday;
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
	fprintf(stderr, 
		"hash_a = %d, hash_b = %d, num_buckets = %d, attempt = %d\n",
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
	fprintf(stderr, "i= %d, index = %d\n", i, index);
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
    fprintf(stderr, "Found %d records\n", num);
#endif
    ht_item_ptr list = NULL;
    if (num > 0) {
	list = (ht_item_ptr) calloc((size_t)num, sizeof(ht_item));
	for(int ix = 0; ix < num; ix++) {
#ifdef DEBUG
	    fprintf(stderr, "ix = %d\n", ix);
#endif
	    float ratio = atof(PQgetvalue(res, ix, 0));
	    char* dt = PQgetvalue(res, ix, 1);
	    ht_new_divi(list + ix, dt, ratio);
#ifdef DEBUG
	    fprintf(stderr, "value = %12.6f\n", ratio);
#endif
	}
    }
    return ht_new(list, num);
}

void ht_print(hashtable_ptr ht) {
    fprintf(stderr, "Hashtable: \n");
    if(ht->size == 0)
	return;
    for(int ix = 0; ix < ht->count; ix++) {
	ht_item_ptr crs = ht->list + ix;
	if (crs->item_type == DIVI_HT)
	    fprintf(stderr, "  %s, %12.6f\n", crs->key, crs->val.ratio);
	else
	    fprintf(stderr, "  %s, %5d %5d %d\n", crs->key, 
		    crs->val.cal->day_number, crs->val.cal->busday_number, 
		    crs->val.cal->is_busday);
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

#endif
