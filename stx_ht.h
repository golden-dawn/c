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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct ht_item_t {
    char key[16];
    float value;
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

ht_item_ptr ht_new_item(const char* k, float v) {
    ht_item* i = malloc(sizeof(ht_item));
    strcpy(i->key, k);
    i->value = v;
    return i;
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
    const int hash_a = ht_hash(s, HT_PRIME_1, num_buckets);
    const int hash_b = ht_hash(s, HT_PRIME_2, num_buckets);
    return (hash_a + (attempt * (hash_b + 1))) % num_buckets;
}

void ht_insert(hashtable_ptr ht, ht_item_ptr crs) {
    char* key = crs->key;
    float value = crs->value;
    ht_item_ptr item = ht_new_item(key, value);
    int index = ht_get_hash(item->key, ht->size, 0);
    ht_item_ptr crt_item = ht->items[index];
    int i = 1;
    while (crt_item != NULL) {
        index = ht_get_hash(item->key, ht->size, i);
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
    for (ht_item_ptr crs = list; crs != NULL; ++crs)
	ht_insert(ht, crs);
    return ht;
}

hashtable_ptr ht_divis(PGresult* res) {
    hashtable_ptr ht = calloc((size_t)1, sizeof(hashtable));
    ht->count = 0;
    ht->size = 0;
    int num = PQntuples(res);
    if (num <= 0)
	return ht;
    ht->list = calloc((size_t)num, sizeof(ht_item));
    ht->size = next_prime(2 * num);
    ht->items = calloc((size_t)ht->size, sizeof(ht_item_ptr));
    for(int ix = 0; ix < num; ix++) {
	ht->list[ix].value = atof(PQgetvalue(res, ix, 0));
	strcpy(ht->list[ix].key, PQgetvalue(res, ix, 1));
	ht_insert(ht, ht->list + ix);
    }
    return ht;
}

void ht_print(hashtable_ptr ht) {}


void ht_free(hashtable_ptr ht) {
    free(ht->items);
    free(ht->list);
    free(ht);
}

#endif
