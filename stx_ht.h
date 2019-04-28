#ifndef __STX_HT_H__
#define __STX_HT_H__

/**
This is based on : https://github.com/jamesroutley/write-a-hash-table/
**/

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct ht_item_t {
    char* key;
    char* value;
} ht_item, *ht_item_ptr;

typedef struct hashtable_t {
    int base_size;
    int size;
    int count;
    ht_item_ptr* items;
} hashtable, *hashtable_ptr;

static ht_item HT_DELETED_ITEM = {NULL, NULL};
static int HT_INITIAL_BASE_SIZE = 53;
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

ht_item_ptr ht_new_item(const char* k, const char* v) {
    ht_item* i = malloc(sizeof(ht_item));
    i->key = strdup(k);
    i->value = strdup(v);
    return i;
}

hashtable_ptr ht_new_sized(const int base_size) {
    hashtable_ptr ht = malloc(sizeof(hashtable));
    ht->base_size = base_size;
    ht->size = next_prime(base_size);
    ht->count = 0;
    ht->items = calloc((size_t)ht->size, sizeof(ht_item_ptr));
    return ht;
}

hashtable_ptr ht_new() {
    return ht_new_sized(HT_INITIAL_BASE_SIZE);
}

void ht_del_item(ht_item_ptr i) {
    free(i->key);
    free(i->value);
    free(i);
}

void ht_del_hash_table(hashtable_ptr ht) {
    for (int i = 0; i < ht->size; i++) {
        ht_item* item = ht->items[i];
        if (item != NULL) {
            ht_del_item(item);
        }
    }
    free(ht->items);
    free(ht);
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

void ht_insert(hashtable_ptr ht, const char* key, const char* value);

void ht_resize(hashtable_ptr ht, const int base_size) {
    if (base_size < HT_INITIAL_BASE_SIZE) {
        return;
    }
    hashtable_ptr new_ht = ht_new_sized(base_size);
    for (int i = 0; i < ht->size; i++) {
        ht_item_ptr item = ht->items[i];
        if (item != NULL && item != &HT_DELETED_ITEM) {
            ht_insert(new_ht, item->key, item->value);
        }
    }

    ht->base_size = new_ht->base_size;
    ht->count = new_ht->count;

    /* To delete new_ht, we give it ht's size and items  */
    const int tmp_size = ht->size;
    ht->size = new_ht->size;
    new_ht->size = tmp_size;

    ht_item_ptr* tmp_items = ht->items;
    ht->items = new_ht->items;
    new_ht->items = tmp_items;

    ht_del_hash_table(new_ht);
}

void ht_resize_up(hashtable_ptr ht) {
    const int new_size = ht->base_size * 2;
    ht_resize(ht, new_size);
}

void ht_resize_down(hashtable_ptr ht) {
    const int new_size = ht->base_size / 2;
    ht_resize(ht, new_size);
}

void ht_insert(hashtable_ptr ht, const char* key, const char* value) {
    const int load = ht->count * 100 / ht->size;
    if (load > 70) {
        ht_resize_up(ht);
    }
    ht_item_ptr item = ht_new_item(key, value);
    int index = ht_get_hash(item->key, ht->size, 0);
    ht_item_ptr crt_item = ht->items[index];
    int i = 1;
    while (crt_item != NULL) {
	if (crt_item != &HT_DELETED_ITEM) {
            if (strcmp(crt_item->key, key) == 0) {
                ht_del_item(crt_item);
                ht->items[index] = item;
                return;
            }
	}
        index = ht_get_hash(item->key, ht->size, i);
        crt_item = ht->items[index];
        i++;
    } 
    ht->items[index] = item;
    ht->count++;
}

char* ht_search(hashtable_ptr ht, const char* key) {
    int index = ht_get_hash(key, ht->size, 0);
    ht_item_ptr item = ht->items[index];
    int i = 1;
    while (item != NULL) {
	if (item != &HT_DELETED_ITEM) {
	    if (strcmp(item->key, key) == 0)
		return item->value;
	}
        index = ht_get_hash(key, ht->size, i);
        item = ht->items[index];
        i++;
    } 
    return NULL;
}

void ht_delete(hashtable_ptr ht, const char* key) {
    const int load = ht->count * 100 / ht->size;
    if (load < 10) {
        ht_resize_down(ht);
    }
    int index = ht_get_hash(key, ht->size, 0);
    ht_item_ptr item = ht->items[index];
    int i = 1;
    while (item != NULL) {
        if (item != &HT_DELETED_ITEM) {
            if (strcmp(item->key, key) == 0) {
                ht_del_item(item);
                ht->items[index] = &HT_DELETED_ITEM;
            }
        }
        index = ht_get_hash(key, ht->size, i);
        item = ht->items[index];
        i++;
    } 
    ht->count--;
}

#endif
