#ifndef __STX_NET_H__
#define __STX_NET_H__

#include <cjson/cJSON.h>
#include <curl/curl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include "stx_core.h"

#define Y_1 "https://query1.finance.yahoo.com/v7/finance"
#define Y_2 "?formatted=true&crumb=BfPVqc7QhCQ&lang=en-US&region=US"
#define Y_3 "?formatted=true&crumb=SCD1VzLlKv0&lang=en-US&region=US"
#define Y_4 "&fields=regularMarketPrice,regularMarketVolume,regularMarketDayLow,regularMarketDayHigh,regularMarketOpen,"
#define Y_5 "&corsDomain=finance.yahoo.com"

typedef struct net_mem_t {
    char *memory;
    size_t size;
} net_mem, *net_mem_ptr;

static size_t
WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    net_mem_ptr mem = (net_mem_ptr)userp;
 
    char *ptr = realloc(mem->memory, mem->size + realsize + 1);
    if(ptr == NULL) {
        /* out of memory! */ 
        printf("not enough memory (realloc returned NULL)\n");
        return 0;
    }
 
    mem->memory = ptr;
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;
 
    return realsize;
}

void net_print_json_err(cJSON *json, char* err_msg) {
    char* s_json = cJSON_Print(json);
    LOGERROR("%s\n%s\n", err_msg, s_json);
    free(s_json);
}

int net_number_from_json(cJSON* json, char* name, bool has_raw) {
    cJSON *num = cJSON_GetObjectItemCaseSensitive(json, name);
    char err_msg[80];
    if (num == NULL) {
/*      sprintf(err_msg, "No '%s' found in 'quote'", name); */
/*      if (strcmp(name, "volume")) */
/*          net_print_json_err(json, err_msg); */
        return -1;
    }
    if (has_raw) {
        num = cJSON_GetObjectItemCaseSensitive(num, "raw");
        if (num == NULL) {
            sprintf(err_msg, "No '%s'/'raw' found in 'quote'", name);
            net_print_json_err(json, err_msg);
            return -1;
        }
    }
    if (!cJSON_IsNumber(num)) {
        sprintf(err_msg, "'%s' is not a number", name);
        net_print_json_err(json, err_msg);
        return -1;
    }
    if (!strcmp(name, "regularMarketVolume"))
        return (int)(num->valuedouble / 1000);
    return (int)(100 * num->valuedouble);
}

net_mem_ptr net_get_quote(char* url) {
    net_mem_ptr chunk = (net_mem_ptr) malloc(sizeof(net_mem));
    CURL *curl_handle;
    CURLcode res;
    chunk->memory = malloc(1);  /* will be grown by the realloc above */
    chunk->size = 0;    /* no data at this point */
    /* init the curl session */
    curl_handle = curl_easy_init();
    /* specify URL to get */
    curl_easy_setopt(curl_handle, CURLOPT_URL, url);
    /* send all data to this function */
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    /* we pass our 'chunk' struct to the callback function */
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)chunk);
    /* some servers don't like requests that are made without a user-agent
       field, so we provide one */
    curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    /* get it! */
    res = curl_easy_perform(curl_handle);
    /* check for errors */
    if(res != CURLE_OK) {
        LOGERROR( "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        free(chunk);
        chunk = NULL;
    }
    /* cleanup curl stuff */
    curl_easy_cleanup(curl_handle);
    return chunk;
}

cJSON* net_parse_quote(char* buffer) {
    cJSON *json = cJSON_Parse(buffer);
    if (json == NULL) {
        const char *error_ptr = cJSON_GetErrorPtr();
        if (error_ptr != NULL) 
            LOGERROR("Failed to parse:\n%s\nError before: %s\n",
                     buffer, error_ptr);
        /* TODO: do we need to free error_ptr in this case? */
        else
            LOGERROR("Failed to parse:\n%s\n", buffer);
    }
    return json;
}

cJSON* net_navigate_to_opt_quote(cJSON *json) {
    cJSON *opt_chain = cJSON_GetObjectItemCaseSensitive(json, "optionChain");
    if (opt_chain == NULL) {
        net_print_json_err(json, "No 'optionChain' found in data");
        return NULL;
    }
    cJSON *opt_result = cJSON_GetObjectItemCaseSensitive(opt_chain, "result");
    if (opt_result == NULL) {
        net_print_json_err(json, "No 'result' found in 'optionChain'");
        return NULL;
    }
    if (!cJSON_IsArray(opt_result)) {
        net_print_json_err(json, "'optionChain'/'result' is not an array");
        return NULL;
    }
    return cJSON_GetArrayItem(opt_result, 0);
}

cJSON* net_navigate_to_eod_quote(cJSON *json) {
    cJSON *q_resp = cJSON_GetObjectItemCaseSensitive(json, "quoteResponse");
    if (q_resp == NULL) {
        net_print_json_err(json, "No 'quoteResponse' found in data");
        return NULL;
    }
    cJSON *result = cJSON_GetObjectItemCaseSensitive(q_resp, "result");
    if (result == NULL) {
        net_print_json_err(json, "No 'result' found in 'quoteResponse'");
        return NULL;
    }
    if (!cJSON_IsArray(result)) {
        net_print_json_err(json, "'quoteResponse'/'result' is not an array");
        return NULL;
    }
    return cJSON_GetArrayItem(result, 0);
}

int net_parse_eod(FILE *eod_fp, cJSON* quote, char* stk, char* dt, 
                  bool has_raw) {
    int c = net_number_from_json(quote, "regularMarketPrice", has_raw);
    char err_msg[80];
    if (c == -1) {
        sprintf(err_msg, "%s: no regularMarketPrice in 'result'", stk);
        net_print_json_err(quote, err_msg);
        return -1;
    }
    if (eod_fp) {
        int v = net_number_from_json(quote, "regularMarketVolume", has_raw);
        int o = net_number_from_json(quote, "regularMarketOpen", has_raw);
        int hi = net_number_from_json(quote, "regularMarketDayHigh", has_raw);
        int lo = net_number_from_json(quote, "regularMarketDayLow", has_raw);
        if (o > 0 && hi > 0 && lo > 0 && v >= 0 && hi >= lo)
            fprintf(eod_fp, "%s\t%s\t%d\t%d\t%d\t%d\t%d\t1\n",
                    stk, dt, o, hi, lo, c, v);
        else {
            sprintf(err_msg, "%s quote error: v=%d, o=%d, hi=%d, lo=%d\n", 
                    stk, v, o, hi, lo);
            net_print_json_err(quote, err_msg);
        }
    }
    return c;
}

void net_parse_options(FILE* opt_fp, cJSON* options, char* opt_type,
                       char* exp, char* und, char* dt) {
    cJSON *opts = cJSON_GetObjectItemCaseSensitive(options, opt_type);
    char err_msg[80];
    if (opts == NULL) {
        sprintf(err_msg, "No '%s' found in 'options'", opt_type);
        net_print_json_err(options, err_msg);
        return;
    }
    if (!cJSON_IsArray(opts)) {
        sprintf(err_msg, "'options'/'%s' is not an array", opt_type);
        net_print_json_err(options, err_msg);
        return;
    }
    cJSON *opt = NULL, *crs = NULL;
    cJSON_ArrayForEach(opt, opts) {
        int volume = 0, strike, bid, ask;
        volume = net_number_from_json(opt, "volume", true);
        if (volume == -1)
            volume = 0;
        strike = net_number_from_json(opt, "strike", true);
        if (strike == -1)
            continue;
        bid = net_number_from_json(opt, "bid", true);
        if (bid == -1)
            continue;
        ask = net_number_from_json(opt, "ask", true);
        if ((ask == -1) || (ask == 0))
            continue;
        fprintf(opt_fp, "%s\t%s\t%c\t%d\t%s\t%d\t%d\t%d\t1\n",
                exp, und, opt_type[0], strike, dt, bid, ask, volume / 100);
    }
}

void net_get_eod_data(FILE *eod_fp, char* stk, char* dt) {
#ifdef DEBUG
    LOGDEBUG("%s: Getting quote for %s\n", dt, stk);
#endif
    char url[256];
    sprintf(url, "%s/quote%s&symbols=%s%s%s", Y_1, Y_3, stk, Y_4, Y_5);
    net_mem_ptr chunk = net_get_quote(url);
    if (chunk == NULL) {
        LOGERROR("%s: net_get_quote() returned null\n", stk);
        return;
    }
    cJSON *json = net_parse_quote(chunk->memory);
    if (json == NULL) {
        LOGERROR("%s: net_parse_quote() failed for %s\n", stk, chunk->memory);
        free(chunk->memory);
        free(chunk);
        return;
    }
    cJSON *quote = net_navigate_to_eod_quote(json);
    if (quote == NULL) {
        LOGERROR("%s: failed to navigate to eod quote\n", stk);
        net_print_json_err(json, "No 'quote' found in 'result'");
    } else
        net_parse_eod(eod_fp, quote, stk, dt, true);
    cJSON_Delete(json);
    free(chunk->memory);
    free(chunk);
}

void net_get_option_data(FILE *eod_fp, FILE *opt_fp, char* und, char* dt, 
                         char* exp, long exp_ms) {
    LOGINFO("%s: Getting %s option data for expiry %s\n", dt, und, exp);
    char url[256];
    sprintf(url, "%s/options/%s%s&date=%ld%s", Y_1, und, Y_2, exp_ms, Y_5);
    net_mem_ptr chunk = net_get_quote(url);
    if (chunk == NULL)
        return;
    cJSON *json = net_parse_quote(chunk->memory);
    if (json == NULL) {
        free(chunk->memory);
        free(chunk);
        return;
    }
    cJSON *opt_quote = net_navigate_to_opt_quote(json);
    cJSON *quote = cJSON_GetObjectItemCaseSensitive(opt_quote, "quote");
    if (quote == NULL) {
        net_print_json_err(json, "No 'quote' found in 'result'");
        goto end;
    }
    int spot = net_parse_eod(eod_fp, quote, und, dt, false);
    if (spot == -1)
        goto end;
    cJSON *opt_arr = cJSON_GetObjectItemCaseSensitive(opt_quote, "options");
    if (opt_arr == NULL) {
        net_print_json_err(opt_quote, "No 'options' found in options quote");
        goto end;
    }
    if (!cJSON_IsArray(opt_arr)) {
        net_print_json_err(json, "'options' is not an array");
        goto end;
    }
    cJSON *options = cJSON_GetArrayItem(opt_arr, 0);
    net_parse_options(opt_fp, options, "calls", exp, und, dt);
    net_parse_options(opt_fp, options, "puts", exp, und, dt);
 end:
    cJSON_Delete(json);
    free(chunk->memory);
    free(chunk);
}
 
/* <BEGIN> Add these two functions before and after using curlib */
/*     /\* initialize libcurl before using it *\/  */
/*     curl_global_init(CURL_GLOBAL_ALL); */
/*     /\* we're done with libcurl, so clean it up *\/  */
/*     curl_global_cleanup(); */
/*     return 0; */
/* <END> Add these two functions before and after using curlib */
#endif
