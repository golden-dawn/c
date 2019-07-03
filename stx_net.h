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

#define Y_1 "https://query1.finance.yahoo.com/v7/finance/options/"
#define Y_2 "?formatted=true&crumb=BfPVqc7QhCQ&lang=en-US&region=US&date="
#define Y_3 "&corsDomain=finance.yahoo.com"

struct MemoryStruct {
    char *memory;
    size_t size;
};
 
static size_t
WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    struct MemoryStruct *mem = (struct MemoryStruct *)userp;
 
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
 
int main(void) {
    curl_global_init(CURL_GLOBAL_ALL);


    /* we're done with libcurl, so clean it up */ 
    curl_global_cleanup();
    return 0;
}

void print_json_err(cJSON *json, char* err_msg) {
    char* s_json = cJSON_Print(json);
    LOGERROR("%s\n%s\n", err_msg, s_json);
    free(s_json);
}

int get_from_quote(cJSON* quote, char* param_name) {
    cJSON *c = cJSON_GetObjectItemCaseSensitive(quote, param_name);
    if (c == NULL) {
	char err_msg[80];
	sprintf(err_msg, "No '%s' found in 'quote'", param_name);
	print_json_err(json, err_msg);
	return -1;
    }
    if (!cJSON_IsNumber(c)) {
	char err_msg[80];
	sprintf(err_msg, "'%s' is not a number", param_name);
	print_json_err(json, err_msg);
	return -1;
    }
    return (int)(100 * c->valuedouble);
}

void net_get_option_data(char* und, char* dt, long exp_ms, bool save_eod,
			 bool save_opts) {
    LOGINFO("%s: Getting %s option data for expiry %ld\n", dt, und, exp);
    char yhoo_url[256];
    sprintf(yhoo_url, "%s%s%s%ld%s", Y_1, und, Y_2, exp_ms, Y_3);
    CURL *curl_handle;
    CURLcode res;
    struct MemoryStruct chunk;
    chunk.memory = malloc(1);  /* will be grown by the realloc above */ 
    chunk.size = 0;    /* no data at this point */ 
    /* init the curl session */ 
    curl_handle = curl_easy_init();
    /* specify URL to get */ 
    curl_easy_setopt(curl_handle, CURLOPT_URL, yhoo_url);
    /* send all data to this function  */ 
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    /* we pass our 'chunk' struct to the callback function */ 
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);
    /* some servers don't like requests that are made without a user-agent
       field, so we provide one */ 
    curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    /* get it! */ 
    res = curl_easy_perform(curl_handle);
    /* check for errors */ 
    if(res != CURLE_OK)
	LOGERROR( "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
    else {
	cJSON *json = cJSON_Parse(chunk.memory);
	if (json == NULL) {
	    const char *error_ptr = cJSON_GetErrorPtr();
	    if (error_ptr != NULL) 
		LOGERROR("Failed to parse:\n%s\n  Error before: %s\n",
			 chunk.memory, error_ptr);
	    else
		LOGERROR("Failed to parse:\n%s\n", chunk.memory);
	    goto end;
	}
	cJSON *opt_chain = cJSON_GetObjectItemCaseSensitive(json, 
							    "optionChain");
	if (opt_chain == NULL) {
	    print_json_err(json, "No 'optionChain' found in data");
	    goto end;
	}
	cJSON *opt_result = cJSON_GetObjectItemCaseSensitive(opt_chain, 
							     "result");
	if (opt_result == NULL) {
	    print_json_err(json, "No 'result' found in 'optionChain'");
	    goto end;
	}
	if (!cJSON_IsArray(opt_result)) {
	    print_json_err(json, "'optionChain'/'result' is not an array");
	    goto end;
	}
	cJSON *opt_result_0 = cJSON_GetArrayItem(opt_result, 0);
	cJSON *quote = cJSON_GetObjectItemCaseSensitive(opt_result_0, "quote");
	if (quote == NULL) {
	    print_json_err(json, "No 'quote' found in 'result'");
	    goto end;
	}
	int c = get_from_quote(quote, "regularMarketPrice");
	if (c == -1)
	    goto end;
	if (save_eod) {
	    int v = get_from_quote(quote, "regularMarketVolume");
	    int o = get_from_quote(quote, "regularMarketOpen");
	    int hi = get_from_quote(quote, "regularMarketDayHigh");
	    int lo = get_from_quote(quote, "regularMarketDayLow");
	    if (o == -1 || hi == -1 || lo == -1 || v == -1)
		goto end;
	    /* TODO: Write the result here to file or DB */
	}
    end:
	cJSON_Delete(json);
    }

    /* cleanup curl stuff */ 
    curl_easy_cleanup(curl_handle);
    free(chunk.memory);

}

#endif
