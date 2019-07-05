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

typedef struct net_mem_t {
    char *memory;
    size_t size;
} net_mem, *net_mem_ptr;

struct MemoryStruct {
    char *memory;
    size_t size;
};
 
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
 
/* int main(void) { */
/*     curl_global_init(CURL_GLOBAL_ALL); */
/*     /\* we're done with libcurl, so clean it up *\/  */
/*     curl_global_cleanup(); */
/*     return 0; */
/* } */

void net_print_json_err(cJSON *json, char* err_msg) {
    char* s_json = cJSON_Print(json);
    LOGERROR("%s\n%s\n", err_msg, s_json);
    free(s_json);
}

/* int get_from_quote(cJSON* quote, char* param_name) { */
int net_number_from_json(cJSON* json, char* name) {
    cJSON *num = cJSON_GetObjectItemCaseSensitive(json, name);
    char err_msg[80];
    if (num == NULL) {
	sprintf(err_msg, "No '%s' found in 'quote'", name);
	print_json_err(json, err_msg);
	return -1;
    }
    if (!cJSON_IsNumber(num)) {
	sprintf(err_msg, "'%s' is not a number", name);
	print_json_err(json, err_msg);
	return -1;
    }
    return (int)(100 * num->valuedouble);
}

net_mem_ptr net_get_quote(char* und, char* exp, long exp_ms) {
    net_mem_ptr chunk = (net_mem_ptr) malloc(sizeof(net_mem));
    char yhoo_url[256];
    sprintf(yhoo_url, "%s%s%s%ld%s", Y_1, und, Y_2, exp_ms, Y_3);
    CURL *curl_handle;
    CURLcode res;
    chunk->memory = malloc(1);  /* will be grown by the realloc above */
    chunk->size = 0;    /* no data at this point */
    /* init the curl session */
    curl_handle = curl_easy_init();
    /* specify URL to get */
    curl_easy_setopt(curl_handle, CURLOPT_URL, yhoo_url);
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

cJSON* net_navigate_to_quote(cJSON *json) {
    cJSON *opt_chain = cJSON_GetObjectItemCaseSensitive(json, "optionChain");
    if (opt_chain == NULL) {
	print_json_err(json, "No 'optionChain' found in data");
	return NULL;
    }
    cJSON *opt_result = cJSON_GetObjectItemCaseSensitive(opt_chain, "result");
    if (opt_result == NULL) {
	print_json_err(json, "No 'result' found in 'optionChain'");
	return NULL;
    }
    if (!cJSON_IsArray(opt_result)) {
	print_json_err(json, "'optionChain'/'result' is not an array");
	return NULL;
    }
    return cJSON_GetArrayItem(opt_result, 0);
}

int net_parse_eod(
void net_get_option_data(FILE *eod_fp, FILE *opt_fp, char* und, char* dt, 
			 char* exp, long exp_ms, bool save_eod, 
			 bool save_opts) {
    LOGINFO("%s: Getting %s option data for expiry %s\n", dt, und, exp);
    net_mem_ptr chunk = net_get_quote(und, exp, exp_ms);
    if (chunk == NULL)
	return;
    cJSON *json = net_parse_quote(chunk->memory);
    if (json == NULL)
	return;
    cJSON *opt_quote = net_navigate_to_quote(json);
    int spot = net_parse_eod(eod_fp, und, dt);
    if (spot == -1)
	goto end;

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
	if (!save_opts)
	    goto end;
	cJSON *options = cJSON_GetObjectItemCaseSensitive(opt_result_0, 
							  "options");
	if (!cJSON_IsArray(options)) {
	    print_json_err(json, "'options' is not an array");
	    goto end;
	}
	cJSON *options_0 = cJSON_GetArrayItem(options, 0);
	
	cJSON *calls = cJSON_GetObjectItemCaseSensitive(options_0, "calls");
	cJSON *puts = cJSON_GetObjectItemCaseSensitive(options_0, "puts");
	cJSON *opt = NULL, *crs = NULL;
	cJSON_ArrayForEach(opt, calls) {
	    int volume = 0, strike, bid, ask;
	    crs = cJSON_GetObjectItemCaseSensitive(opt, "volume");
	    if (crs != NULL)
		volume = get_from_quote(crs, "raw");
	    crs = cJSON_GetObjectItemCaseSensitive(opt, "strike");
	    if (crs == NULL)
		continue;
	    strike = get_from_quote(crs, "raw");
	    crs = cJSON_GetObjectItemCaseSensitive(opt, "bid");
	    if (crs == NULL)
		continue;
	    bid = get_from_quote(crs, "raw");
	    crs = cJSON_GetObjectItemCaseSensitive(opt, "ask");
	    if (crs == NULL)
		continue;
	    ask = get_from_quote(crs, "raw");
	}

    end:
	cJSON_Delete(json);
    }

    free(chunk.memory);

}

/**

        res = requests.get(self.yhoo_url.format(stk, expiry))
        if res.status_code != 200:
            print('Failed to get {0:s} data for {1:s}: {2:d}'.
                  format(exp_date, stk, res.status_code))
            return
        res_json = json.loads(res.text)
        res_0 = res_json['optionChain']['result'][0]
        quote = res_0.get('quote', {})
        c = quote.get('regularMarketPrice', -1)
        if c == -1:
            print('Failed to get closing price for {0:s}'.format(stk))
            return
        if save_eod:
            v = quote.get('regularMarketVolume', -1)
            o = quote.get('regularMarketOpen', -1)
            hi = quote.get('regularMarketDayHigh', -1)
            lo = quote.get('regularMarketDayLow', -1)
            if o == -1 or hi == -1 or lo == -1 or v == -1:
                print('Failed to get EOD quote for {0:s}'.format(stk))
            else:
                stxdb.db_insert_eods([[stk, crt_date, o, hi, lo, c, v / 1000,
                                       -1]])
        if not save_opts:
            return
        opts = res_0.get('options', [{}])
        calls = opts[0].get('calls', [])
        puts = opts[0].get('puts', [])
        cnx = stxdb.db_get_cnx()
        with cnx.cursor() as crs:
            for call in calls:
                opt_volume = 0 if call.get('volume') is None \
                             else call['volume']['raw']
                crs.execute('insert into opt_cache values' +
                            crs.mogrify(
                                '(%s,%s,%s,%s,%s,%s,%s,%s)',
                                [call['expiration']['fmt'], stk, 'c',
                                 call['strike']['raw'], crt_date,
                                 call['bid']['raw'], call['ask']['raw'],
                                 opt_volume]) +
                            'on conflict do nothing')
            for put in puts:
                opt_volume = 0 if put.get('volume') is None \
                             else put['volume']['raw']
                crs.execute('insert into opt_cache values' +
                            crs.mogrify(
                                '(%s,%s,%s,%s,%s,%s,%s,%s)',
                                [put['expiration']['fmt'], stk, 'p',
                                 put['strike']['raw'], crt_date,
                                 put['bid']['raw'], put['ask']['raw'],
                                 opt_volume]) +
                            'on conflict do nothing')
        print('Got {0:d} calls and {1:d} puts for {2:s} exp {3:s}'.format(
            len(calls), len(puts), stk, exp_date))
**/

#endif
