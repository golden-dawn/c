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
    CURL *curl_handle;
    CURLcode res;
    struct MemoryStruct chunk;
    chunk.memory = malloc(1);  /* will be grown by the realloc above */ 
    chunk.size = 0;    /* no data at this point */ 
    curl_global_init(CURL_GLOBAL_ALL);
    /* init the curl session */ 
    curl_handle = curl_easy_init();
    /* specify URL to get */ 
    curl_easy_setopt(curl_handle, CURLOPT_URL, "https://www.example.com/");
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
    if(res != CURLE_OK) {
	fprintf(stderr, "curl_easy_perform() failed: %s\n",
		curl_easy_strerror(res));
    }
    else {
	/*
	 * Now, our chunk.memory points to a memory block that is chunk.size
	 * bytes big and contains the remote file.
	 *
	 * Do something nice with it!
	 */
	printf("%lu bytes retrieved\n", (unsigned long)chunk.size);
    }
    /* cleanup curl stuff */ 
    curl_easy_cleanup(curl_handle);
    free(chunk.memory);
    /* we're done with libcurl, so clean it up */ 
    curl_global_cleanup();
    return 0;
}


void net_get_option_data(char* und, char* dt, char* exp) {
    LOGINFO("%s: Getting %s option data for expiry %s\n", dt, und, exp);
    char yhoo_url[256];
    sprintf(yhoo_url, "%s%s%s%d%s", Y_1, und, Y_2, num_exp, Y_3);

}

#endif
