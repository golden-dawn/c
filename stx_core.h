#ifndef __STX_CORE_H__
#define __STX_CORE_H__

#include <string.h>


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

#endif
