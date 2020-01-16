/* charts.c */
#include <stdlib.h>
#include <stdio.h>
#include "stx_core.h"
#include "stx_ts.h"


char* chart_xticks(stx_data_ptr data, int start, int end) {
    char *xtics = NULL, *xlabel = NULL, *cursor = NULL;
    int ix, xlength, change_month = 0;
    xtics = (char*) malloc(2048);
    memset(xtics, 0, 2048);
    xtics = strcat(xtics, "set xtics (");
    xlength = end - start;
    xlabel = (char*) malloc(16);
    memset(xlabel, 0, 16);
    /** setup weekly labels */
    for(ix = start; ix <= end; ix++) {
	if ((ix == 0) || (strcmp(strrchr(data[ix].date, '-'),
				 strrchr(data[ix - 1].date, '-')))) {
	    cursor = strrchr(data[ix].date, '-') + 1;
	    if (xlength > 64)
		sprintf(xtics, "%s \"%s\" %d,", xtics, cursor, ix - start);
	    else
		change_month = 1;
	}
	else if ((ix == 0) || (strcmp(strchr(data[ix].date, '-'),
				      strchr(data[ix - 1].date, '-')))) {
	    memset(xlabel, 0, 16);
	    strcpy(xlabel, (strchr(data[ix].date, '-') + 1));
	    cursor = strrchr(xlabel, '-');
	    *cursor = '\0';
	    if(xlength > 64)
		sprintf(xtics, "%s \"%s\" %d,", xtics, xlabel, ix - start);
	    else
		change_month = 1;
	}
	else if ((xlength < 64) && (ix % 5 == 0)) {
	    memset(xlabel, 0, 16);
	    strcpy(xlabel, data[ix].date);
	    if (change_month == 0) {
		cursor = strchr(xlabel, '-');
		*cursor = '\0';
	    }
	    else
		change_month = 0;
	    sprintf(xtics, "%s \"%s\" %d,", xtics, xlabel, ix - start);
	}
    }
    *(xtics + strlen(xtics) - 1) = '\0';
    xtics = strcat(xtics, ")");
    return xtics;
}


void draw_chart(char* name, char* filename, char* xtics, char* title,
		 int size, float min, float max) {
    FILE* gp = NULL;
    char command[128];
    if((gp = fopen("/tmp/gp.gp", "w"))== NULL) {
	LOGERROR("Failed to open /tmp/gp.gp for writing.  Exiting..\n");
	return;
    }
    fprintf(gp, "set grid x \n");
    fprintf(gp, "set grid y \n");
    fprintf(gp, "set terminal png color \n");
    fprintf(gp, "set output \"/tmp/%s.png\" \n", name);
    fprintf(gp, "set multiplot \n");
    fprintf(gp, "set origin 0, 0 \n");
    fprintf(gp, "set format y \"%%10.0f\" \n");
    fprintf(gp, "set size nosquare 1, 0.28 \n");
    fprintf(gp, "%s\n", xtics);
    fprintf(gp, "set xrange [-1:%d] \n", size+ 1);    
    fprintf(gp, "plot \"%s\" using 1:10 notitle with impulses 9 \n",
	    filename);
    fprintf(gp, "set title \"%s\" \n", title);
    fprintf(gp, "set origin 0, 0.25 \n");
    fprintf(gp, "set format y \"%%10.2f\" \n");
    fprintf(gp, "set size nosquare 1, 0.7 \n");
    fprintf(gp, "set xrange [-1:%d] \n", size+ 1);    
    fprintf(gp, "set yrange [%f:%f] \n", min, max);    
    fprintf(gp, "set nokey \n");    
    fprintf(gp, "plot \"%s\" using 1:2:3:4:5 with candlesticks 2\n",
	    filename);
    fprintf(gp, "set xrange [-1:%d] \n", size+ 1);    
    fprintf(gp, "set yrange [%f:%f] \n", min, max);    
    fprintf(gp, "set nokey \n");    
    fprintf(gp, "plot \"%s\" using 1:6:7:8:9 with candlesticks\n", filename);
    fprintf(gp, "set nomultiplot\n");
    fclose(gp);
    int status = system("gnuplot /tmp/gp.gp");
    LOGINFO("gnuplot exited with status %d\n", status);
}

void chart_stock(char* name, char* start_date, char* end_date) {
    FILE *fp = NULL;
    char *filename = "/tmp/gp.txt";
    if ((fp = fopen(filename, "w")) == NULL) {
	LOGERROR("Failed to open /tmp/gp.txt for writing.  Exiting..\n");
	return;
    }
    stx_data_ptr data = ts_load_stk(name);
    char *xticks = NULL, *title = NULL;
    int min = 100000000, max = 0, start, end, ix;
    start = ts_find_date_record(data, start_date, 1);
    end = ts_find_date_record(data, end_date, -1);
    for(ix = start; ix <= end; ix++) {
	if (max < data->data[ix].high)
	    max = data->data[ix].high;
	if (min > data->data[ix].low)
	    min = data->data[ix].low;
	if (data->data[ix].open > data->data[ix].close)
	    fprintf(fp, "%d ? ? ? ? %.2f %.2f %.2f %.2f %.0f\n", 
		    ix - start, data->data[ix].open, data->data[ix].high, 
		    data->data[ix].low, data->data[ix].close, 
		    data->data[ix].volume);
	else
	    fprintf(fp, "%d %.2f %.2f %.2f %.2f ? ? ? ? %.0f\n", 
		    ix - start, data->data[ix].open, data->data[ix].high, 
		    data->data[ix].low, data->data[ix].close, 
		    data->data[ix].volume);
    }
    fclose(fp);
    xticks = set_xticks(data, start, end);
    title = get_title(data, lines, end, name);
    draw_chart(name, filename, xticks, title, end - start, min, max);
    free(xticks);
    free(title);
    free( data);
    data= NULL;
}


void main( int argc, char* argv[]) {

  name_value     parameters[ 128];
  time_frame_ptr tf=    NULL;
  char           *name= NULL, *value= NULL, *buffer= NULL; 
  int            len, ix= 0, is_idx= 0;
  char           filename[ 512], suffix[ 16];

  memset( suffix, 0, 16);
  printf( "Content-type: text/html\n\n");
  printf( "<body bgcolor=#000000 text=#ffffff link=#ffffff vlink=#ffffff>");
  buffer= strdup( getenv( "QUERY_STRING")); /** Assume GET */
/*   printf( "buffer= %s<p>", buffer); */
  name= buffer;
  len= 0;
  while( name!= NULL) {
    value= strchr( name, '=');
    if( value!= NULL) { 
      *value++= '\0';
      strcpy( parameters[ len].name, name);
      name=  strchr( value, '&');
      if( name!= NULL)
	*name++='\0';
      if( strcmp( value, ""))
	strcpy( parameters[ len].value, value);
      else
	strcpy( parameters[ len].value, "nil");
    } else {
      name= NULL;
    }
    len++;
  }

  if( !strcmp( parameters[ ix].name, "name")&&
      strcmp( parameters[ ix].value, "nil"))
    strcpy( filename, parameters[ ix].value);
  else
    is_idx= 1;
  ix++;

  if(( tf= parse_time_frame( len, parameters, &ix))!= NULL) {
    if( !strcmp( parameters[ ix].name, "tf")&&
	strcmp( parameters[ ix].value, "nil"))
      strcpy( suffix, parameters[ ix].value);
    if( is_idx== 1) {
      chart_stock( "dji", *tf, suffix);
      chart_stock( "nasdaq", *tf, suffix);
      chart_stock( "sp500", *tf, suffix);
    } else
      build_chart_list( filename, *tf, suffix);
    free( tf);
  }
  free( buffer);
}
