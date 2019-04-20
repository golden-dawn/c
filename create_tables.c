#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>

void do_exit(PGconn *cnx, PGresult *res) {
    fprintf(stderr, "%s\n", PQerrorMessage(cnx));
    PQclear(res);
    PQfinish(cnx);    
    exit(1);
}

void create_table_if_missing(PGconn *cnx, char *tbl_name, char *create_sql) {
    char cmd_buffer[256];
    memset(cmd_buffer, 0, 256);
    sprintf(cmd_buffer, "select * from information_schema.tables where " \
	    "table_schema='public' and table_name='%s'", tbl_name);
    PGresult *res = PQexec(cnx, cmd_buffer);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
	printf("No table data retrieved for %s\n", tbl_name);
	do_exit(cnx, res);
    }
    int rows = PQntuples(res);
    if(rows == 0) {
	fprintf(stderr, "Table %s does not exist, creating\n", tbl_name);
	PQclear(res);
	res = PQexec(cnx, create_sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	    do_exit(cnx, res); 
    } else
	fprintf(stderr, "Table %s exists, skipping\n", tbl_name);
    PQclear(res);
}

void create_index_if_missing(PGconn *cnx, char *tbl_name, char *idx_name,
			     char *create_sql) {
    char cmd_buffer[256];
    memset(cmd_buffer, 0, 256);
    sprintf(cmd_buffer, "select * from pg_indexes where tablename='%s' " \
	    "and indexname = '%s' ", tbl_name, idx_name);
    PGresult *res = PQexec(cnx, cmd_buffer);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
	printf("No index data retrieved for table %s index %s\n", tbl_name,
	       idx_name);
	do_exit(cnx, res);
    }
    int rows = PQntuples(res);
    if(rows == 0) {
	fprintf(stderr, "Index %s for table %s does not exist, creating\n",
		idx_name, tbl_name);
	PQclear(res);
	res = PQexec(cnx, create_sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	    do_exit(cnx, res); 
    } else
	fprintf(stderr, "Index %s for table %s exists, skipping\n",
		idx_name, tbl_name);
    PQclear(res);
    
}

int main() {
    PGconn *cnx = PQconnectdb(getenv("POSTGRES_CNX"));
    if (PQstatus(cnx) == CONNECTION_BAD) {
        fprintf(stderr, "Connection to database failed: %s\n",
		PQerrorMessage(cnx));
        PQfinish(cnx);
        exit(1);
    }
    char* create_eods = "CREATE TABLE eods( "	 \
	"stk VARCHAR(16) NOT NULL, "		 \
	"dt DATE NOT NULL, "			 \
	"o INTEGER NOT NULL, "			 \
	"hi INTEGER NOT NULL, "			 \
	"lo INTEGER NOT NULL, "			 \
	"c INTEGER NOT NULL, "			 \
	"v INTEGER, "				 \
	"oi INTEGER, "				 \
	"PRIMARY KEY(stk, dt))";
    create_table_if_missing(cnx, "eods", create_eods);
    char* create_eods_dt_idx = "CREATE INDEX eods_dt_idx ON eods(dt)";
    create_index_if_missing(cnx, "eods", "eods_dt_idx", create_eods_dt_idx);

    char* create_divis = "CREATE TABLE dividends( "	 \
	"stk VARCHAR(16) NOT NULL, "			 \
	"dt DATE NOT NULL, "				 \
	"ratio DOUBLE PRECISION NOT NULL, "		 \
	"divi_type INTEGER NOT NULL, "			 \
	"PRIMARY KEY(stk, dt))";
    create_table_if_missing(cnx, "dividends", create_divis);
    char* create_divis_dt_idx = "CREATE INDEX dividends_dt_idx ON " \
	"dividends(dt)";
    create_index_if_missing(cnx, "dividends", "dividends_dt_idx",
			    create_divis_dt_idx);

    char* create_holis = "CREATE TABLE holidays( "	 \
	"dt DATE NOT NULL, "				 \
	"holiday_type INTEGER NOT NULL, "		 \
	"holiday_name VARCHAR(128), "			 \
	"PRIMARY KEY(dt))";
    create_table_if_missing(cnx, "holidays", create_holis);
    char* create_holis_holiday_type_idx = "CREATE INDEX " \
	"holidays_holiday_type_idx ON holidays(holiday_type)";
    create_index_if_missing(cnx, "holidays", "holidays_holiday_type_idx",
			    create_holis_holiday_type_idx);

    char* create_spots = "CREATE TABLE opt_spots( "	 \
	"stk VARCHAR(16) NOT NULL, "			 \
	"dt DATE NOT NULL, "				 \
	"spot INTEGER NOT NULL, "			 \
	"PRIMARY KEY(stk, dt))";
    create_table_if_missing(cnx, "opt_spots", create_spots);
    char* create_spots_dt_idx = "CREATE INDEX opt_spots_dt_idx ON " \
	"opt_spots(dt)";
    create_index_if_missing(cnx, "opt_spots", "opt_spots_dt_idx",
			    create_spots_dt_idx);


    char* create_opts = "CREATE TABLE options( "	 \
	"expiry DATE NOT NULL, "			 \
	"und VARCHAR(16) NOT NULL, "			 \
	"cp VARCHAR(1) NOT NULL, "			 \
	"strike INTEGER NOT NULL, "			 \
	"dt DATE NOT NULL, "				 \
	"bid INTEGER, "					 \
	"ask INTEGER, "					 \
	"v INTEGER, "					 \
	"oi INTEGER, "					 \
	"PRIMARY KEY(expiry, und, cp, strike, dt))";
    create_table_if_missing(cnx, "options", create_opts);
    char* create_opts_und_dt_idx = "CREATE INDEX optionss_und_dt_idx ON " \
	"options(und, dt)";
    create_index_if_missing(cnx, "options", "options_und_dt_idx",
			    create_opts_und_dt_idx);

    PQfinish(cnx);
    return 0;
}
