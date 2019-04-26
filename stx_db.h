#ifndef __STX_DB_H__
#define __STX_DB_H__

#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

static PGconn *conn = NULL;


void do_exit(PGconn *conn) {
    PQfinish(conn);
    exit(1);
}


void db_connect() {
    if(conn == NULL)
	conn = PQconnectdb(getenv("POSTGRES_CNX"));
    if (PQstatus(conn) == CONNECTION_BAD) {
        fprintf(stderr, "Connection to database failed: %s\n",
		PQerrorMessage(conn));
        do_exit(conn);
    }
}


void db_disconnect() {
    PQfinish(conn);
}


PGresult* db_query(char* sql_cmd) {
    db_connect();
    PGresult *res = PQexec(conn, sql_cmd);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to get data for command %s\n", sql_cmd);        
        PQclear(res);
        do_exit(conn);
    }
    return res;
}

#endif
