#ifndef POWER_H
#define POWER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void    POWER;
typedef void**  POWER_ROW;
typedef void    POWER_RES;
typedef void    POWER_SUB;
typedef void    POWER_STREAM;
typedef void    POWER_STMT;

#define POWERDB_DATA_TYPE_NULL       0
#define POWERDB_DATA_TYPE_BOOL       1     // 1 bytes
#define POWERDB_DATA_TYPE_TINYINT    2     // 1 byte
#define POWERDB_DATA_TYPE_SMALLINT   3     // 2 bytes
#define POWERDB_DATA_TYPE_INT        4     // 4 bytes
#define POWERDB_DATA_TYPE_BIGINT     5     // 8 bytes
#define POWERDB_DATA_TYPE_FLOAT      6     // 4 bytes
#define POWERDB_DATA_TYPE_DOUBLE     7     // 8 bytes
#define POWERDB_DATA_TYPE_BINARY     8     // string
#define POWERDB_DATA_TYPE_TIMESTAMP  9     // 8 bytes
#define POWERDB_DATA_TYPE_NCHAR      10    // multibyte string

typedef struct powerField {
  char  name[64];
  short bytes;
  char  type;
} POWER_FIELD;

void  power_init();
POWER *power_connect(const char *ip, const char *user, const char *pass, const char *db, int port);
void  power_close(POWER *power);

typedef struct POWER_BIND {
  int            buffer_type;
  void *         buffer;
  unsigned long  buffer_length;  // unused
  unsigned long *length;
  int *          is_null;
  int            is_unsigned;  // unused
  int *          error;        // unused
} POWER_BIND;

POWER_STMT *power_stmt_init(POWER *power);
int        power_stmt_prepare(POWER_STMT *stmt, const char *sql, unsigned long length);
int        power_stmt_bind_param(POWER_STMT *stmt, POWER_BIND *bind);
int        power_stmt_add_batch(POWER_STMT *stmt);
int        power_stmt_execute(POWER_STMT *stmt);
POWER_RES * power_stmt_use_result(POWER_STMT *stmt);
int        power_stmt_close(POWER_STMT *stmt);

int power_query(POWER *power, const char *sql);
POWER_RES *power_use_result(POWER *power);
POWER_ROW power_fetch_row(POWER_RES *res);
int power_result_precision(POWER_RES *res);  // get the time precision of result
void power_free_result(POWER_RES *res);
int power_field_count(POWER *power);
int power_num_fields(POWER_RES *res);
int power_affected_rows(POWER *power);
POWER_FIELD *power_fetch_fields(POWER_RES *res);
int power_select_db(POWER *power, const char *db);
int power_print_row(char *str, POWER_ROW row, POWER_FIELD *fields, int num_fields);
void power_stop_query(POWER_RES *res);

int power_fetch_block(POWER_RES *res, POWER_ROW *rows);
int power_validate_sql(POWER *power, const char *sql);

// POWER_RES   *power_list_tables(POWER *mysql, const char *wild);
// POWER_RES   *power_list_dbs(POWER *mysql, const char *wild);

// TODO: the return value should be `const`
char *power_get_server_info(POWER *power);
char *power_get_client_info();
char *power_errstr(POWER *power);

int power_errno(POWER *power);

void power_query_a(POWER *power, const char *sql, void (*fp)(void *param, POWER_RES *, int code), void *param);
void power_fetch_rows_a(POWER_RES *res, void (*fp)(void *param, POWER_RES *, int numOfRows), void *param);
void power_fetch_row_a(POWER_RES *res, void (*fp)(void *param, POWER_RES *, POWER_ROW row), void *param);

POWER_SUB *power_subscribe(const char *host, const char *user, const char *pass, const char *db, const char *table, int64_t time, int mseconds);
POWER_ROW power_consume(POWER_SUB *tsub);
void power_unsubscribe(POWER_SUB *tsub);
int power_subfields_count(POWER_SUB *tsub);
POWER_FIELD *power_fetch_subfields(POWER_SUB *tsub);

POWER_STREAM *power_open_stream(POWER *power, const char *sql, void (*fp)(void *param, POWER_RES *, POWER_ROW row),
                              int64_t stime, void *param, void (*callback)(void *));
void power_close_stream(POWER_STREAM *tstr);

int power_load_table_info(POWER *power, const char* tableNameList);

// TODO: `configDir` should not be declared here
extern char configDir[];  // the path to global configuration

#ifdef __cplusplus
}
#endif

#endif
