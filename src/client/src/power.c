/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <stdlib.h>

#include "taos.h"
#include "power.h"

void power_init() { taos_init(); }

POWER *power_connect(const char *ip, const char *user, const char *pass, const char *db, int port) { return taos_connect(ip, user, pass, db, port); }

void power_close(POWER *power) { taos_close(power); }

POWER_STMT *power_stmt_init(POWER *power) { return taos_stmt_init(power); }

int power_stmt_prepare(POWER_STMT *stmt, const char *sql, unsigned long length) { taos_stmt_prepare(stmt, sql, length); }

int power_stmt_bind_param(POWER_STMT *stmt, POWER_BIND *bind) { taos_stmt_bind_param(stmt, bind); }

int power_stmt_add_batch(POWER_STMT *stmt) { return taos_stmt_add_batch(stmt); }

int power_stmt_execute(POWER_STMT *stmt) { return taos_stmt_execute(stmt); }

POWER_RES *power_stmt_use_result(POWER_STMT *stmt) { return taos_stmt_use_result(stmt); }

int power_stmt_close(POWER_STMT *stmt) { taos_stmt_close(stmt); }

int power_query(POWER *power, const char *sql) { return taos_query(power, sql); }

POWER_RES *power_use_result(POWER *power) { taos_use_result(power); }

POWER_ROW power_fetch_row(POWER_RES *res) { taos_fetch_row(res); }

int power_result_precision(POWER_RES *res) { taos_result_precision(res); }

void power_free_result(POWER_RES *res) { taos_free_result(res); }

int power_field_count(POWER *power) { return taos_field_count(power); }

int power_num_fields(POWER_RES *res) { return taos_num_fields(res); }

int power_affected_rows(POWER *power) { return taos_affected_rows(power); }

POWER_FIELD *power_fetch_fields(POWER_RES *res) { return taos_fetch_fields(res); }

int power_select_db(POWER *power, const char *db) { return taos_select_db(power, db); }

int power_print_row(char *str, POWER_ROW row, POWER_FIELD *fields, int num_fields) { return taos_print_row(str, row, fields, num_fields); }

void power_stop_query(POWER_RES *res) { taos_stop_query(res); }

int power_fetch_block(POWER_RES *res, POWER_ROW *rows) { return taos_fetch_block(res, rows); }

int power_validate_sql(POWER *power, const char *sql) { return taos_validate_sql(power, sql); }

char *power_get_server_info(POWER *power) { return taos_get_server_info(power); }
char *power_get_client_info() { return taos_get_client_info(); }
char *power_errstr(POWER *power) { return taos_errstr(power); }

int power_errno(POWER *power) { return taos_errno(power); }

void power_query_a(POWER *power, const char *sql, void (*fp)(void *param, POWER_RES * res, int code), void *param) { taos_query_a(power, sql, fp, param); }
void power_fetch_rows_a(POWER_RES *res, void (*fp)(void *param, POWER_RES *, int numOfRows), void *param) { taos_fetch_rows_a(res, fp, param); }
void power_fetch_row_a(POWER_RES *res, void (*fp)(void *param, POWER_RES *, POWER_ROW row), void *param) { taos_fetch_row_a(res, fp, param); }

POWER_SUB *power_subscribe(const char *host, const char *user, const char *pass, const char *db, const char *table, int64_t time, int mseconds)
{ taos_subscribe(host, user, pass, db, table, time, mseconds); }

POWER_ROW power_consume(POWER_SUB *tsub) { taos_consume(tsub);}
void power_unsubscribe(POWER_SUB *tsub) { taos_unsubscribe(tsub); }
int power_subfields_count(POWER_SUB *tsub) { return taos_subfields_count(tsub); }
POWER_FIELD *power_fetch_subfields(POWER_SUB *tsub) { return taos_fetch_subfields(tsub); }

POWER_STREAM *power_open_stream(POWER *power, const char *sql, void (*fp)(void *param, POWER_RES * res, POWER_ROW row),
                              int64_t stime, void *param, void (*callback)(void *)) {
    return taos_open_stream(power, sql, fp,stime, param, callback);
}

void power_close_stream(POWER_STREAM *tstr) { taos_close_stream(tstr); }

int power_load_table_info(POWER *power, const char* tableNameList) { taos_load_table_info(power, tableNameList); }
