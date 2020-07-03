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

#define _GNU_SOURCE

#include <argp.h>
#include <assert.h>
#include <inttypes.h>

#ifndef _ALPINE
#include <error.h>
#endif
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>

#include "taos.h"
#include "tutil.h"

extern char configDir[];

#define BUFFER_SIZE      65536
#define MAX_DB_NAME_SIZE 64
#define MAX_TB_NAME_SIZE 64
#define MAX_DATA_SIZE    1024
#define MAX_NUM_DATATYPE 8
#define OPT_ABORT        1 /* –abort */
#define STRING_LEN       512

/* The options we understand. */
static struct argp_option options[] = {
  {0, 'h', "host",                     0, "The host to connect to TDEngine. Default is localhost.",                                                           0},
  {0, 'p', "port",                     0, "The TCP/IP port number to use for the connection. Default is 0.",                                                  1},
  {0, 'u', "user",                     0, "The TDEngine user name to use when connecting to the server. Default is 'root'.",                                  2},
  {0, 'P', "password",                 0, "The password to use when connecting to the server. Default is 'taosdata'.",                                        3},
  {0, 'd', "database",                 0, "Destination database. Default is 'test'.",                                                                         3},
  {0, 'm', "table_prefix",             0, "Table prefix name. Default is 't'.",                                                                               3},
  {0, 'M', 0,                          0, "Use metric flag.",                                                                                                 13},
  {0, 'o', "outputfile",               0, "Direct output to the named file. Default is './output.txt'.",                                                      14},
  {0, 'q', "query_mode",               0, "Query mode--0: SYNC, 1: ASYNC. Default is SYNC.",                                                                  6},
  {0, 'b', "type_of_cols",             0, "The data_type of columns: 'INT', 'TINYINT', 'SMALLINT', 'BIGINT', 'FLOAT', 'DOUBLE', 'BINARY'. Default is 'INT'.", 7},
  {0, 'w', "length_of_binary",         0, "The length of data_type 'BINARY'. Only applicable when type of cols is 'BINARY'. Default is 8",                    8},
  {0, 'l', "num_of_cols_per_record",   0, "The number of columns per record. Default is 3.",                                                                  8},
  {0, 'T', "num_of_threads",           0, "The number of threads. Default is 10.",                                                                            9},
  {0, 'r', "num_of_records_per_req",   0, "The number of records per request. Default is 1000.",                                                              10},
  {0, 't', "num_of_tables",            0, "The number of tables. Default is 10000.",                                                                          11},
  {0, 'n', "num_of_records_per_table", 0, "The number of records per table. Default is 100000.",                                                              12},
  {0, 'c', "config_directory",         0, "Configuration directory. Default is '/etc/taos/'.",                                                                14},
  {0, 'x', 0,                          0, "Insert only flag.",                                                                                                13},
  {0, 'O', "order",                    0, "Insert mode--0: In order, 1: Out of order. Default is in order.",                                                  14},
  {0, 'R', "rate",                     0, "Out of order data's rate--if order=1 Default 10, min: 0, max: 50.",                                                14},
  {0, 'D', "delete table",             0, "Delete data methods——0: don't delete, 1: delete by table, 2: delete by stable, 3: delete by database",             14},
  {0}};

/* Used by main to communicate with parse_opt. */
typedef struct DemoArguments {
  char  *host;
  uint16_t    port;
  char  *user;
  char  *password;
  char  *database;
  char  *tb_prefix;
  bool   use_metric;
  bool   insert_only;
  char  *output_file;
  int    mode;
  char  *datatype[MAX_NUM_DATATYPE+1];
  int    len_of_binary;
  int    num_of_CPR;
  int    num_of_threads;
  int    num_of_RPR;
  int    num_of_tables;
  int    num_of_DPT;
  int    abort;
  int    order;
  int    rate;
  int    method_of_delete;
  char **arg_list;
} SDemoArguments;

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  /* Get the input argument from argp_parse, which we
     know is a pointer to our arguments structure. */
  SDemoArguments *arguments = state->input;
  wordexp_t full_path;
  char **sptr;
  switch (key) {
    case 'h':
      arguments->host = arg;
      break;
    case 'p':
      arguments->port = atoi(arg);
      break;
    case 'u':
      arguments->user = arg;
      break;
    case 'P':
      arguments->password = arg;
      break;
    case 'o':
      arguments->output_file = arg;
      break;
    case 'q':
      arguments->mode = atoi(arg);
      break;
    case 'T':
      arguments->num_of_threads = atoi(arg);
      break;
    case 'r':
      arguments->num_of_RPR = atoi(arg);
      break;
    case 't':
      arguments->num_of_tables = atoi(arg);
      break;
    case 'n':
      arguments->num_of_DPT = atoi(arg);
      break;
    case 'd':
      arguments->database = arg;
      break;
    case 'l':
      arguments->num_of_CPR = atoi(arg);
      break;
    case 'b':
      sptr = arguments->datatype;
      if (strstr(arg, ",") == NULL) {
        if (strcasecmp(arg, "INT") != 0 && strcasecmp(arg, "FLOAT") != 0 &&
            strcasecmp(arg, "TINYINT") != 0 && strcasecmp(arg, "BOOL") != 0 &&
            strcasecmp(arg, "SMALLINT") != 0 &&
            strcasecmp(arg, "BIGINT") != 0 && strcasecmp(arg, "DOUBLE") != 0 &&
            strcasecmp(arg, "BINARY")) {
          argp_error(state, "Invalid data_type!");
        }
        sptr[0] = arg;
      } else {
        int index = 0;
        char *dupstr = strdup(arg);
        char *running = dupstr;
        char *token = strsep(&running, ",");
        while (token != NULL) {
          if (strcasecmp(token, "INT") != 0 &&
              strcasecmp(token, "FLOAT") != 0 &&
              strcasecmp(token, "TINYINT") != 0 &&
              strcasecmp(token, "BOOL") != 0 &&
              strcasecmp(token, "SMALLINT") != 0 &&
              strcasecmp(token, "BIGINT") != 0 &&
              strcasecmp(token, "DOUBLE") != 0 && strcasecmp(token, "BINARY")) {
            argp_error(state, "Invalid data_type!");
          }
          sptr[index++] = token;
          token = strsep(&running, ", ");
        }
      }
      break;
    case 'w':
      arguments->len_of_binary = atoi(arg);
      break;
    case 'm':
      arguments->tb_prefix = arg;
      break;
    case 'M':
      arguments->use_metric = true;
      break;
    case 'x':
      arguments->insert_only = true;
      break;
    case 'c':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
    case 'O':
      arguments->order = atoi(arg);
      if (arguments->order > 1 || arguments->order < 0)
      {
        arguments->order = 0;
      } else if (arguments->order == 1)
      {
        arguments->rate = 10;
      }
      break;
    case 'R':
      arguments->rate = atoi(arg);
      if (arguments->order == 1 && (arguments->rate > 50 || arguments->rate <= 0))
      {
        arguments->rate = 10;
      }
      break;
    case 'D':
      arguments->method_of_delete = atoi(arg);
      if (arguments->method_of_delete < 0 || arguments->method_of_delete > 3)
      {
        arguments->method_of_delete = 0;
      }
      break;
    case OPT_ABORT:
      arguments->abort = 1;
      break;
    case ARGP_KEY_ARG:
      /*arguments->arg_list = &state->argv[state->next-1];
      state->next = state->argc;*/
      argp_usage(state);
      break;

    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

/* ******************************* Structure
 * definition*******************************  */
enum MODE {
  SYNC, ASYNC
};
typedef struct {
  TAOS *taos;
  int threadID;
  char db_name[MAX_DB_NAME_SIZE];
  char fp[4096];
  char **datatype;
  int len_of_binary;
  char tb_prefix[MAX_TB_NAME_SIZE];
  int start_table_id;
  int end_table_id;
  int ncols_per_record;
  int nrecords_per_table;
  int nrecords_per_request;
  int data_of_order;
  int data_of_rate;
  int64_t start_time;
  bool do_aggreFunc;

  sem_t mutex_sem;
  int notFinished;
  sem_t lock_sem;
} info;

typedef struct {
  TAOS  *taos;

  char   tb_name[MAX_TB_NAME_SIZE];
  int64_t   timestamp;
  int    target;
  int    counter;
  int    nrecords_per_request;
  int    ncols_per_record;
  char **data_type;
  int    len_of_binary;
  int data_of_order;
  int data_of_rate;

  sem_t *mutex_sem;
  int   *notFinished;
  sem_t *lock_sem;
} sTable;

/* ******************************* Global
 * variables*******************************  */
char *aggreFunc[] = {"*", "count(*)", "avg(f1)", "sum(f1)", "max(f1)", "min(f1)", "first(f1)", "last(f1)"};

/* ******************************* Global
 * functions*******************************  */
static struct argp argp = {options, parse_opt, 0, 0};

void queryDB(TAOS *taos, char *command);

void *readTable(void *sarg);

void *readMetric(void *sarg);

void *syncWrite(void *sarg);

void *deleteTable();

void *asyncWrite(void *sarg);

int generateData(char *res, char **data_type, int num_of_cols, int64_t timestamp, int len_of_binary);

void rand_string(char *str, int size);

double getCurrentTime();

void callBack(void *param, TAOS_RES *res, int code);

int main(int argc, char *argv[]) {
  SDemoArguments arguments = {  NULL,            // host
                                0,               // port
                                "root",          // user
                                "taosdata",      // password
                                "test",          // database
                                "t",             // tb_prefix
                                false,           // use_metric
                                false,           // insert_only
                                "./output.txt",  // output_file
                                0,               // mode
                                {
                                "int",           // datatype
                                "",
                                "",
                                "",
                                "",
                                "",
                                "",
                                ""
                                },
                                8,               // len_of_binary
                                1,               // num_of_CPR
                                1,               // num_of_connections/thread
                                1,               // num_of_RPR
                                1,               // num_of_tables
                                50000,           // num_of_DPT
                                0,               // abort
                                0,               // order
                                0,               // rate
                                0,               // method_of_delete
                                NULL             // arg_list
                                };

  /* Parse our arguments; every option seen by parse_opt will be
     reflected in arguments. */
  // For demo use, change default values for some parameters;
  arguments.num_of_tables = 10000;
  arguments.num_of_CPR = 3; 
  arguments.num_of_threads = 10;
  arguments.num_of_DPT = 100000;
  arguments.num_of_RPR = 1000;
  arguments.use_metric = true;
  arguments.insert_only = true;
  // end change

  argp_parse(&argp, argc, argv, 0, 0, &arguments);

  if (arguments.abort) {
    #ifndef _ALPINE
      error(10, 0, "ABORTED");
    #else
      abort();
    #endif
  }
  
  enum MODE query_mode = arguments.mode;
  char *ip_addr = arguments.host;
  uint16_t port = arguments.port;
  char *user = arguments.user;
  char *pass = arguments.password;
  char *db_name = arguments.database;
  char *tb_prefix = arguments.tb_prefix;
  int len_of_binary = arguments.len_of_binary;
  int ncols_per_record = arguments.num_of_CPR;
  int order = arguments.order;
  int rate = arguments.rate;
  int method_of_delete = arguments.method_of_delete;
  int ntables = arguments.num_of_tables;
  int threads = arguments.num_of_threads;
  int nrecords_per_table = arguments.num_of_DPT;
  int nrecords_per_request = arguments.num_of_RPR;
  bool use_metric = arguments.use_metric;
  bool insert_only = arguments.insert_only;
  char **data_type = arguments.datatype;
  int count_data_type = 0;
  char dataString[STRING_LEN];
  bool do_aggreFunc = true;

  memset(dataString, 0, STRING_LEN);
  int len = 0;

  if (strcasecmp(data_type[0], "BINARY") == 0 || strcasecmp(data_type[0], "BOOL") == 0) {
    do_aggreFunc = false;
  }
  for (; count_data_type <= MAX_NUM_DATATYPE; count_data_type++) {
    if (strcasecmp(data_type[count_data_type], "") == 0) {
      break;
    }

    len += snprintf(dataString + len, STRING_LEN - len, "%s ", data_type[count_data_type]);
  }

  FILE *fp = fopen(arguments.output_file, "a");
  if (NULL == fp) {
    fprintf(stderr, "Failed to open %s for writing\n", arguments.output_file);
    return 1;
  };
  
  time_t tTime = time(NULL);
  struct tm tm = *localtime(&tTime);
  printf("###################################################################\n");
  printf("# Server IP:                         %s:%hu\n", ip_addr == NULL ? "localhost" : ip_addr, port);
  printf("# User:                              %s\n", user);
  printf("# Password:                          %s\n", pass);
  printf("# Use metric:                        %s\n", use_metric ? "true" : "false");
  printf("# Datatype of Columns:               %s\n", dataString);
  printf("# Binary Length(If applicable):      %d\n",
          (strcasestr(dataString, "BINARY") != NULL) ? len_of_binary : -1);
  printf("# Number of Columns per record:      %d\n", ncols_per_record);
  printf("# Number of Threads:                 %d\n", threads);
  printf("# Number of Tables:                  %d\n", ntables);
  printf("# Number of Data per Table:          %d\n", nrecords_per_table);
  printf("# Records/Request:                   %d\n", nrecords_per_request);
  printf("# Database name:                     %s\n", db_name);
  printf("# Table prefix:                      %s\n", tb_prefix);
  if (order == 1)
  {
    printf("# Data order:                        %d\n", order);
    printf("# Data out of order rate:            %d\n", rate);

  }
  printf("# Delete method:                     %d\n", method_of_delete);
  printf("# Test time:                         %d-%02d-%02d %02d:%02d:%02d\n", tm.tm_year + 1900, tm.tm_mon + 1,
          tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
  printf("###################################################################\n\n");
  printf("Press enter key to continue");
  (void)getchar();

  fprintf(fp, "###################################################################\n");
  fprintf(fp, "# Server IP:                         %s:%hu\n", ip_addr == NULL ? "localhost" : ip_addr, port);
  fprintf(fp, "# User:                              %s\n", user);
  fprintf(fp, "# Password:                          %s\n", pass);
  fprintf(fp, "# Use metric:                        %s\n", use_metric ? "true" : "false");
  fprintf(fp, "# Datatype of Columns:               %s\n", dataString);
  fprintf(fp, "# Binary Length(If applicable):      %d\n",
          (strcasestr(dataString, "BINARY") != NULL) ? len_of_binary : -1);
  fprintf(fp, "# Number of Columns per record:      %d\n", ncols_per_record);
  fprintf(fp, "# Number of Threads:                 %d\n", threads);
  fprintf(fp, "# Number of Tables:                  %d\n", ntables);
  fprintf(fp, "# Number of Data per Table:          %d\n", nrecords_per_table);
  fprintf(fp, "# Records/Request:                   %d\n", nrecords_per_request);
  fprintf(fp, "# Database name:                     %s\n", db_name);
  fprintf(fp, "# Table prefix:                      %s\n", tb_prefix);
  if (order == 1)
  {
    printf("# Data order:                        %d\n", order);
    printf("# Data out of order rate:            %d\n", rate);

  }
  fprintf(fp, "# Test time:                         %d-%02d-%02d %02d:%02d:%02d\n", tm.tm_year + 1900, tm.tm_mon + 1,
          tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
  fprintf(fp, "###################################################################\n\n");
  fprintf(fp, "|  WRecords  | Records/Second | Requests/Second |  WLatency(ms) |\n");

  taos_init();
  TAOS *taos = taos_connect(ip_addr, user, pass, NULL, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
    return 1;
  }
  char command[BUFFER_SIZE] = "\0";

  sprintf(command, "drop database %s;", db_name);
  TAOS_RES* res = taos_query(taos, command);
  taos_free_result(res);

  sprintf(command, "create database %s;", db_name);
  res = taos_query(taos, command);
  taos_free_result(res);

  char cols[STRING_LEN] = "\0";
  int colIndex = 0;
  len = 0;

  for (; colIndex < ncols_per_record - 1; colIndex++) {
    if (strcasecmp(data_type[colIndex % count_data_type], "BINARY") != 0) {
      len += snprintf(cols + len, STRING_LEN - len, ",f%d %s", colIndex + 1, data_type[colIndex % count_data_type]);
    } else {
      len += snprintf(cols + len, STRING_LEN - len, ",f%d %s(%d)", colIndex + 1, data_type[colIndex % count_data_type], len_of_binary);
    }
  }

  if (strcasecmp(data_type[colIndex % count_data_type], "BINARY") != 0) {
    len += snprintf(cols + len, STRING_LEN - len, ",f%d %s)", colIndex + 1, data_type[colIndex % count_data_type]);
  } else {
    len += snprintf(cols + len, STRING_LEN - len, ",f%d %s(%d))", colIndex + 1, data_type[colIndex % count_data_type], len_of_binary);
  }

  if (!use_metric) {
    /* Create all the tables; */
    printf("Creating %d table(s)......\n", ntables);
    for (int i = 0; i < ntables; i++) {
      snprintf(command, BUFFER_SIZE, "create table if not exists %s.%s%d (ts timestamp%s;", db_name, tb_prefix, i, cols);
      queryDB(taos, command);
    }

    printf("Table(s) created!\n");
    taos_close(taos);

  } else {
    /* Create metric table */
    printf("Creating meters super table...\n");
    snprintf(command, BUFFER_SIZE, "create table if not exists %s.meters (ts timestamp%s tags (areaid int, loc binary(10))", db_name, cols);
    queryDB(taos, command);
    printf("meters created!\n");

    /* Create all the tables; */
    printf("Creating %d table(s)......\n", ntables);
    for (int i = 0; i < ntables; i++) {
      int j;
      if (i % 10 == 0) {
        j = 10;
      } else {
        j = i % 10;
      }
    if (j % 2 == 0) {
      snprintf(command, BUFFER_SIZE, "create table if not exists %s.%s%d using %s.meters tags (%d,\"%s\");", db_name, tb_prefix, i, db_name, j, "shanghai");
    } else {
      snprintf(command, BUFFER_SIZE, "create table if not exists %s.%s%d using %s.meters tags (%d,\"%s\");", db_name, tb_prefix, i, db_name, j, "beijing");
    }
      queryDB(taos, command);
    }

    printf("Table(s) created!\n");
    taos_close(taos);
  }
  /* Wait for table to create  */
  

  /* Insert data */
  double ts = getCurrentTime();
  printf("Inserting data......\n");
  pthread_t *pids = malloc(threads * sizeof(pthread_t));
  info *infos = malloc(threads * sizeof(info));

  int a = ntables / threads;
  if (a < 1) {
    threads = ntables;
    a = 1;
  }

  int b = 0;
  if (threads != 0)
    b = ntables % threads;
  int last = 0;
  for (int i = 0; i < threads; i++) {
    info *t_info = infos + i;
    t_info->threadID = i;
    tstrncpy(t_info->db_name, db_name, MAX_DB_NAME_SIZE);
    tstrncpy(t_info->tb_prefix, tb_prefix, MAX_TB_NAME_SIZE);
    t_info->datatype = data_type;
    t_info->ncols_per_record = ncols_per_record;
    t_info->nrecords_per_table = nrecords_per_table;
    t_info->start_time = 1500000000000;
    t_info->taos = taos_connect(ip_addr, user, pass, db_name, port);
    t_info->len_of_binary = len_of_binary;
    t_info->nrecords_per_request = nrecords_per_request;
    t_info->start_table_id = last;
    t_info->data_of_order = order;
    t_info->data_of_rate = rate;
    t_info->end_table_id = i < b ? last + a : last + a - 1;
    last = t_info->end_table_id + 1;

    sem_init(&(t_info->mutex_sem), 0, 1);
    t_info->notFinished = t_info->end_table_id - t_info->start_table_id + 1;
    sem_init(&(t_info->lock_sem), 0, 0);

    if (query_mode == SYNC) {
      pthread_create(pids + i, NULL, syncWrite, t_info);
    } else {
      pthread_create(pids + i, NULL, asyncWrite, t_info);
    }
  }
  for (int i = 0; i < threads; i++) {
    pthread_join(pids[i], NULL);
  }

  double t = getCurrentTime() - ts;
  if (query_mode == SYNC) {
    printf("SYNC Insert with %d connections:\n", threads);
  } else {
    printf("ASYNC Insert with %d connections:\n", threads);
  }

  fprintf(fp, "|%10.d  |  %10.2f    |  %10.2f     |  %10.4f   |\n\n",
          ntables * nrecords_per_table, ntables * nrecords_per_table / t,
          (ntables * nrecords_per_table) / (t * nrecords_per_request),
          t * 1000);

  printf("Spent %.4f seconds to insert %d records with %d record(s) per request: %.2f records/second\n",
         t, ntables * nrecords_per_table, nrecords_per_request,
         ntables * nrecords_per_table / t);

  for (int i = 0; i < threads; i++) {
    info *t_info = infos + i;
    taos_close(t_info->taos);
    sem_destroy(&(t_info->mutex_sem));
    sem_destroy(&(t_info->lock_sem));
  }

  free(pids);
  free(infos);
  fclose(fp);

  if (method_of_delete != 0)
  {
    TAOS *dtaos = taos_connect(ip_addr, user, pass, db_name, port);
    double dts = getCurrentTime();
    printf("Deleteing %d table(s)......\n", ntables);

    switch (method_of_delete)
    {
    case 1:
      // delete by table
      /* Create all the tables; */
      for (int i = 0; i < ntables; i++) {
        sprintf(command, "drop table %s.%s%d;", db_name, tb_prefix, i);
        queryDB(dtaos, command);
      }
      break;
    case 2:
      // delete by stable
      if (!use_metric) {
        break;
      }
      else
      {
        sprintf(command, "drop table %s.meters;", db_name);
        queryDB(dtaos, command);
      }
      break;
    case 3:
      // delete by database
      sprintf(command, "drop database %s;", db_name);
      queryDB(dtaos, command);
      break;
    default:
      break;
    }

    printf("Table(s) droped!\n");
    taos_close(dtaos);

    double dt = getCurrentTime() - dts;
    printf("Spent %.4f seconds to drop %d tables\n", dt, ntables);

    FILE *fp = fopen(arguments.output_file, "a");
    fprintf(fp, "Spent %.4f seconds to drop %d tables\n", dt, ntables);
    fclose(fp);

  }
  

  if (!insert_only) {
    // query data
    pthread_t read_id;
    info *rInfo = malloc(sizeof(info));
    rInfo->start_time = 1500000000000;
    rInfo->start_table_id = 0;
    rInfo->end_table_id = ntables - 1;
    rInfo->do_aggreFunc = do_aggreFunc;
    rInfo->nrecords_per_table = nrecords_per_table;
    rInfo->taos = taos_connect(ip_addr, user, pass, db_name, port);
    strcpy(rInfo->tb_prefix, tb_prefix);
    strcpy(rInfo->fp, arguments.output_file);

    if (!use_metric) {
      pthread_create(&read_id, NULL, readTable, rInfo);
    } else {
      pthread_create(&read_id, NULL, readMetric, rInfo);
    }
    pthread_join(read_id, NULL);
    taos_close(rInfo->taos);
  }

  return 0;
}

void *readTable(void *sarg) {
  info *rinfo = (info *)sarg;
  TAOS *taos = rinfo->taos;
  char command[BUFFER_SIZE] = "\0";
  int64_t sTime = rinfo->start_time;
  char *tb_prefix = rinfo->tb_prefix;
  FILE *fp = fopen(rinfo->fp, "a");
  int num_of_DPT = rinfo->nrecords_per_table;
  int num_of_tables = rinfo->end_table_id - rinfo->start_table_id + 1;
  int totalData = num_of_DPT * num_of_tables;
  bool do_aggreFunc = rinfo->do_aggreFunc;

  int n = do_aggreFunc ? (sizeof(aggreFunc) / sizeof(aggreFunc[0])) : 2;
  if (!do_aggreFunc) {
    printf("\nThe first field is either Binary or Bool. Aggregation functions are not supported.\n");
  }
  printf("%d records:\n", totalData);
  fprintf(fp, "| QFunctions |    QRecords    |   QSpeed(R/s)   |  QLatency(ms) |\n");

  for (int j = 0; j < n; j++) {
    double totalT = 0;
    int count = 0;
    for (int i = 0; i < num_of_tables; i++) {
      sprintf(command, "select %s from %s%d where ts>= %" PRId64, aggreFunc[j], tb_prefix, i, sTime);

      double t = getCurrentTime();
      TAOS_RES *pSql = taos_query(taos, command);
      int32_t code = taos_errno(pSql);

      if (code != 0) {
        fprintf(stderr, "Failed to query:%s\n", taos_errstr(pSql));
        taos_free_result(pSql);
        taos_close(taos);
        exit(EXIT_FAILURE);
      }

      while (taos_fetch_row(pSql) != NULL) {
        count++;
      }

      t = getCurrentTime() - t;
      totalT += t;

      taos_free_result(pSql);
    }

    fprintf(fp, "|%10s  |   %10d   |  %12.2f   |   %10.2f  |\n",
            aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j], totalData,
            (double)(num_of_tables * num_of_DPT) / totalT, totalT * 1000);
    printf("select %10s took %.6f second(s)\n", aggreFunc[j], totalT);
  }
  fprintf(fp, "\n");

  fclose(fp);
  return NULL;
}

void *readMetric(void *sarg) {
  info *rinfo = (info *)sarg;
  TAOS *taos = rinfo->taos;
  char command[BUFFER_SIZE] = "\0";
  FILE *fp = fopen(rinfo->fp, "a");
  int num_of_DPT = rinfo->nrecords_per_table;
  int num_of_tables = rinfo->end_table_id - rinfo->start_table_id + 1;
  int totalData = num_of_DPT * num_of_tables;
  bool do_aggreFunc = rinfo->do_aggreFunc;

  int n = do_aggreFunc ? (sizeof(aggreFunc) / sizeof(aggreFunc[0])) : 2;
  if (!do_aggreFunc) {
    printf("\nThe first field is either Binary or Bool. Aggregation functions are not supported.\n");
  }
  printf("%d records:\n", totalData);
  fprintf(fp, "Querying On %d records:\n", totalData);

  for (int j = 0; j < n; j++) {
    char condition[BUFFER_SIZE - 30] = "\0";
    char tempS[64] = "\0";

    int m = 10 < num_of_tables ? 10 : num_of_tables;

    for (int i = 1; i <= m; i++) {
      if (i == 1) {
        sprintf(tempS, "index = %d", i);
      } else {
        sprintf(tempS, " or index = %d ", i);
      }
      strcat(condition, tempS);

      sprintf(command, "select %s from m1 where %s", aggreFunc[j], condition);

      printf("Where condition: %s\n", condition);
      fprintf(fp, "%s\n", command);

      double t = getCurrentTime();

      TAOS_RES *pSql = taos_query(taos, command);
      int32_t code = taos_errno(pSql);

      if (code != 0) {
        fprintf(stderr, "Failed to query:%s\n", taos_errstr(pSql));
        taos_free_result(pSql);
        taos_close(taos);
        exit(1);
      }
      int count = 0;
      while (taos_fetch_row(pSql) != NULL) {
        count++;
      }
      t = getCurrentTime() - t;

      fprintf(fp, "| Speed: %12.2f(per s) | Latency: %.4f(ms) |\n", num_of_tables * num_of_DPT / t, t * 1000);
      printf("select %10s took %.6f second(s)\n\n", aggreFunc[j], t);

      taos_free_result(pSql);
    }
    fprintf(fp, "\n");
  }

  fclose(fp);
  return NULL;
}

void queryDB(TAOS *taos, char *command) {
  int i;
  TAOS_RES *pSql = NULL;
  int32_t   code = -1;

  for (i = 0; i < 5; i++) {
    if (NULL != pSql) {
      taos_free_result(pSql);
      pSql = NULL;
    }
    
    pSql = taos_query(taos, command);
    code = taos_errno(pSql);
    if (0 == code) {
      break;
    }    
  }

  if (code != 0) {
    fprintf(stderr, "Failed to run %s, reason: %s\n", command, taos_errstr(pSql));
    taos_free_result(pSql);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }

  taos_free_result(pSql);
}

// sync insertion
void *syncWrite(void *sarg) {
  info *winfo = (info *)sarg;
  char buffer[BUFFER_SIZE] = "\0";
  char data[MAX_DATA_SIZE];
  char **data_type = winfo->datatype;
  int len_of_binary = winfo->len_of_binary;
  int ncols_per_record = winfo->ncols_per_record;
  srand(time(NULL));
  int64_t time_counter = winfo->start_time;
  for (int i = 0; i < winfo->nrecords_per_table;) {
    for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; tID++) {
      int inserted = i;
      int64_t tmp_time = time_counter;

      char *pstr = buffer;
      pstr += sprintf(pstr, "insert into %s.%s%d values", winfo->db_name, winfo->tb_prefix, tID);
      int k;
      for (k = 0; k < winfo->nrecords_per_request;) {
        int rand_num = rand() % 100;
        int len = -1;
        if (winfo->data_of_order ==1 && rand_num < winfo->data_of_rate) {
          long d = tmp_time - rand() % 1000000 + rand_num;
          len = generateData(data, data_type, ncols_per_record, d, len_of_binary);
        } else {
          len = generateData(data, data_type, ncols_per_record, tmp_time += 1000, len_of_binary);
        }

        //assert(len + pstr - buffer < BUFFER_SIZE);
        if (len + pstr - buffer >= BUFFER_SIZE) { // too long
          break;
        }

        pstr += sprintf(pstr, " %s", data);
        inserted++;
        k++;

        if (inserted >= winfo->nrecords_per_table) break;
      }

      /* puts(buffer); */
      queryDB(winfo->taos, buffer);

      if (tID == winfo->end_table_id) {
        i = inserted;
        time_counter = tmp_time;
      }
    }
  }
  return NULL;
}

void *asyncWrite(void *sarg) {
  info *winfo = (info *)sarg;

  sTable *tb_infos = (sTable *)malloc(sizeof(sTable) * (winfo->end_table_id - winfo->start_table_id + 1));

  for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; tID++) {
    sTable *tb_info = tb_infos + tID - winfo->start_table_id;
    tb_info->data_type = winfo->datatype;
    tb_info->ncols_per_record = winfo->ncols_per_record;
    tb_info->taos = winfo->taos;
    sprintf(tb_info->tb_name, "%s.%s%d", winfo->db_name, winfo->tb_prefix, tID);
    tb_info->timestamp = winfo->start_time;
    tb_info->counter = 0;
    tb_info->target = winfo->nrecords_per_table;
    tb_info->len_of_binary = winfo->len_of_binary;
    tb_info->nrecords_per_request = winfo->nrecords_per_request;
    tb_info->mutex_sem = &(winfo->mutex_sem);
    tb_info->notFinished = &(winfo->notFinished);
    tb_info->lock_sem = &(winfo->lock_sem);
    tb_info->data_of_order = winfo->data_of_order;
    tb_info->data_of_rate = winfo->data_of_rate;

    /* char buff[BUFFER_SIZE] = "\0"; */
    /* sprintf(buff, "insert into %s values (0, 0)", tb_info->tb_name); */
    /* queryDB(tb_info->taos,buff); */

    taos_query_a(winfo->taos, "show databases", callBack, tb_info);
  }

  sem_wait(&(winfo->lock_sem));
  free(tb_infos);

  return NULL;
}

void callBack(void *param, TAOS_RES *res, int code) {
  sTable *tb_info = (sTable *)param;
  char **datatype = tb_info->data_type;
  int ncols_per_record = tb_info->ncols_per_record;
  int len_of_binary = tb_info->len_of_binary;
  int64_t tmp_time = tb_info->timestamp;

  if (code < 0) {
    fprintf(stderr, "failed to insert data %d:reason; %s\n", code, taos_errstr(res));
    exit(EXIT_FAILURE);
  }

  // If finished;
  if (tb_info->counter >= tb_info->target) {
    sem_wait(tb_info->mutex_sem);
    (*(tb_info->notFinished))--;
    if (*(tb_info->notFinished) == 0) sem_post(tb_info->lock_sem);
    sem_post(tb_info->mutex_sem);
    return;
  }

  char buffer[BUFFER_SIZE] = "\0";
  char data[MAX_DATA_SIZE];
  char *pstr = buffer;
  pstr += sprintf(pstr, "insert into %s values", tb_info->tb_name);

  for (int i = 0; i < tb_info->nrecords_per_request; i++) {
    int rand_num = rand() % 100;
    if (tb_info->data_of_order ==1 && rand_num < tb_info->data_of_rate)
    {
      long d = tmp_time - rand() % 1000000 + rand_num;
      generateData(data, datatype, ncols_per_record, d, len_of_binary);
    } else 
    {
      generateData(data, datatype, ncols_per_record, tmp_time += 1000, len_of_binary);
    }
    pstr += sprintf(pstr, "%s", data);
    tb_info->counter++;

    if (tb_info->counter >= tb_info->target) {
      break;
    }
  }
   tb_info->timestamp = tmp_time;

  taos_query_a(tb_info->taos, buffer, callBack, tb_info);

  taos_free_result(res);
}

double getCurrentTime() {
  struct timeval tv;
  if (gettimeofday(&tv, NULL) != 0) {
    perror("Failed to get current time in ms");
    exit(EXIT_FAILURE);
  }

  return tv.tv_sec + tv.tv_usec / 1E6;
}

int32_t generateData(char *res, char **data_type, int num_of_cols, int64_t timestamp, int len_of_binary) {
  memset(res, 0, MAX_DATA_SIZE);
  char *pstr = res;
  pstr += sprintf(pstr, "(%" PRId64, timestamp);
  int c = 0;

  for (; c < MAX_NUM_DATATYPE; c++) {
    if (strcasecmp(data_type[c], "") == 0) {
      break;
    }
  }

  if (0 == c) {
    perror("data type error!");
    exit(-1);
  }

  for (int i = 0; i < num_of_cols; i++) {
    if (strcasecmp(data_type[i % c], "tinyint") == 0) {
      pstr += sprintf(pstr, ", %d", (int)(rand() % 128));
    } else if (strcasecmp(data_type[i % c], "smallint") == 0) {
      pstr += sprintf(pstr, ", %d", (int)(rand() % 32767));
    } else if (strcasecmp(data_type[i % c], "int") == 0) {
      pstr += sprintf(pstr, ", %d", (int)(rand() % 10)); 
    } else if (strcasecmp(data_type[i % c], "bigint") == 0) {
      pstr += sprintf(pstr, ", %" PRId64, rand() % 2147483648);
    } else if (strcasecmp(data_type[i % c], "float") == 0) {
      pstr += sprintf(pstr, ", %10.4f", (float)(rand() / 1000.0));
    } else if (strcasecmp(data_type[i % c], "double") == 0) {
      double t = (double)(rand() / 1000000.0);
      pstr += sprintf(pstr, ", %20.8f", t);
    } else if (strcasecmp(data_type[i % c], "bool") == 0) {
      bool b = rand() & 1;
      pstr += sprintf(pstr, ", %s", b ? "true" : "false");
    } else if (strcasecmp(data_type[i % c], "binary") == 0) {
      char s[len_of_binary];
      rand_string(s, len_of_binary);
      pstr += sprintf(pstr, ", \"%s\"", s);
    }

    if (pstr - res > MAX_DATA_SIZE) {
      perror("column length too long, abort");
      exit(-1);
    }
  }

  pstr += sprintf(pstr, ")");

  return pstr - res;
}

static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJK1234567890";
void rand_string(char *str, int size) {
  str[0] = 0;
  if (size > 0) {
    --size;
    int n;
    for (n = 0; n < size; n++) {
      int key = rand() % (int)(sizeof charset - 1);
      str[n] = charset[key];
    }
    str[n] = 0;
  }
}
