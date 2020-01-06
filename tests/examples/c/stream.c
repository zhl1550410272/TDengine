#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <power.h>

typedef struct {
  char server_ip[64];
  char db_name[64];
  char tbl_name[64];
} param;

int g_thread_exit_flag = 0;

void *insert_rows(void *sarg);

void streamCallBack(void *param, POWER_RES *res, POWER_ROW row) {
  // in this simple demo, it just print out the result
  char temp[128];

  POWER_FIELD *fields = power_fetch_fields(res);
  int numFields = power_num_fields(res);

  power_print_row(temp, row, fields, numFields);

  printf("\n%s\n", temp);
}

int main(int argc, char *argv[]) {
  POWER *power;
  char db_name[64];
  char tbl_name[64];
  char sql[1024] = {0};

  if (argc != 4) {
    printf("usage: %s server-ip dbname tbname\n", argv[0]);
    exit(0);
  }

  // init POWER
  power_init();

  strcpy(db_name, argv[2]);
  strcpy(tbl_name, argv[3]);

  // create pthread to insert into row per second for stream calc
  param *t_param = (param *) malloc(sizeof(param));
  if (NULL == t_param) {
    printf("failed to malloc\n");
    exit(1);
  }
  memset(t_param, 0, sizeof(param));
  strcpy(t_param->server_ip, argv[1]);
  strcpy(t_param->db_name, db_name);
  strcpy(t_param->tbl_name, tbl_name);

  pthread_t pid;
  pthread_create(&pid, NULL, (void *(*)(void *)) insert_rows, t_param);

  sleep(3); // waiting for database is created.
  // open connection to database
  power = power_connect(argv[1], "root", "powerdb", db_name, 0);
  if (power == NULL) {
    printf("failed to connet to server:%s\n", argv[1]);
    free(t_param);
    exit(1);
  }

  // starting stream calc, 
  printf("please input stream SQL:[e.g., select count(*) from tbname interval(5s) sliding(2s);]\n");
  fgets(sql, sizeof(sql), stdin);
  if (sql[0] == 0) {
    printf("input NULL stream SQL, so exit!\n");
    free(t_param);
    exit(1);
  }

  // param is set to NULL in this demo, it shall be set to the pointer to app context 
  POWER_STREAM *pStream = power_open_stream(power, sql, streamCallBack, 0, NULL, NULL);
  if (NULL == pStream) {
    printf("failed to create stream\n");
    free(t_param);
    exit(1);
  }

  printf("presss any key to exit\n");
  getchar();

  power_close_stream(pStream);

  g_thread_exit_flag = 1;
  pthread_join(pid, NULL);

  power_close(power);
  free(t_param);

  return 0;
}


void *insert_rows(void *sarg) {
  POWER *power;
  char command[1024] = {0};
  param *winfo = (param *) sarg;

  if (NULL == winfo) {
    printf("para is null!\n");
    exit(1);
  }

  power = power_connect(winfo->server_ip, "root", "powerdb", NULL, 0);
  if (power == NULL) {
    printf("failed to connet to server:%s\n", winfo->server_ip);
    exit(1);
  }

  // drop database
  sprintf(command, "drop database %s;", winfo->db_name);
  if (power_query(power, command) != 0) {
    printf("failed to drop database, reason:%s\n", power_errstr(power));
    exit(1);
  }

  // create database
  sprintf(command, "create database %s;", winfo->db_name);
  if (power_query(power, command) != 0) {
    printf("failed to create database, reason:%s\n", power_errstr(power));
    exit(1);
  }

  // use database
  sprintf(command, "use %s;", winfo->db_name);
  if (power_query(power, command) != 0) {
    printf("failed to use database, reason:%s\n", power_errstr(power));
    exit(1);
  }

  // create table
  sprintf(command, "create table %s (ts timestamp, speed int);", winfo->tbl_name);
  if (power_query(power, command) != 0) {
    printf("failed to create table, reason:%s\n", power_errstr(power));
    exit(1);
  }

  // insert data
  int64_t begin = (int64_t)time(NULL);
  int index = 0;
  while (1) {
    if (g_thread_exit_flag) break;

    index++;
    sprintf(command, "insert into %s values (%ld, %d)", winfo->tbl_name, (begin + index) * 1000, index);
    if (power_query(power, command)) {
      printf("failed to insert row [%s], reason:%s\n", command, power_errstr(power));
    }
    sleep(1);
  }

  power_close(power);
  return 0;
}

