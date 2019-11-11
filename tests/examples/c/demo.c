#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <power.h>

void powerMsleep(int mseconds);

int main(int argc, char *argv[]) {
  POWER *    power;
  char      qstr[1024];
  POWER_RES *result;

  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");
    return 0;
  }

  // init POWER
  power_init();

  power = power_connect(argv[1], "root", "powerdb", NULL, 0);
  if (power == NULL) {
    printf("failed to connect to server, reason:%s\n", power_errstr(power));
    exit(1);
  }
  printf("success to connect to server\n");
  

  power_query(power, "drop database demo");
  if (power_query(power, "create database demo") != 0) {
    printf("failed to create database, reason:%s\n", power_errstr(power));
    exit(1);
  }
  printf("success to create database\n");

  power_query(power, "use demo");

  // create table
  if (power_query(power, "create table m1 (ts timestamp, speed int)") != 0) {
    printf("failed to create table, reason:%s\n", power_errstr(power));
    exit(1);
  }
  printf("success to create table\n");

  // sleep for one second to make sure table is created on data node
  // powerMsleep(1000);

  // insert 10 records
  int i = 0;
  for (i = 0; i < 10; ++i) {
    sprintf(qstr, "insert into m1 values (%ld, %d)", 1546300800000 + i * 1000, i * 10);
    if (power_query(power, qstr)) {
      printf("failed to insert row: %i, reason:%s\n", i, power_errstr(power));
    }
    //sleep(1);
  }
  printf("success to insert rows, total %d rows\n", i);

  // query the records
  sprintf(qstr, "SELECT * FROM m1");
  if (power_query(power, qstr) != 0) {
    printf("failed to select, reason:%s\n", power_errstr(power));
    exit(1);
  }

  result = power_use_result(power);

  if (result == NULL) {
    printf("failed to get result, reason:%s\n", power_errstr(power));
    exit(1);
  }

  POWER_ROW    row;
  int         rows = 0;
  int         num_fields = power_field_count(power);
  POWER_FIELD *fields = power_fetch_fields(result);
  char        temp[256];

  printf("select * from table, result:\n");
  // fetch the records row by row
  while ((row = power_fetch_row(result))) {
    rows++;
    power_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  power_free_result(result);
  printf("====demo end====\n\n");
  return getchar();
}
