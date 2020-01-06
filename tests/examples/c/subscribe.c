#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <power.h>

int main(int argc, char *argv[]) 
{
  POWER_SUB   *tsub;
  POWER_ROW    row;
  char        dbname[64], table[64];
  char        temp[256];

  if ( argc == 1 ) {
    printf("usage: %s server-ip db-name table-name \n", argv[0]);
    exit(0);
  } 

  if ( argc >= 2 ) strcpy(dbname, argv[2]);
  if ( argc >= 3 ) strcpy(table, argv[3]);

  tsub = power_subscribe(argv[1], "root", "powerdb", dbname, table, 0, 1000);
  if ( tsub == NULL ) {
    printf("failed to connet to db:%s\n", dbname);
    exit(1);
  }

  POWER_FIELD *fields = power_fetch_subfields(tsub);
  int fcount = power_subfields_count(tsub);

  printf("start to retrieve data\n");
  printf("please use other power client, insert rows into %s.%s\n", dbname, table);
  while ( 1 ) {
    row = power_consume(tsub);
    if ( row == NULL ) break;

    power_print_row(temp, row, fields, fcount);
    printf("%s\n", temp);
  }

  power_unsubscribe(tsub);

  return 0;
}

