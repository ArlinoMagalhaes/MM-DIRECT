/*
  Counts the number of records from a Redis log file.  
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

char sourceFile[30] = "../logs/sequentialLog.aof"; //Sequential log file from REDIS database.

char *strupr(char *str){
  unsigned char *p = (unsigned char *)str;
  while (*p) {
     *p = toupper(*p);
      p++;
  }
  return str;
}

int main(void){
  FILE *fp; 
  char con[1000];
  fp =fopen(sourceFile,"r");
  if (!fp){
    printf("Error while oppening the source file!\n");
    return 1;
  }else
    printf("Source file opened!\n");

  unsigned long long setCommands = 0, incrCommands = 0, delCommands = 0, selectCommands = 0;
  printf("Counting records ...\n");
  strcpy(con, strupr(con));
  while (fgets(con,1000, fp)!=NULL){
    strcpy(con, strupr(con));
    if(strstr( con, "SELECT") != NULL)
      selectCommands++;
    if(strstr( con, "SET") != NULL)
      setCommands++;
    if(strstr( con, "INCR") != NULL)
      incrCommands++;
    if(strstr( con, "DEL") != NULL)
      delCommands++;
  }

  printf("Number of SET log records = %llu\n", selectCommands);
  printf("Number of SET log records = %llu\n", setCommands);
  printf("Number of INCR log records = %llu\n", incrCommands);
  printf("Number of DEL log records = %llu\n", delCommands);
  printf("Total = %llu\n", setCommands + incrCommands);
  fclose(fp);
    
  return 0;
}