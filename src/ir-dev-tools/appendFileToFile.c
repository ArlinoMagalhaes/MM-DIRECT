/*
  Appends the contents of a text file (source) to another text file (target). The target file will have its contents plus
  the source contents. This program is useful to increase a the size of a log file.  
*/

#include <stdio.h>
#include <stdlib.h>

char sourceFile[30] = "../logs/sequentialLog1.aof", //Source file
     targetFile[30] = "../logs/sequentialLog2.aof"; //Target file
int times = 7;  //Number of times for appending

int main(void){
  FILE *source, *target; 
  size_t len= 100;
  char *line= malloc(len);
  
  source = fopen(sourceFile, "r");
  
  if(source == NULL){
  	printf("Error while oppening the source file!\n");
  	return 1;
  }else 
  	printf("Source file opened!\n");

  target = fopen(targetFile, "a");
  
  if(target == NULL){
  	printf("Error while oppening the target file!\n");
  	return 1;
  }else printf("Target file opened!\n");

  //usando fprintf para armazenar a string no arquivo
  //fputs(line, target);

  int i = 1;
  while(i <= times){
    printf("Time %d. Coping the records ... \n", i);
    while (getline(&line, &len, source) > 0){
      fputs(line, target);
    }
    i++;
    fclose(source);
    source = fopen(sourceFile, "r");
  }
  
  fclose(source);
  fclose(target);
  
  printf("Finished!\n");
  
  return(0);
}