/* 
  Arlino Magalhães 
  arlino@ufpi.edu.br


                                    INSTANT RECOVERY TECHINIQUE

  Here are the main functions to implement the instant recovery techinique in Redis database. 
  The instant recovery provides a recovery during the database processing, giving the impression
  that the system was instatly restored. 
  You can read more about the techinique in the works:
    https://doi.org/10.5441/002/edbt.2021.34
    https://doi.org/10.1145/3442197
    http://ceur-ws.org/Vol-2971/paper10.pdf

  Additional files:
    Previous Redis files modficated: server.c, server.h, aof.c, t_string.c, dc.c, 
                                     and src/Makefile
    Files created: redis_ir.conf, an instant_recovery.c.
    Direcories created: datasets, logs, recovery_report, system_monitoring, indexing_report, 
                        graphics, and ir-dev-tools (and its files).
    Libraries included: uthash.h, hiredis.h, and db.h (BerkeleyDB).
    Program included: Memtier benchmark (memtier_benchmark).
*/



#include "server.h"

#include <sys/stat.h> 
#include <assert.h>
#include <libconfig.h>
#include <db.h>

#include "hiredis.h"
#include "uthash.h"



struct client *createFakeClient(void);
void freeFakeClientArgv(struct client *c); 
void freeFakeClient(struct client *c);
void *executeCheckpoint();
int stopMemtierBenchmark();
void insertFirstCommandExecuted (commandExecuted **first_cmd_executed, commandExecuted **last_cmd_executed);
void insertFirstIndexingReport (indexingReport **first_indexing_report, indexingReport **last_indexing_report);
void selfTuneCheckpointTimeInterval(int time_interval);
//void *indexesSequentialLogToIndexedLogV1();
void *indexesSequentialLogToIndexedLogV2();
void stopThredas();
int creat_env();


// ==================================================================================
// Auxiliar functions

//Trims the right of a string
char *ltrim(char *s){
    while(isspace(*s)) s++;
    return s;
}

//Trims the left of a string
char *rtrim(char *s){
    char* back = s + strlen(s);
    while(isspace(*--back));
    *(back+1) = '\0';
    return s;
}

//Trims a string
char *trim(char *s){
    return rtrim(ltrim(s)); 
}

//Removes spaces into a string
void removesSpaces(char str[]) {
    int j = 1;
    for (int i = 1; str[i]; i++) {
        if (str[i] != ' ' || (str[i - 1] != ' ')) {
           str[j] = str[i];
           j++;
        }
    }
    str[j] = '\0';
}

//Splits a string in a vector (char*) by a delimiter (a_delim)
char** str_split(char* a_str, const char a_delim){
    char** result    = 0;
    size_t count     = 0;
    char* tmp        = a_str;
    char* last_comma = 0;
    char delim[2];
    delim[0] = a_delim;
    delim[1] = 0;

    /* Count how many elements will be extracted. */
    while (*tmp)
    {
        if (a_delim == *tmp)
        {
            count++;
            last_comma = tmp;
        }
        tmp++;
    }


    /* Add space for trailing token. */
    count += last_comma < (a_str + strlen(a_str) - 1);

    /* Add space for terminating null string so caller
       knows where the list of returned strings ends. */
    count++;


    result = zmalloc(sizeof(char*) * count);
    //result = (char **) calloc(count, sizeof (char *));


    if (result)
    {
        size_t idx  = 0;
        char* token = strtok(a_str, delim);

        while (token)
        {
            assert(idx < count);
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
        assert(idx == count - 1);
        *(result + idx) = 0;
    }

    return result;
}

//Returns a string containing some information about the system settings.
char *getRedisIRSettings(){
  char* str = zmalloc(600);
  strcpy(str, "");
  if(server.instant_recovery_synchronous == IR_ON)
      strcat(str, "    The Synchronous logging is ON!\n");
  else
      strcat(str, "    The Asynchronous logging is ON!\n");
  if(server.indexedlog_replicated == IR_ON)
      strcat(str, "    The indexed log replication is ON!\n");
  else
      strcat(str, "    The indexed log replication is OFF!\n");
  if(server.log_corruption != 0)
      strcat(str, "    The log corruption is ON!\n");
  else
      strcat(str, "    The log corruption is OFF!\n");
  if(server.checkpoint_state == IR_ON){
      strcat(str, "    The checkpoint is ON! ");
      if(server.checkpoint_state == IR_ON)
        strcat(str, "The auto tune of time interval of checkpoints is ON!!");
      strcat(str, "\n");
  }else
      strcat(str, "    The checkpoint is OFF!\n");
  if(server.memtier_benchmark_state == IR_ON)
      strcat(str, "    Memtier benckmark is ON!\n");
  else
      strcat(str, "    Memtier benckmark is OFF!\n");
  if(server.generate_recovery_report == IR_ON)
      strcat(str, "    Report generation is ON!\n");
  else
      strcat(str, "    Report generation is OFF!\n");
  if(server.generate_executed_commands_csv == IR_ON)
      strcat(str, "    CSV file generation is ON!\n");
  else
      strcat(str, "    CSV file generation is OFF!\n");

  return str;       
}

/*
    Reads an int value from a binary file by a filename parameter.
    If the binary file does not exist, returns -1.
    filename: name of the file
*/
int readFile(char *filename){
  FILE *binaryFile = fopen(filename, "rb"); 

    if (binaryFile == NULL){
        return -1;
    }

    int counter = 0;

    int result = fread( &counter, sizeof(int), 1, binaryFile );
    fclose(binaryFile);
    if(result == 0)
      counter = 0;
    return counter;
}

/*
    Writes an int value to a binary file by a filename parameter.
    Returns true if the wrinting was successful. 
    If the binary file does not exist, returns -1.
    filename: name of the file
    counter: value stored in the file
*/
int writeFile(char *filename, int counter){
  FILE *binaryFile = fopen(filename, "wb");

  if (binaryFile == NULL){
    return -1;
  }
    
  int result = fwrite (&counter, sizeof(unsigned long long int), 1, binaryFile);
  fclose(binaryFile);

  return result;
}

/*
  Removes a file. 
  Returns 0 if the wrinting was successful. Otherwise, returns a non-zero value.
  filename: name of the file
*/
int removeFile(char *filename){
  return remove(filename);
}

/*
    Initializes IR parameters from redis_ir.conf at database restart.
*/
void initializeIRParameters(){
  // Initialization e configuration of server parameters
  config_t cfg;
  const char *str;

  config_init(&cfg);

  /* Read the Redis-IR conf file. If there is an error, report it and exit. */
  if(! config_read_file(&cfg, "../redis_ir.conf")){
    serverLog(LL_NOTICE, "Error in 'redis_ir.conf' configuration file in Redis-IR root path. "
                          "Error message: %s; error line: %d. \n", config_error_text(&cfg), config_error_line(&cfg));
    config_destroy(&cfg);
    exit(0);
  }

  //server.aof_filename
  if(config_lookup_string(&cfg, "aof_filename", &str)){
    server.aof_filename = sdsnew(str);
  }
  else{
    serverLog(LL_NOTICE, "No 'aof_filename' setting in 'redis_ir.conf' configuration file in Redis-IR root path.\n");
    exit(0);
  }

  //server.instant_recovery_state
  if(config_lookup_string(&cfg, "instant_recovery_state", &str)){
    if(strcmp(str, "ON") == 0)
      server.instant_recovery_state = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.instant_recovery_state = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'instant_recovery_state' in 'redis_ir.conf' configuration file in "
                                "Redis-IR root path. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    serverLog(LL_NOTICE, "No 'instant_recovery_state' setting in 'redis_ir.conf' configuration file in Redis-IR root path. "
                    "Use \"ON\" or \"OFF\" values.\n");
    exit(0);
  }

  //server.indexedlog_structure
  if(config_lookup_string(&cfg, "indexedlog_structure", &str)){
    if(strcmp(str, "BTREE") == 0 || strcmp(str, "HASH") == 0){
        strcpy(server.indexedlog_structure, str);
    }else{
        serverLog(LL_NOTICE, "Invalid setting for 'indexedlog_structure' in 'redis_ir.conf' configuration file in "
                                "Redis-IR root path. Use \"BTREE\" or \"HASH\" values.\n");
        exit(0);
      }
  }
  else{
    serverLog(LL_NOTICE,  "No 'indexedlog_structure' setting in 'redis_ir.conf' configuration file in Redis-IR root path."
                        " Use \"BTREE\" or \"HASH\" values.\n");
    exit(0);
  }

  //server.instant_recovery_synchronous
  if(server.instant_recovery_state == IR_ON){
    if(config_lookup_string(&cfg, "instant_recovery_synchronous", &str)){
      if(strcmp(str, "ON") == 0)
        server.instant_recovery_synchronous = IR_ON;
      else
        if(strcmp(str, "OFF") == 0)
          server.instant_recovery_synchronous = IR_OFF;
        else{
          serverLog(LL_NOTICE,  "Invalid 'instant_recovery_synchronous' setting in 'redis_ir.conf' configuration file in "
                            "Redis-IR root path. Use \"ON\" or \"OFF\" values.\n");
          exit(0);
        }
    }
    else{
      server.instant_recovery_synchronous = IR_OFF; //default value
    }
  }

  //server.indexedlog_filename
  if(config_lookup_string(&cfg, "indexedlog_filename", &str)){
    server.indexedlog_filename = sdsnew(str);
  }
  else{
    serverLog(LL_NOTICE, "No 'indexedlog_filename' setting in 'redis_ir.conf' configuration file.\n");
    exit(0);
  }

  //server.starts_log_indexing
  if(config_lookup_string(&cfg, "starts_log_indexing", &str)){
    if(strcmp(str, "A") == 0 || strcmp(str, "B") == 0)
      strcpy(server.starts_log_indexing, str);
    else{
      serverLog(LL_NOTICE,  "Invalid 'starts_log_indexing' setting in 'redis_ir.conf' configuration file in "
                            "Redis-IR root path. Use \"B\" (before) or \"A\" (after) values.\n");
      exit(0);
    }
  }
  else{
    strcpy(server.starts_log_indexing, "A");
  }

  int int_aux;
  //server.indexer_time_interval
  if(config_lookup_int(&cfg, "indexer_time_interval", &int_aux)){
    server.indexer_time_interval = int_aux;
  }
  else{
    server.indexer_time_interval = 500000; //default value
  }

  //server.redisHostname
  if(server.instant_recovery_state == IR_ON){
    if(config_lookup_string(&cfg, "redisHostname", &str)){
      server.redisHostname = sdsnew(str);
    }
    else{
      server.redisHostname = sdsnew("127.0.0.1");
    }
  }

  //server.redisPort
  if(config_lookup_int(&cfg, "redisPort", &int_aux)){
    server.redisPort = int_aux;
  }
  else{
    server.redisPort = 6379; //default value
  }

  //server.display_restorer_information
  if(config_lookup_string(&cfg, "display_restorer_information", &str)){
      if(strcmp(str, "ON") == 0)
        server.display_restorer_information = IR_ON;
      else
        if(strcmp(str, "OFF") == 0)
          server.display_restorer_information = IR_OFF;
        else{
          serverLog(LL_NOTICE, "Invalid 'display_restorer_information' setting in 'redis_ir.conf' configuration file in Redis-IR "
                            "root path. Use \"ON\" or \"OFF\" values.\n");
          exit(0);
        }
  }else{
      server.display_restorer_information = IR_OFF; //default value
  }

  //server.restorer_information_time_interaval
  if(config_lookup_int(&cfg, "restorer_information_time_interaval", &int_aux)){
    server.restorer_information_time_interaval = int_aux;
  }
  else{
    server.restorer_information_time_interaval = 60; //default value
  }

  //server.display_indexer_information
  if(config_lookup_string(&cfg, "display_indexer_information", &str)){
      if(strcmp(str, "ON") == 0)
        server.display_indexer_information = IR_ON;
      else
        if(strcmp(str, "OFF") == 0)
          server.display_indexer_information = IR_OFF;
        else{
          serverLog(LL_NOTICE, "Invalid 'display_indexer_information' setting in 'redis_ir.conf' configuration file in Redis-IR "
                            "root path. Use \"ON\" or \"OFF\" values.\n");
          exit(0);
        }
  }else{
      server.display_indexer_information = IR_OFF; //default value
  }

  //server.indexer_information_time_interaval
  if(config_lookup_int(&cfg, "indexer_information_time_interaval", &int_aux)){
    server.indexer_information_time_interaval = int_aux;
  }
  else{
    server.indexer_information_time_interaval = 60; //default value
  }

  //server.indexedlog_replicated
  if(config_lookup_string(&cfg, "indexedlog_replicated", &str)){
    if(strcmp(str, "ON") == 0)
      server.indexedlog_replicated = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.indexedlog_replicated = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'indexedlog_replicated' in 'redis_ir.conf' configuration file in "
                                "Redis-IR root path. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.indexedlog_replicated = IR_OFF;
  }

  //server.indexedlog_replicated_filename
  if(config_lookup_string(&cfg, "indexedlog_replicated_filename", &str)){
    server.indexedlog_replicated_filename = sdsnew(str);
  }
  else{
    server.indexedlog_replicated_filename = "logs/indexedLog_rep.db";
  }

  //server.log_corruption
  if(config_lookup_int(&cfg, "log_corruption", &int_aux)){
    server.log_corruption = int_aux;
  }
  else{
    server.log_corruption = 0; //default value
  }

  //server.rebuild_indexedlog
  if(config_lookup_string(&cfg, "rebuild_indexedlog", &str)){
    if(strcmp(str, "ON") == 0)
      server.rebuild_indexedlog = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.rebuild_indexedlog = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'rebuild_indexedlog' in 'redis_ir.conf' configuration file in Redis-IR "
                                "root path. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.rebuild_indexedlog = IR_OFF;
  }

  //server.checkpoint_state
  if(config_lookup_string(&cfg, "checkpoint_state", &str)){
    if(strcmp(str, "ON") == 0)
      server.checkpoint_state = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.checkpoint_state = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'checkpoint_state' in 'redis_ir.conf' configuration file in Redis-IR "
                                "root path. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    serverLog(LL_NOTICE, "No 'checkpoint_state' setting in 'redis_ir.conf' configuration file in Redis-IR root path. "
                    "Use \"ON\" or \"OFF\" values.\n");
    exit(0);
  }

  //server.checkpoints_only_mfu
  if(config_lookup_string(&cfg, "checkpoints_only_mfu", &str)){
    if(strcmp(str, "ON") == 0)
      server.checkpoints_only_mfu = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.checkpoints_only_mfu = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'checkpoints_only_mfu' in 'redis_ir.conf' configuration file in Redis-IR "
                                "root path. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.checkpoints_only_mfu = IR_OFF;
  }
  if(server.checkpoints_only_mfu == IR_ON)
    server.accessed_tuples_logger_state = IR_ON;
  else
    server.accessed_tuples_logger_state = IR_OFF; 

    //server.first_checkpoint_start_time
  if(config_lookup_int(&cfg, "first_checkpoint_start_time", &int_aux)){
    server.first_checkpoint_start_time = int_aux;
  }
  else{
    server.first_checkpoint_start_time = 0; //default value
  }

  //server.checkpoint_time_interval
  if(config_lookup_int(&cfg, "checkpoint_time_interval", &int_aux)){
    server.checkpoint_time_interval = int_aux;
  }
  else{
    server.checkpoint_time_interval = 60; //default value
  }

  //server.selftune_checkpoint_time_interval
  if(config_lookup_string(&cfg, "selftune_checkpoint_time_interval", &str)){
    if(strcmp(str, "ON") == 0)
      server.selftune_checkpoint_time_interval = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.selftune_checkpoint_time_interval = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'selftune_checkpoint_time_interval' in 'redis_ir.conf' configuration "
                              "file in Redis-IR root path. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.selftune_checkpoint_time_interval = IR_OFF; //default value
  }

  //server.number_checkpoints
  if(config_lookup_int(&cfg, "number_checkpoints", &int_aux)){
    server.number_checkpoints = int_aux;
  }
  else{
    server.number_checkpoints = 0; //default value
  }

  //server.stop_checkpoint_after_benchmark
  if(config_lookup_string(&cfg, "stop_checkpoint_after_benchmark", &str)){
    if(strcmp(str, "ON") == 0)
      server.stop_checkpoint_after_benchmark = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.stop_checkpoint_after_benchmark = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'stop_checkpoint_after_benchmark' in 'redis_ir.conf' configuration "
                              "file in Redis-IR root path. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.stop_checkpoint_after_benchmark = IR_ON; //default value
  }

  //server.display_checkpoint_information
  if(config_lookup_string(&cfg, "display_checkpoint_information", &str)){
    if(strcmp(str, "ON") == 0)
      server.display_checkpoint_information = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.display_checkpoint_information = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'stop_checkpoint_after_benchmark' in 'redis_ir.conf' configuration "
                              "file in Redis-IR root path. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.display_checkpoint_information = IR_OFF; //default value
  }

  //server.generate_recovery_report
  if(config_lookup_string(&cfg, "generate_recovery_report", &str)){
    if(strcmp(str, "ON") == 0)
      server.generate_recovery_report = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.generate_recovery_report = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'generate_recovery_report' in 'redis_ir.conf' configuration file. "
                                "Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.generate_recovery_report = IR_OFF;//default value
  }

  //server.recovery_report_filename
  if(config_lookup_string(&cfg, "recovery_report_filename", &str)){
      server.recovery_report_filename = sdsnew(str);
  }
  else{
    if(server.generate_recovery_report == IR_ON){
      server.recovery_report_filename = sdsnew("recovery_report.txt");   
    }
  }

//server.generate_indexing_report_csv
  if(config_lookup_string(&cfg, "generate_indexing_report_csv", &str)){
    if(strcmp(str, "ON") == 0)
      server.generate_indexing_report_csv = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.generate_indexing_report_csv = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'generate_indexing_report_csv' in 'redis_ir.conf' configuration file. "
                                "Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.generate_indexing_report_csv = IR_OFF;//default value
  }

  //server.indexing_report_csv_filename
  if(config_lookup_string(&cfg, "indexing_report_csv_filename", &str)){
      server.indexing_report_csv_filename = sdsnew(str);
  }
  else{
      server.indexing_report_csv_filename = sdsnew("recovery_report.txt");   
  }

  //server.generate_executed_commands_csv
  if(config_lookup_string(&cfg, "generate_executed_commands_csv", &str)){
    if(strcmp(str, "ON") == 0){
      server.generate_executed_commands_csv = IR_ON;
    }
    else
      if(strcmp(str, "OFF") == 0)
        server.generate_executed_commands_csv = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'generate_executed_commands_csv' in 'redis_ir.conf' configuration file. "
                                "Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.generate_executed_commands_csv = IR_OFF; //Default value
  }

  //server.generate_setir_executed_commands_csv
  /*if(config_lookup_string(&cfg, "generate_setir_executed_commands_csv", &str)){
    if(strcmp(str, "ON") == 0){
      server.generate_setir_executed_commands_csv = IR_ON;
    }
    else
      if(strcmp(str, "OFF") == 0)
        server.generate_setir_executed_commands_csv = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'generate_setir_executed_commands_csv' in 'redis_ir.conf' "
                                  "configuration file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.generate_setir_executed_commands_csv = IR_OFF; //Default value
  }*/

  //server.executed_commands_csv_filename
  if(server.generate_executed_commands_csv == IR_ON){
    if(config_lookup_string(&cfg, "executed_commands_csv_filename", &str)){
      server.executed_commands_csv_filename = sdsnew(str);
    }
    else{
      if(server.generate_executed_commands_csv == IR_ON){
        serverLog(LL_NOTICE, "No 'executed_commands_csv_filename' setting in 'redis_ir.conf' configuration file.\n");
        exit(0);
      }
    }
  }

  //server.memtier_benchmark_state
  if(config_lookup_string(&cfg, "memtier_benchmark_state", &str)){
    if(strcmp(str, "ON") == 0)
      server.memtier_benchmark_state = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.memtier_benchmark_state = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'memtier_benchmark_state' in 'redis_ir.conf' configuration file. "
                                "Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.memtier_benchmark_state = IR_OFF; //default value
  }

  //memtier_benchmark_workload_run_times
  if(config_lookup_int(&cfg, "memtier_benchmark_workload_run_times", &int_aux)){
    server.memtier_benchmark_workload_run_times = int_aux;
  }
  else{
    server.memtier_benchmark_workload_run_times = 1; //default value
  }

  //restart_after_benchmarking
  if(config_lookup_int(&cfg, "restart_after_benchmarking", &int_aux)){
    server.restart_after_benchmarking = int_aux;
  }
  else{
    server.restart_after_benchmarking = 0; //default value
  }

  //time_tostop_benchmarking
  if(config_lookup_int(&cfg, "time_tostop_benchmarking", &int_aux)){
    server.time_tostop_benchmarking = int_aux;
  }
  else{
    server.time_tostop_benchmarking = 0; //default value
  }

  //restart_after_time
  if(config_lookup_int(&cfg, "restart_after_time", &int_aux)){
    server.restart_after_time = int_aux;
  }
  else{
    server.restart_after_time = 200; //default value
  }

//number_restarts_after_time
  if(config_lookup_int(&cfg, "number_restarts_after_time", &int_aux)){
    server.number_restarts_after_time = int_aux;
  }
  else{
    server.number_restarts_after_time = 0; //default value
  }

  //preload_database_and_restart
  if(config_lookup_int(&cfg, "preload_database_and_restart", &int_aux)){
    server.preload_database_and_restart = int_aux;
  }
  else{
    server.preload_database_and_restart = 0; //default value
  }

  //number_restarts_after_preloading
  if(config_lookup_int(&cfg, "number_restarts_after_preloading", &int_aux)){
    server.number_restarts_after_preloading = int_aux;
  }
  else{
    server.number_restarts_after_preloading = 1; //default value
  }

  if( (server.time_tostop_benchmarking > 0 || server.restart_after_benchmarking > 0) 
        && server.preload_database_and_restart > 0){
    serverLog(LL_NOTICE, "You cannot use 'preload_database_and_restart' if 'restart_after_benchmarking' or "
                          "'time_tostop_benchmarking' are enabled. Set the options in "
                          "'redis_ir.conf' configuration file.\n");
    exit(0);
  }

  //server.memtier_benchmark_parameters
  if(config_lookup_string(&cfg, "memtier_benchmark_parameters", &str)){
    strcpy(server.memtier_benchmark_parameters, str);
  }
  else{
    strcpy(server.memtier_benchmark_parameters, "");
  }

  //server.start_memtier_benchmark
  if(config_lookup_string(&cfg, "start_memtier_benchmark", &str)){
    if(strcmp(str, "S") == 0 || strcmp(str, "R") == 0)
      strcpy(server.start_memtier_benchmark, str);
    else{
      serverLog(LL_NOTICE, "Invalid setting for 'start_memtier_benchmark' in 'redis_ir.conf' configuration file. "
                                "Use \"R\" or \"S\" values.\n");
      exit(0);
    }
  }
  else{
    strcpy(server.start_memtier_benchmark, "R");
  }

  //server.generate_report_file_after_benchmarking
  if(config_lookup_string(&cfg, "generate_report_file_after_benchmarking", &str)){
    if(strcmp(str, "ON") == 0)
       server.generate_report_file_after_benchmarking = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
         server.generate_report_file_after_benchmarking = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'generate_report_file_after_benchmarking' in 'redis_ir.conf' configuration "
                                "file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }

  //server.overwrite_report_files
  if(config_lookup_string(&cfg, "overwrite_report_files", &str)){
    if(strcmp(str, "ON") == 0)
       server.overwrite_report_files = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
         server.overwrite_report_files = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'overwrite_report_files' in 'redis_ir.conf' configuration "
                                "file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.overwrite_report_files = IR_ON; //default value
  }

  //server.system_monitoring
  if(config_lookup_string(&cfg, "system_monitoring", &str)){
    if(strcmp(str, "ON") == 0){
       server.system_monitoring = IR_ON;
    }else
      if(strcmp(str, "OFF") == 0){
         server.system_monitoring = IR_OFF;
      }else{
        serverLog(LL_NOTICE, "Invalid setting for 'system_monitoring' in 'redis_ir.conf' configuration "
                                "file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.system_monitoring = IR_OFF; //default value
  }

  //server.stop_system_monitoring_end_benckmark
  if(config_lookup_string(&cfg, "stop_system_monitoring_end_benckmark", &str)){
    if(strcmp(str, "ON") == 0){
       server.stop_system_monitoring_end_benckmark = IR_ON;
    }else
      if(strcmp(str, "OFF") == 0){
         server.stop_system_monitoring_end_benckmark = IR_OFF;
      }else{
        serverLog(LL_NOTICE, "Invalid setting for 'stop_system_monitoring_end_benckmark' in 'redis_ir.conf' configuration "
                                "file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.stop_system_monitoring_end_benckmark = IR_ON; //default value
  }

  //server.system_monitoring_csv_filename
  if(config_lookup_string(&cfg, "system_monitoring_csv_filename", &str)){
    server.system_monitoring_csv_filename = sdsnew(str);
  }
  else{
    server.system_monitoring_csv_filename = "system_monitoring/system_monitoring.csv";
  }

  //system_monitoring_time_interval
  if(config_lookup_int(&cfg, "system_monitoring_time_interval", &int_aux)){
    server.system_monitoring_time_interval = int_aux;
  }
  else{
    server.system_monitoring_time_interval = 1; //default value
  }

  //server.overwrite_system_monitoring
  if(config_lookup_string(&cfg, "overwrite_system_monitoring", &str)){
    if(strcmp(str, "ON") == 0){
       server.overwrite_system_monitoring = IR_ON;
    }else
      if(strcmp(str, "OFF") == 0){
         server.overwrite_system_monitoring = IR_OFF;
      }else{
        serverLog(LL_NOTICE, "Invalid setting for 'overwrite_system_monitoring' in 'redis_ir.conf' configuration "
                                "file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.overwrite_system_monitoring = IR_ON; //default value
  }

  config_destroy(&cfg);

  if(creat_env()){
    serverLog(LL_NOTICE, "Indexed log Environment started!");
  }else{
    serverLog(LL_NOTICE, "The system was not started! Indexeed log Environment could not be started!");
    exit(0);
  }


  //Initialize the linked list of commands executed
  if(server.generate_executed_commands_csv == IR_ON)
    insertFirstCommandExecuted (&first_cmd_executed_List, &last_cmd_executed_List);

  //Initialize the linked list of indexing report
  if(server.generate_indexing_report_csv == IR_ON)
      insertFirstIndexingReport (&first_indexing_report, &last_indexing_report);
}

// ==================================================================================
// Functions to handle Berkeley DB

//Creates a Berkeley DB environment
int creat_env(){
  u_int32_t env_flags; /* env open flags */
  int ret; /* function return value */

  /* 
   Create an environment object and initialize it for error
   reporting. 
  */
  ret = db_env_create(&server.IR_env, 0);
  if (ret != 0) {
   serverLog(LL_NOTICE,  "Error creating Environment handle: %s\n", db_strerror(ret));
   return 0;
  }

  /* Open the environment. */
  env_flags = DB_CREATE | /* If the environment does not exist, create it. */
              DB_INIT_MPOOL|
              DB_THREAD; /* Initialize the in-memory cache. */

  ret = server.IR_env->open(server.IR_env,       /* DB_ENV ptr */
                    "", /* env home directory */
                    env_flags,          /* Open flags */
                    0);                 /* File mode (default) */
  if (ret != 0) {
   serverLog(LL_NOTICE,  "Environment open failed: %s", db_strerror(ret));
   return 0;
  }
  return 1;
}

/*
Opens Berkeley DB.
Returns a pointer to hadle the Berkeley database.
file_name: Berkeley database file name;
flags: 
    DB_CREATE: Create the underlying database and any necessary physical files.
    DB_NOMMAP: Do not map this database into process memory.
    DB_RDONLY: Treat the data base as read-only.
    DB_THREAD: The returned handle is free-threaded, that is, it can be used simultaneously by multiple threads within the process. 
    DB_TRUNCATE: Physically truncate the underlying database file, discarding all databases it contained.
    DB_UPGRADE: Upgrade the database format as necessary. 
duplicates: 
    DB_DUP: The database supports non-sorted duplicate records.
    DB_DUPSORT: The database supports sorted duplicate records. Note that this flag also sets the DB_DUP flag for you.
data_structure: database access method (DB_BTREE, DB_HASH, DB_HEAP, DB_QUEUE, DB_RECNO, or DB_UNKNOWN)
retult: return 0 (zero) if the indexel log is openned. If fail, return a code error.
*/
DB* openBerkeleyDB(char* file_name, u_int32_t flags, u_int32_t duplicates, u_int32_t data_structure, int *result){

    DB *BDB_database;//A pointer to the database

    int ret = db_create(&BDB_database, server.IR_env, 0);
    *result = ret;
    if (ret != 0) {
        serverLog(LL_NOTICE,"Error while creating the BerkeleyDB database! \n");
        return BDB_database;
    }
    *result = ret;
  
  /* We want to support duplicates keys*/
    if(duplicates == DB_DUP || duplicates == DB_DUPSORT){
        ret = BDB_database->set_flags(BDB_database, DB_DUP);
        if (ret != 0) {
            serverLog(LL_NOTICE,"Error while setting the DB_DUMP flag on BerkeleyDB! %s\n", db_strerror(ret));
            return BDB_database;
        }
    }  

    /* open the database */
    ret = BDB_database->open(BDB_database,      /* DB structure pointer */
                            NULL,               /* Transaction pointer */
                            file_name,          /* On-disk file that holds the database. */
                            NULL,               /* Optional logical database name */
                            data_structure,     /* Database access method */
                            flags,              /* If the database does not exist, create it. */
                            0);                 /* File mode (using defaults) */
    *result = ret;  

    if (ret != 0) {
        serverLog(LL_NOTICE,"Error while openning the BerkeleyDB database! %s", db_strerror(ret));
        return BDB_database;
    }

    return BDB_database;
}

/* 
Insert a pair key/data to BerkeleyDB
Return a non-zero DB->put() error if fail
*/
int addDataBerkeleyDB(DB *BDB_database, DBT key, DBT data){
  int error;

  error = BDB_database->put(BDB_database, NULL, &key, &data, 0);
  if ( error == 0){
    ;//printf("db: %s : key stored.\n", (char *)key.data);
  }else{
    BDB_database->err(BDB_database, error, "DB->put error: ");  
  } 

  return error;
}

/*
Get a pair key/data on BerkeleyDB
Return a record in DBT format or null (if it does not exist)
*/
DBT getDataBerkeleyDB(DB *dbp, DBT key){
    DBT data;
    int error;
    
    error = dbp->get(dbp, NULL, &key, &data, 0);    
    if (error != 0)
        dbp->err(dbp, error, "DB->get error key = %s", (char *)key.data);
    
    return data;
}

/* 
    Delete a data on BerkeleyDB
    Return a non-zero DB->del() error if fail
*/
int delDataBerkeleyDB(DB *dbp, DBT key){
    int error;

    error = dbp->del(dbp, NULL, &key, 0);

    return error;
}


// ==================================================================================
// Functions to store and access records in the indexed log using Berkeley DB funcitons


/*
Opens the indexed log.
Returns a pointer to hadle the Berkeley database.
file_name: Berkeley database file name;
mode: 
    W: Create the underlying database and any necessary physical files.
    R: Treat the data base as read-only.
    T: The returned handle is free-threaded, that is, it can be used simultaneously by multiple threads within the process. 
retult: returns 0 (zero) if the indexed log is openned. If fail, returns a code error.
*/
DB* openIndexedLog(char* file_name, char mode, int *result){
    u_int32_t flags;
    switch (mode){
        case 'W': flags = DB_CREATE; break;
        case 'R': flags = DB_RDONLY; break;
        case 'T': flags = DB_THREAD; break;
        default:
        serverLog(LL_NOTICE,"Invalide database openning mode! \n");
        exit(0);
    }

    u_int32_t data_structure;
    if(strcmp(server.indexedlog_structure,"BTREE") == 0)
        data_structure = DB_BTREE;
    else
        if(strcmp(server.indexedlog_structure,"HASH") == 0)
           data_structure = DB_HASH;
        else
            data_structure = DB_BTREE;

    return openBerkeleyDB(file_name, flags, DB_DUP, data_structure, result);
}

/*
Close the BerkeleyDB
*/
void closeIndexedLog(DB* dbp){
  if (dbp != NULL)
    dbp->close(dbp, 0);
}

/*
Closes the BerkeleyDB and does not flush the data to sencondary memory
*/
void closeIndexedLogNoSync(DB* dbp){
  if (dbp != NULL)
    dbp->close(dbp, DB_NOSYNC);
}

/* 
Insert a log record (sting) by its tuple key (string) to indexed log (BerkeleyDB)
Return a non-zero DB->put() error if fail
*/
int addRecordIndexedLog(DB* dbp, char* key, char* data){
  DBT key2, data2;

  memset(&key2, 0, sizeof(DBT));
  memset(&data2, 0, sizeof(DBT));

  key2.data = key;
  key2.size = strlen(key) + 1;

  data2.data = data;
  data2.size = strlen(data) + 1; 

  return addDataBerkeleyDB(dbp, key2, data2);
}

/*
Get a log record (sting) by its tuple key (string) on indexed log (BerkeleyDB)
Return a record record or null (if it does not exist)
*/
char* getRecordIndexedLog(DB* dbp,  char* key){
    DBT key2, data;
    int error;

    memset(&key2, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key2.data = key;
    key2.size = strlen(key) + 1;

    error = dbp->get(dbp, NULL, &key2, &data, 0);    
    if (error != 0)
        return NULL;

    return (char *)data.data;
}

/* 
    Delete a log record on indexed log by a key (string)
    Return a non-zero DB->del() error if fail
*/
int delRecordIndexdLog(DB* dbp, char *key){
    DBT key2;
    int error;

    memset(&key2, 0, sizeof(DBT));

    key2.data = key;
    key2.size = strlen(key) + 1;

    error = delDataBerkeleyDB(dbp, key2);

    return error;
}

/*
    Returns the number of log records in the indexed log.
*/
unsigned long long countRecordsIndexedLog(DB* dbp){
  DBC *cursorp;
  DBT key, data;
  int error;
  unsigned long long count = 0;

  /* Zero out the DBTs before using them. */
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  /* Get a cursor */
  dbp->cursor(dbp, NULL, &cursorp, 0); 

    /* Iterate over the database, retrieving each record in turn. */
  while ((error = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    count++;
  }
  if (error != DB_NOTFOUND) {
  /* Error handling goes here */
  }

  // Cursors must be closed
  if (cursorp != NULL)
    cursorp->close(cursorp); 

  return count;
}

/*
    Returns the number of tuples in the indexed log.
*/
unsigned long long countTuplesIndexedLog(DB* dbp){
  DBC *cursorp;
  DBT key, data;
  int error;
  unsigned long long count = 0;

  /* Zero out the DBTs before using them. */
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  /* Get a cursor */
  dbp->cursor(dbp, NULL, &cursorp, 0); 

    /* Iterate over the database, retrieving each record in turn. */
  while ((error = cursorp->get(cursorp, &key, &data, DB_NEXT_NODUP)) == 0) {
    count++;
  }
  if (error != DB_NOTFOUND) {
  /* Error handling goes here */
  }

  // Cursors must be closed
  if (cursorp != NULL)
    cursorp->close(cursorp); 

  return count;
}

/*
    Prints the pair key/log record from the indexed log. 
*/
int printIndexedLog(DB* dbp){
  DBC *cursorp;
  DBT key, data;
  int error;

  /* Zero out the DBTs before using them. */
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  /* Get a cursor */
  dbp->cursor(dbp, NULL, &cursorp, 0); 

  unsigned long long i = 1;
  printf("Indexed log:\n");
    /* Iterate over the database, retrieving each record in turn. */
  while ((error = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    printf("%llu: Key[%s] => log[%s]\n", i, (char *)key.data,(char *)data.data);
    i++;
  }
  
  if (error != DB_NOTFOUND) {
  /* Error handling goes here */
  }

  // Cursors must be closed
    if (cursorp != NULL)
    cursorp->close(cursorp); 

  return error;
}

/*
  Implements a Redis function to be performed in redis-cli aplication.
  The function prints the result of the function printIndexedLog();
*/
void printIndex(client *c) {
    int ret;
    DB *dbp = openIndexedLog(server.indexedlog_filename, 'R', &ret);
    if(ret != 0){
        shared.ir_error = createObject(OBJ_STRING,sdsnew(
        "- the indexer could not openned!\r\n"));
        addReply(c,shared.ir_error);
    }else{
        printIndexedLog(dbp);
        closeIndexedLog(dbp);
        addReply(c,shared.ok);
    }
}

// ==================================================================================
// Function of linked lists to store information about the database execution to provide 
// the building of graphichs. 
// The types commandExecuted and indexingReport are implemented in server.h file.

/*
    Inserts the first record in the linked list that is is empty command. 
    This command is added only to optmize inserctions of commands by the addCommandExecuted() 
    function.
 */
void insertFirstCommandExecuted (commandExecuted **first_cmd_executed, commandExecuted **last_cmd_executed){
   commandExecuted *new = (commandExecuted *) zmalloc(sizeof(commandExecuted));
   
   strcpy(new->key, "");
   strcpy(new->command, "");
   new->startTime = 0;
   new->finishTime = 0;
   new->latency = 0;
   new->type = '0';
   new->next = NULL;
   
   *first_cmd_executed = new;
   *last_cmd_executed = new;
}

/*
    Inserts informations about the commands executed on the database in a record on the end 
    of the linked list.
    key: key of the tuple
    command: operation performed on the tuple
    startTime: start time of the command execution
    finishTime: end time  of the command execution
    type: command type
    latency: latency duration time of the command excution
*/
void addCommandExecuted (commandExecuted **last_cmd_executed, char key[50], char command[20], 
                    long long startTime, long long finishTime, char type, long long latency){
   commandExecuted *new = (commandExecuted *) zmalloc(sizeof(commandExecuted));
   
   strcpy(new->key, key);
   strcpy(new->command, command);
   new->startTime = startTime;
   new->finishTime = finishTime;
   new->type = type;
   new->latency = latency;

   (*last_cmd_executed)->next = new;
   *last_cmd_executed = new;
}

/*
    Inserts the first record that is is empty indexing write information. 
    This record is added only to optmize inserctions of records by the addIndexingReport() 
    function.
*/
void insertFirstIndexingReport (indexingReport **first_indexing_report, indexingReport **last_indexing_report){
   indexingReport *new = (indexingReport *) zmalloc(sizeof(indexingReport));
   
   new->indexing_start_time = 0;
   new->indexing_end_time = 0;
   new->records_processed = 0;
   new->records_indexed = 0;
   new->next = NULL;
   
   *first_indexing_report = new;
   *last_indexing_report = new;
}

/*
    Inserts a record containg an indexing write information on the end of a linked list.  
*/
void addIndexingReport (indexingReport **last_indexing_report, long long indexing_start_time, 
                          long long indexing_end_time, unsigned long long int records_processed, 
                          unsigned long long int records_indexed){
   indexingReport *new = (indexingReport *) zmalloc(sizeof(indexingReport));
   
   new->indexing_start_time = indexing_start_time;
   new->indexing_end_time = indexing_end_time;
   new->records_processed = records_processed;
   new->records_indexed = records_indexed;
   new->next = NULL;

   (*last_indexing_report)->next = new;
   *last_indexing_report = new;
}

// ==================================================================================
// Functoins that store information about the database system execution in CSV and text files
// in order to build graphics.

/*
    Flushes to a CSV file the properties of all executed commands, such as: key, command, 
    start time, finish time and the way the value of command was executed (type). 
    See the information fileds in the struct commandExecuted in server.h file.
    
    The three first CSV data lines are not executed commands. They are the time (in seconds) 
    of the database startup, recovery, and benchmark execution. When the time is not obtained, 
    the value is setted as -1.
    Belows the fields used:
        Belows the fields used:
        key                  commands    startTime           finishTime          latency  type
        "Database startup"               <obtained time>                                  0      
        "Recovery"                       <obtained time>     <obtained time>              0      
        "Benchmark"                      <obtained time>     <obtained time>              0      
        "Shutdown"                       <obtained time>                                  0      
    
    Checkpoint process information is stored before the commands executed as bellows:
        key               command           startTime           finishTime         latency  type
       "Checkpoint"       <idCheckpoint>    <obtained time>     <obtained time>             0
       "Checkpoint End"   <idCheckpoint>                        <obtained time>             0

    The remaind CSV data lines are really executed commands.
        key               command               startTime           finishTime          latency                  type              
        <command key>     <command performed>   <obtained time>     <obtained time>     <obtained time>          <command type>
*/
void *printCommandsExecutedToCSV_thread(){
    if(server.generate_executed_commands_csv == IR_OFF)
      return (void *)0;

    serverLog(LL_NOTICE, "Generating information about executed database commands ...");

    char str[600];

    FILE *ptr_file;
    if(server.overwrite_report_files == IR_ON)
      ptr_file = fopen(server.executed_commands_csv_filename, "w");
    else
      ptr_file = fopen(server.executed_commands_csv_filename, "a");

    //Flushes the CSV header
    fputs("key,command,startTime,finishTime,latency,type\n", ptr_file);

    //Flushes the recovery startup time. It is not a command executed.
    if(server.database_startup_time != -1){
      sprintf(str, "Database startup,,%lld,,,0\n",server.database_startup_time);
      fputs(str, ptr_file);
    }

    //Flushes the executed commands.
    commandExecuted *end, 
                    *store = first_cmd_executed_List,
                    *clear = first_cmd_executed_List, *aux; 
    while(server.stop_generate_executed_commands_csv == IR_OFF){
      end = last_cmd_executed_List;

      //stores in the CSV
      while(store != end && store != NULL){
        sprintf(str, "%s,%s,%lld,%lld,%lld,%c\n", store->key, store->command, store->startTime, store->finishTime, store->latency, store->type);
        fputs(str, ptr_file);
        store = store->next;
      }
      fflush(ptr_file);
      
      //clear (free) the stored records
      while(clear != end && clear != NULL){
        aux = clear;
        clear = clear->next;
        zfree(aux);
        first_cmd_executed_List = clear;
      }

      sleep(0.05);
    }
    
    fclose(ptr_file);

    serverLog(LL_NOTICE, "Generation of executed database commands finished!"
                         " See the file 'src/%s' on Redis instalation path. ", 
                         server.executed_commands_csv_filename);

    server.generate_executed_commands_csv = IR_OFF;

    return (void *)0;
}

/*
  Stops the thread printCommandsExecutedToCSV_thread()
*/
void stopCommandsExecuted(){
  server.stop_generate_executed_commands_csv = IR_ON;
}

/*
  Waits until the thread printCommandsExecutedToCSV_thread() is finished
*/
void waitCommandsExecutedFinish(){
  while(server.generate_executed_commands_csv == IR_ON){
    sleep(0.05);
  }
}

/*
    Flushes to a CSV file information about the usage of CPU and memory of the system, 
    such as: memory in megabytes, cpu in percentage, and time of the information collection.
    
    Time (in seconds) of the database startup, recovery, and benchmark execution. 
    When the time is not obtained, the value is setted as -1.
    Belows the fields used:
        Belows the fields used:
        time                 cpu         memory
        "Database startup"               <obtained time>    
        "Recovery"                       <obtained time>     <obtained time>     
        "Benchmark"                      <obtained time>     <obtained time> 
        "Shutdown"                       <obtained time> 
    
    Checkpoint process information is stored before the commands executed as bellows:
        time              cpu               memory
       "Checkpoint"       <idCheckpoint>    <obtained time>     <obtained time>
       "Checkpoint End"   <idCheckpoint>                        <obtained time>

    The remaind CSV data lines are really information obtained.
        time               cpu                memory
        <obtained time>    <obtained value>   <obtained value>
*/

void *printSysteMonitoringToCsv_thread() {
    FILE *arq_temp, *arq_csv;
    char str[200], program[100], line[100], system_monitoring_tmp[50] ="system_monitoring.tmp", system_monitoring_csv_filename[50];
    int i = 0,error = 0, system_monitoring_time_delay = server.system_monitoring_time_interval;
    char** tokens = NULL;

    strcpy(system_monitoring_csv_filename, server.system_monitoring_csv_filename);

    if(server.overwrite_system_monitoring == IR_ON)
      arq_csv = fopen(system_monitoring_csv_filename, "w");
    else
      arq_csv = fopen(system_monitoring_csv_filename, "a");

    serverLog(LL_NOTICE, "Generating system monitoring ... ");

    //Flushes the CSV header
    fputs("time;cpu;memory\n", arq_csv);

    //Flushes the database startup time.
    if(server.database_startup_time != -1){
      sprintf(str, "Database startup;;%lld;\n",server.database_startup_time);
      fputs(str, arq_csv);
    }

    do{
        i++;
        strcpy(program, "top -b -n 1 > ");
        strcat(program, system_monitoring_tmp);
        error = system(program);

        if (error == -1){
            serverLog(LL_NOTICE, "Problem in collecting system monitoring. Erro in command Top. Sytem monitoring collecting aborted!\n");
            return (void *)1;
        }

        arq_temp = fopen(system_monitoring_tmp, "rt");
        if (arq_temp == NULL){
            serverLog(LL_NOTICE,"Problem on openning the system monitoring file. System monitoring collecting aborted!\n");
            return (void *)1;
        }
        //printf("\nPID USUARIO PR NI VIRT RES SHR S CPU MEM TEMPO+ COMANDO\n");

        while (!feof(arq_temp)){
            if (fgets(line, 100, arq_temp))
                if(strstr(line, "redis-s") != NULL){
                    removesSpaces(line);
                    //printf("%s\n",trim(line));

                    tokens = str_split(trim(line), ' ');
                    //printf("CPU = %s\n",tokens[8]);
                    //printf("MEM = %s\n",tokens[5]);

                    sprintf(str, "%lld;%s;%s\n", ustime(), tokens[8], tokens[5]);
                    fputs(str, arq_csv);
                    fflush(arq_csv);
                    break;
                }
        }
        zfree(tokens);
        fclose(arq_temp);

        if(server.system_monitoring == IR_OFF)
          sleep(system_monitoring_time_delay);

    }while(server.system_monitoring == IR_ON);
    fclose(arq_csv);

    serverLog(LL_NOTICE,"System monitoring collecting finished!"
                        " See the file %s.\n", server.system_monitoring_csv_filename);
    server.system_monitoring = IR_OFF;

  return (void *)1;
}

/*
    Kills the collectio of system monitoring.
*/
void stopSystemMonitoringFinish(){
      server.system_monitoring = IR_OFF;
}

/*
  Waits until the Indexer thread to finish
*/
void waitSystemMonitoringFinish(){
  while(server.system_monitoring == IR_ON){
    sleep(0.05);
  }
}

/*
    Flushes to a CSV file of all indexing reports.
    The three first CSV data lines are not executed commands, they are the time (in seconds) of database startup, recovery, and benchmark execution. When the
    time is not obtained, the value is -1.
    Belows the fields used:
        Belows the fields used:
        startTime         finishTime        records_processed   records_indexed   type
        <obtained time>                                                           D    //Database startup
        <obtained time>   <obtained time>                                         R    //Recovery
        <obtained time>   <obtained time>                                         B    //Benchmark
        <obtained time>                                                           S    //Shutdown
    
    Checkpoint process information is stored before the commands executed as bellows:
        startTime           finishTime         records_processed   records_indexed   type
        <obtained time>     <obtained time>                                          C
    
    Fields of indexing:
        startTime         finishTime            records_processed   records_indexed   type
        <command key>     <command performed>   <obtained time>     <obtained time>   I

*/
void *printIndexingReportToCSV_thread(){
    if(server.generate_indexing_report_csv == IR_OFF)
      return (void *)0;

    if(first_indexing_report == NULL)
        return 0;

    serverLog(LL_NOTICE, "Generating information about the indexing write ...");
    
    char str[600];

    FILE *ptr_file;
    if(server.overwrite_report_files == IR_ON)
      ptr_file = fopen(server.indexing_report_csv_filename, "w");
    else
      ptr_file = fopen(server.indexing_report_csv_filename, "a");

    //Flushes the CSV header
    fputs("startTime,finishTime,recordsProcessed,recordsIndexed,type\n", ptr_file);

    //Flushes the database startup time.
    if(server.database_startup_time != -1){
      sprintf(str, "Database startup,,%lld\n",server.database_startup_time);
      fputs(str, ptr_file);
    }

    indexingReport *end,
                    *store = first_indexing_report,
                    *clear = first_indexing_report, *aux;
 
    while(server.stop_generate_indexing_report_csv == IR_OFF){
      end = last_indexing_report;

      //stores the records in the CSV lines
      while(store != end && store != NULL){
        sprintf(str, "%llu,%llu,%lld,%lld,I\n", store->indexing_start_time, store->indexing_end_time, 
                                              store->records_processed, store->records_indexed);
        fputs(str, ptr_file);
        store = store->next;
      }
      fflush(ptr_file);
      
      //clear (free) the records of the linked list
      while(clear != end && clear != NULL){
        aux = clear;
        clear = clear->next;
        zfree(aux);
        first_indexing_report = clear;
      }

      sleep(0.05);
    }

    fclose(ptr_file);

    serverLog(LL_NOTICE, "Indexing report generation finished!  See the file 'src/%s' on Redis instalation path. ", server.indexing_report_csv_filename);

    server.generate_indexing_report_csv = IR_OFF;

    //pthread_exit(NULL); 
    return (void *)0;
}

/*
  Stops the thread printIndexingReportToCSV_thread()
*/
void stopIndexingReport(){
  server.stop_generate_indexing_report_csv = IR_ON;
}

/*
  Waits until the thread printIndexingReportToCSV_thread() is finished
*/
void waitIndexingReportFinish(){
  while(server.generate_indexing_report_csv == IR_ON){
    sleep(0.05);
  }
}

/*
  Flushes to a CSV file the start and end times of the recovery 
*/
void printRecoveryTimeToCSV(){
    if(server.generate_executed_commands_csv == IR_ON){
      char str[600];

      FILE *ptr_file;
      ptr_file = fopen(server.executed_commands_csv_filename, "a");

      //Flushes the start and end times of the recovery. It is not a command executed.
      sprintf(str, "Recovery,,%lld,%lld,,0\n",server.recovery_start_time, server.recovery_end_time);
      fputs(str, ptr_file);

      fclose(ptr_file);
    }

    if(server.generate_indexing_report_csv == IR_ON){
      char str[600];

      FILE *ptr_file;
      ptr_file = fopen(server.indexing_report_csv_filename, "a");

      //Flushes the start and end times of the recovery. It is not a command executed.
      sprintf(str, "Recovery,,%lld,%lld\n",server.recovery_start_time, server.recovery_end_time);
      fputs(str, ptr_file);

      fclose(ptr_file);
    }

    if(server.system_monitoring == IR_ON){
      char str[600];

      FILE *ptr_file;
      ptr_file = fopen(server.system_monitoring_csv_filename, "a");

      //Flushes the start and end times of the recovery. It is not a command executed.
      sprintf(str, "Recovery;;%lld;%lld;\n",server.recovery_start_time, server.recovery_end_time);
      fputs(str, ptr_file);

      fclose(ptr_file);
    }

}

//Flushes to a CSV file the start and end times of the benchmarking.
void printBenchmarkTimeToCSV(){
    if(server.generate_executed_commands_csv == IR_ON){
      char str[600];

      FILE *ptr_file;
      ptr_file = fopen(server.executed_commands_csv_filename, "a");

      sprintf(str, "Benchmark,,%lld,%lld,,0\n",server.memtier_benchmark_start_time, server.memtier_benchmark_end_time);
      fputs(str, ptr_file);

      fclose(ptr_file);
    }

    if(server.generate_indexing_report_csv == IR_ON){
      char str[600];

      FILE *ptr_file;
      ptr_file = fopen(server.indexing_report_csv_filename, "a");

      sprintf(str, "Benchmark,,%lld,%lld\n",server.memtier_benchmark_start_time, server.memtier_benchmark_end_time);
      fputs(str, ptr_file);

      fclose(ptr_file);
    }

    if(server.system_monitoring == IR_ON){
      char str[600];

      FILE *ptr_file;
      ptr_file = fopen(server.system_monitoring_csv_filename, "a");

      sprintf(str, "Benchmark;;%lld;%lld;\n",server.memtier_benchmark_start_time, server.memtier_benchmark_end_time);
      fputs(str, ptr_file);

      fclose(ptr_file);
    }
}

//Flushes to a CSV file the start and end times of the benchmarking.
void printCheckpointTimeToCSV(int id_checkpoint, long long checkpoint_start_time, long long checkpoint_end_time){
    if(server.generate_executed_commands_csv == IR_ON){
      char str[600];

      FILE *ptr_file;
      ptr_file = fopen(server.executed_commands_csv_filename, "a");

      sprintf(str, "Checkpoint,%d,%lld,%lld,,0\n", id_checkpoint, checkpoint_start_time, checkpoint_end_time);
      fputs(str, ptr_file);

      fclose(ptr_file);
    }

    if(server.generate_indexing_report_csv == IR_ON){
      char str[600];

      FILE *ptr_file;
      ptr_file = fopen(server.indexing_report_csv_filename, "a");

      sprintf(str, "Checkpoint,%d,%lld,%lld\n", id_checkpoint, server.memtier_benchmark_start_time, server.memtier_benchmark_end_time);
      fputs(str, ptr_file);

      fclose(ptr_file);
    }

    if(server.system_monitoring == IR_ON){
      char str[600];

      FILE *ptr_file;
      ptr_file = fopen(server.system_monitoring_csv_filename, "a");

      sprintf(str, "Checkpoint;%d;%lld;%lld\n", id_checkpoint, server.memtier_benchmark_start_time, server.memtier_benchmark_end_time);
      fputs(str, ptr_file);

      fclose(ptr_file);
    }
}


/*
  Flushes to a CSV file the shutdown time.
*/
void printShutdownTimeToCSV(long long int time){

  if(server.generate_executed_commands_csv == IR_ON){
    char str[200];

    FILE *ptr_file;
    ptr_file = fopen(server.executed_commands_csv_filename, "a");

    sprintf(str, "Shutdown,,%lld,,,0\n",time);
    fputs(str, ptr_file);

    fclose(ptr_file);

    //serverLog(LL_NOTICE, "Shutdown information added to file 'src/%s'. ", filename);
  }

  if(server.generate_indexing_report_csv == IR_ON){
    char str[200];

    FILE *ptr_file;
    ptr_file = fopen(server.indexing_report_csv_filename, "a");

    sprintf(str, "Shutdown,,%lld\n",time);
    fputs(str, ptr_file);

    fclose(ptr_file);

    //serverLog(LL_NOTICE, "Shutdown information added to file 'src/%s'. ", filename);
  }

  if(server.system_monitoring == IR_ON){
    char str[200];

    FILE *ptr_file;
    ptr_file = fopen(server.system_monitoring_csv_filename, "a");

    sprintf(str, "Shutdown;;%lld;\n",time);
    fputs(str, ptr_file);

    fclose(ptr_file);

    //serverLog(LL_NOTICE, "Shutdown information added to file 'src/%s'. ", filename);
  }
}

/*
    Prints a report containing recovery informations to a txt file.
*/
void printRecoveryReportToFile(){
    serverLog(LL_NOTICE, "Generating recovery report ...");
    
    char str[250];

    FILE *ptr_file;
    if(server.overwrite_report_files == IR_ON)
      ptr_file = fopen(server.recovery_report_filename, "w");
    else
      ptr_file = fopen(server.recovery_report_filename, "a");

    fputs("DATABASE RECOVERY REPORT\n\n", ptr_file);

    //Print to a file some properties of the recovery from defaul recovery
    if(server.instant_recovery_state != IR_ON){
        fputs("Database restated using sequential log (Default recovery):\n", ptr_file);

        fputs("    Sequential log filename = ", ptr_file); 
        fputs(server.aof_filename, ptr_file);
        fputs("\n    Indexed log filename = ", ptr_file);
        fputs(server.indexedlog_filename, ptr_file);

        float recovery_start_time = (float) (server.recovery_start_time - server.database_startup_time)/1000000;
        fputs("\n    Recovey start time = ", ptr_file);
        sprintf(str, "%f", recovery_start_time);
        fputs(str, ptr_file);
        fputs(" seconds.\n", ptr_file);

        float recovery_end_time = (float) (server.recovery_end_time - server.database_startup_time)/1000000;
        fputs("    Recovey end time = ", ptr_file);
        sprintf(str, "%f", recovery_end_time);
        fputs(str, ptr_file);
        fputs(" seconds.\n", ptr_file);

        fputs("    Recovey time = ", ptr_file);
        sprintf(str, "%f", recovery_end_time - recovery_start_time) ;
        fputs(str, ptr_file);
        fputs(" seconds.\n", ptr_file);

        fputs("    Log records processed = ", ptr_file);
        sprintf(str, "%llu",server.count_tuples_loaded_incr);
        fputs(str, ptr_file);

        //Print to a file some properties of the recovery from indexed recovery
    }else{
        fputs("Database restated using indexed log (Instant recovery):\n", ptr_file);

        fputs("    Sequential log filename = ", ptr_file);
        fputs(server.aof_filename, ptr_file);
        fputs("\n    Data structure of the indexed log = ", ptr_file);
        if(strcmp(server.indexedlog_structure, "BTREE") == 0)
            fputs("B+-tree\n", ptr_file);
        else
            if(strcmp(server.indexedlog_structure, "HASE") == 0)
                fputs("Hash\n", ptr_file);
        fputs("\n    Indexed log filename = ", ptr_file);
        fputs(server.indexedlog_filename, ptr_file);
        if(server.instant_recovery_synchronous == IR_OFF)
          fputs("\n    Asynchronous indexing\n", ptr_file);
        else
          fputs("\n    Synchronous indexing\n", ptr_file);

        float initial_indexing_start_time = (float) (server.initial_indexing_start_time - server.database_startup_time)/1000000;
        fputs("\n    Initial indexing start time = ", ptr_file);
        sprintf(str, "%f", initial_indexing_start_time);
        fputs(str, ptr_file);
        fputs(" seconds.\n", ptr_file);

        float initial_indexing_end_time = (float) (server.initial_indexing_end_time - server.database_startup_time)/1000000;
        fputs("    Initial indexing end time = ", ptr_file);
        sprintf(str, "%f", initial_indexing_end_time);
        fputs(str, ptr_file);
        fputs(" seconds.\n", ptr_file);

        fputs("    Initial indexing time = ", ptr_file);
        sprintf(str, "%f", initial_indexing_end_time - initial_indexing_start_time);
        fputs(str, ptr_file);
        fputs(" seconds.\n", ptr_file);

        fputs("\n    Initial number of log records indexed = ", ptr_file);
        sprintf(str, "%lli", server.initial_indexed_records);
        fputs(str, ptr_file);
        fputs(".\n", ptr_file);

        fputs("\n    Initial number of log records processed = ", ptr_file);
        sprintf(str, "%lli", server.count_initial_records_proc);
        fputs(str, ptr_file);
        fputs(".\n", ptr_file);

        float recovery_start_time = (float) (server.recovery_start_time - server.database_startup_time)/1000000;
        fputs("    Recovey start time = ", ptr_file);
        sprintf(str, "%f", recovery_start_time);
        fputs(str, ptr_file);
        fputs(" seconds.\n", ptr_file);

        float recovery_end_time = (float) (server.recovery_end_time - server.database_startup_time)/1000000;
        fputs("    Recovey finish time = ", ptr_file);
        sprintf(str, "%f", recovery_end_time);
        fputs(str, ptr_file);
        fputs(" seconds.\n", ptr_file);

        fputs("    Recovey time = ", ptr_file);
        sprintf(str, "%f", recovery_end_time - recovery_start_time);
        fputs(str, ptr_file);
        fputs(" seconds.\n", ptr_file);
/*
        int ret;
        DB *dbp = openIndexedLog(server.indexedlog_filename, 'R', &ret);
        if(ret == 0){
            fputs("    Records stored in indexed log = ", ptr_file);
            sprintf(str, "%llu",countRecordsIndexedLog(dbp));
            fputs(str, ptr_file);
            fputs("\n", ptr_file);
            fputs("    Tuples stored in indexed log = ", ptr_file);
            sprintf(str, "%llu",countTuplesIndexedLog(dbp));
            fputs(str, ptr_file);
            fputs("\n", ptr_file);
            closeIndexedLog(dbp);
        }
*/
        fputs("    Tuples loaded into memory incrementally = ", ptr_file);
        sprintf(str, "%llu",server.count_tuples_loaded_incr);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);

        fputs("    Tuples loaded into memory on demand = ", ptr_file);
        sprintf(str, "%llu",server.count_tuples_loaded_ondemand);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);

        fputs("    Total of tuples loaded into memory = ", ptr_file);
        sprintf(str, "%llu",server.count_tuples_loaded_incr + server.count_tuples_loaded_ondemand);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);

        fputs("    Number of inconsistent load attempts during incremental recovery  = ", ptr_file);
        sprintf(str, "%llu",server.count_inconsistent_load_incr);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);

        fputs("    Number of inconsistent load attempts during on on-demand recovery  = ", ptr_file);
        sprintf(str, "%llu",server.count_inconsistent_load_ondemand);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);

        fputs("    Total of inconsistent load attempts = ", ptr_file);
        sprintf(str, "%llu",server.count_inconsistent_load_incr + server.count_inconsistent_load_ondemand);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);

        fputs("    Tuples requested but already loaded previouslly, during recovery = ", ptr_file);
        sprintf(str, "%lli",server.count_tuples_already_loaded);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);

        fputs("    Tuples requested but not found in the log, during recovery = ", ptr_file);
        sprintf(str, "%lli",server.count_tuples_not_in_log);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);
    }

    if(server.memtier_benchmark_state == IR_ON){
      fputs("\nMemteir Bechmark:\n", ptr_file);
      fputs("    Parameters = ", ptr_file);
      fputs(server.memtier_benchmark_parameters, ptr_file);
      fputs("\n", ptr_file);

      float memtier_benchmark_start_time = (float) (server.memtier_benchmark_start_time - server.database_startup_time)/1000000;
      fputs("    Start time = ", ptr_file);
      sprintf(str, "%f",memtier_benchmark_start_time);
      fputs(str, ptr_file);
      fputs(" seconds\n", ptr_file);

      float memtier_benchmark_end_time = (float) (server.memtier_benchmark_end_time - server.database_startup_time)/1000000;
      fputs("    Finish time = ", ptr_file);
      sprintf(str, "%f", memtier_benchmark_end_time);
      fputs(str, ptr_file);
      fputs(" seconds\n", ptr_file);

      fputs("    Execution time = ", ptr_file);
      sprintf(str, "%f", memtier_benchmark_end_time - memtier_benchmark_start_time);
      fputs(str, ptr_file);
      fputs(" seconds\n", ptr_file);
    }

    fputs("\n\n\n\n", ptr_file);
   
    fclose(ptr_file);

    serverLog(LL_NOTICE, "The recovery report was generated!"
                         " See the file 'src/%s' on Redis instalation path.",
                         server.recovery_report_filename);
}

// ==================================================================================
/* 
    Hash in-memory of functions to store the tuples restored during the recovery. 
    This hash stores the key of the tuple restored. It is necessary to mark keys of 
    tuples restored to avoid to restore a tuple again since the incremental and on-demand 
    recoveries are executed in parallel. Besides, requests to tuples already restored are 
    marked on the hash in-memory instead of on indexed log (on disk) for performance reasons. 
    Requests to tuples that are not in the log also are marked as restored to avoid to access 
    the indexed log again in a future request.
*/
    
/*
    Stores the key of a tuple restored in a hash in-memory 
    id: key of the tuple restored
*/
struct restored_tuple {
    sds id;           
    UT_hash_handle hh; 
} *hash_restored_tuples = NULL;

/*
    Adds the key of the restored tuple.
*/
void addRestoredTuple(char *key) {
    struct restored_tuple *rr;
    rr = zmalloc(sizeof(struct restored_tuple));
    rr->id = sdsnew(key);
    HASH_ADD_STR( hash_restored_tuples, id, rr);
    //printf("(key %s added)", key);
}

/*
    Returns true if a tuple have alread been restored, i.e, if the key tuple is in the hash. 
    On the other hand, returns false.
*/
int isRestoredTuple(char *key) {
    struct restored_tuple *s = NULL;

    HASH_FIND_STR( hash_restored_tuples, key, s );
    if(s == NULL)
        return 0;
    else
        return 1;
}

/*
    Removes all key tuples of the hash.
*/
void clearHashRestoredTuples() {
  struct restored_tuple *current_key, *tmp;

  HASH_ITER(hh, hash_restored_tuples, current_key, tmp) {
    HASH_DEL(hash_restored_tuples, current_key);  /* delete; users advances to next */
    zfree(current_key->id);
    zfree(current_key);            /* optional- if you want to free  */
  }
}

/*
    Prints the keys of the restored tuples on the device.
*/
void printRestoredTuples() {
    struct restored_tuple *s;

    printf("Keys of restored tuples = ");
    for(s=hash_restored_tuples; s != NULL; s=s->hh.next) {
        printf("%s, ", s->id);
    }
    printf("\n");
}
/*
    Return the number of keys on the hash.
*/
unsigned long long countRestoredRecords(){
    return HASH_COUNT(hash_restored_tuples);
}


// ==================================================================================
// Instant recovery functions

/*
    Return a pointer to a Redis client connection.
    result: result of a connection. Return zero if the connectio fail.
*/
redisContext *openRedisClient(int *result){
    redisContext *c;
    const char *hostname = server.redisHostname;
    int port = server.redisPort;
    *result = 0;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout(hostname, port, timeout);
    if (c == NULL || c->err) {
        if (c) {
            serverLog(LL_NOTICE, "Redis client connection error: %s!", c->errstr);
            redisFree(c);
        } else {
            serverLog(LL_NOTICE, "Redis client connection error: can't allocate redis context!");
        }
        *result = 1;
    }
    return c;
}

/* 
    Loads ON DEMAND one database record (key/value) into memory by replaying its log records
    from the indexed. 
    Returns true if the searched key was restored into memory.
    key_searched: the key of the database record.
*/
int loadRecordFromIndexedLog(char *key_searched) {
    //long long command_load_start = ustime();

    int error;
    DB *dbp = openIndexedLog(server.indexedlog_filename, 'W', &error);
    if(error != 0){
      serverLog(LL_NOTICE, "⚠ ⚠ ⚠ ⚠ Error on loading data on-demand! Error on indexed log connecting! ⚠ ⚠ ⚠ ⚠ ");
      return 0;
    }

    DBT key, data, key_searched_dbt;
    /* Zero out the DBTs before using them. */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    memset(&key_searched_dbt, 0, sizeof(DBT));

    key_searched_dbt.data = key_searched;
    key_searched_dbt.size = strlen(key_searched) + 1;

    DBC *cursorp;
    /* Get a cursor */
    dbp->cursor(dbp, NULL, &cursorp, 0);

    // Position the cursor to the first record in the database whose key and data begin with the key searched.
    error = cursorp->get(cursorp, &key_searched_dbt, &data, DB_SET);

    //If the key searched is not found in the indexed log, returns false.
    if(error == DB_NOTFOUND){
      //Adds the key in the hash of restored keys to avoid a next search on the indexed log
      addRestoredTuple(key_searched);
      server.count_tuples_not_in_log = server.count_tuples_not_in_log + 1;
      closeIndexedLog(dbp);
      return 0;
    }

    char **array_log_record_lines;
    int countArray;
    unsigned long long count_records = 0;
    sds commandIR = sdsnew(""), valueIR = sdsnew("0"), dataSds;

    /* Scans the Indexed Log and generates a new log record equivalent to the all log records to redo the key searched. */
    while(error != DB_NOTFOUND) {
        count_records++;

        dataSds = sdsnew((char *)data.data);
        //Watch out!!!! sdssplit() can be a source of memory overload. Always free the memory allocated through sdsfreesplitres()
        array_log_record_lines = sdssplitlen(dataSds, sdslen(dataSds), "\n", 1, &countArray);
        sdsfree(dataSds);

        sdsfree(commandIR);
        commandIR = sdsnew((char *) array_log_record_lines[2]);
        sdstoupper(commandIR);
        //printf("record processed -> key %s command %s\n", key_searched, commandIR);
        if(strcmp(commandIR, "SET") == 0){
            sdsfree(valueIR);
            valueIR = sdsnew((char *) array_log_record_lines[6]);
        }else{
            if(strcmp(commandIR, "INCR") == 0){
                sprintf(valueIR, "%d",atoi(valueIR) + 1);
            }
        } 

        sdsfreesplitres(array_log_record_lines, countArray);
        error = cursorp->get(cursorp, &key_searched_dbt, &data, DB_NEXT_DUP);
    }

    // Builds an array of strings contains the log record generated to redo the tuple (key_searched).
    array_log_record_lines = zmalloc(sizeof(char*)*7);
    array_log_record_lines[0] = zmalloc(sizeof(char)*10);
    strcpy(array_log_record_lines[0], "*3");
    array_log_record_lines[1] = zmalloc(sizeof(char)*10);
    strcpy(array_log_record_lines[1], "$5");
    array_log_record_lines[2] = zmalloc(sizeof(char)*10);
    strcpy(array_log_record_lines[2], "SETIR");
    char strAux[120];
    sprintf(strAux, "$%ld",sdslen(key_searched));
    array_log_record_lines[3] = zmalloc(sizeof(char)*110);
    strcpy(array_log_record_lines[3], strAux);
    array_log_record_lines[4] = zmalloc(sizeof(char)*(sdslen(key_searched)+5));
    strcpy(array_log_record_lines[4], key_searched);
    sprintf(strAux, "$%ld", sdslen(valueIR));
    array_log_record_lines[5] = zmalloc(sizeof(char)*110);
    strcpy(array_log_record_lines[5], strAux);
    array_log_record_lines[6] = zmalloc(sizeof(char)*(sdslen(valueIR)+5));
    strcpy(array_log_record_lines[6], valueIR);

    int argc, j, j_aux;
    unsigned long len;
    robj **argv;
    char buf[128];
    sds argsds;
    struct redisCommand *cmd;
    struct client *fakeClient;
    fakeClient = createFakeClient();

    strcpy(buf, array_log_record_lines[0]);

    if (buf[0] != '*'){goto fmterr;}
    if (buf[1] == '\0') goto readerr;
    argc = atoi(buf+1);
    if (argc < 1){goto fmterr;}

    argv = zmalloc(sizeof(robj*)*argc);
    fakeClient->argc = argc;
    fakeClient->argv = argv;

    for (j = 0, j_aux = 1; j < argc; j++) {
        strcpy(buf, array_log_record_lines[j_aux]);
        if (buf[0] != '$'){goto fmterr;}
        len = strtol(buf+1,NULL,10);
        argsds = sdsnewlen(SDS_NOINIT,len);
        argsds = sdscpy(argsds, array_log_record_lines[j_aux+1]);
        argv[j] = createObject(OBJ_STRING,argsds);
        j_aux+=2;
    }

    for (j = 0; j <= 6; j++)
        zfree(array_log_record_lines[j]);
    zfree(array_log_record_lines);


    /* Command lookup */
    cmd = lookupCommand(argv[0]->ptr);
    if (!cmd) {
        serverLog(LL_WARNING,
            "Unknown command '%s' reading the append only file",
            (char*)argv[0]->ptr);
        exit(1);
    }

    /* Run the command in the context of a fake client */
    fakeClient->cmd = cmd;
    if (fakeClient->flags & CLIENT_MULTI &&
        fakeClient->cmd->proc != execCommand)
    {
        queueMultiCommand(fakeClient);
    } else {
        cmd->proc(fakeClient);
    }

    /* The fake client should not have a reply */
    serverAssert(fakeClient->bufpos == 0 &&
                 listLength(fakeClient->reply) == 0);

    /* The fake client should never get blocked */
    serverAssert((fakeClient->flags & CLIENT_BLOCKED) == 0);

    /* Clean up. Command code may have changed argv/argc so we use the
     * argv/argc of the client instead of the local variables. */
    freeFakeClientArgv(fakeClient);
    fakeClient->cmd = NULL;

    server.count_tuples_loaded_ondemand = server.count_tuples_loaded_ondemand + 1;
    //Adds the key in the hash of restored keys to avoid a next search on indexed log
    addRestoredTuple(key_searched);
    /*if(server.generate_setir_executed_commands_csv == IR_ON) 
        addCommandExecuted(&last_cmd_executed_List, key_searched, "setIR", command_load_start, ustime(), 'D');*/

    freeFakeClient(fakeClient);
    closeIndexedLog(dbp);

    return 1;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    serverLog(LL_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
    exit(1);
fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,"Bad file format reading the sequential log file. Last log record read: %s", buf);
    exit(1);
}


//Display information about the database recovery in time intevals
void displayRestorerInformation(long long *restoring_start_time, unsigned long long records_processed, char tag1[50], char tag2[50]){
  if(server.display_restorer_information == IR_ON){
    if(server.restorer_information_time_interaval <= (ustime()-*restoring_start_time)/1000000){
      *restoring_start_time = ustime();
      float time = (float)(*restoring_start_time-server.recovery_start_time)/1000000;
      char time_label[20] = "";
      if(time < 60){
        strcpy(time_label, "seconds");
      }else{
        if(time >= 3600){
          time = time/3600;
          strcpy(time_label, "hours");
        }else{
          time = time/60;
          strcpy(time_label, "minutes");
        }
      }
      serverLog(LL_NOTICE, "Number of tuples loaded into memory: %llu (inclementally = %llu, on-demand = %llu). "
                              "Number of records processed: %llu. "
                              "Time spent recovering the database: %.3f %s. (%s) (%s)",
                              server.count_tuples_loaded_incr + server.count_tuples_loaded_ondemand,
                              server.count_tuples_loaded_incr, server.count_tuples_loaded_ondemand,
                              records_processed, time, time_label, tag1, tag2);   
    }
  }
}

/* 
   Loads INCREMENTALLY all database tuple from indexel log into memory, except thouse
   loaded previously on demand.
   It requires the extra-flag DB_DUP in openIndexedLog() function.
   Return a unsigned long long int with the number of records loaded
*/
void *loadDBFromIndexedLog () {
    server.recovery_start_time = ustime();
    server.instant_recovery_performing = IR_ON;

    DB *dbp;
    int error;
      
    dbp = openIndexedLog(server.indexedlog_filename, 'W', &error);
    if(error != 0){
        serverLog(LL_NOTICE, "⚠ ⚠ ⚠ ⚠ Database loading failed! Error when openning the Indexed Log. ⚠ ⚠ ⚠ ⚠ ");
        exit(0);
    }

    redisContext *redisConnection = openRedisClient(&error);
    if(error != 0){
        serverLog(LL_NOTICE, "⚠ ⚠ ⚠ ⚠ Database loading failed!! Error connecting to redis server. ⚠ ⚠ ⚠ ⚠ ");
        exit(0);
    }
    redisReply *reply;

    if(server.instant_recovery_synchronous == IR_OFF){
      if(strcmp(server.starts_log_indexing, "B") == 0){
        pthread_create(&server.indexer_thread, NULL, indexesSequentialLogToIndexedLogV2, NULL);
        pthread_create(&server.checkpoint_thread, NULL, executeCheckpoint, NULL);
      }
    }

    serverLog(LL_NOTICE, "Loading the database from indexed log ... ");

    DBT key, data;
    /* Zero out the DBTs before using them. */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    DBC *cursorp;
    /* Get a cursor */
    dbp->cursor(dbp, NULL, &cursorp, 0); 

    char **array_log_record_lines;
    int countArray;
    unsigned long long count_records = 0, count_tuples_loaded = 0, count_inconsistent_load = 0, 
                        count_records_tuple = 0;

    /* Iterate over the indexed log, retrieving each record in berkeley db and reloading the data on redis. */
    error = cursorp->get(cursorp, &key, &data, DB_NEXT);
    sds current_key = sdsnew(""), old_key = sdsnew(""), commandIR = sdsnew(""), keyIR = sdsnew(""), valueIR = sdsnew("0"), dataSds;
    sdsfree(current_key);
    current_key = sdsnew((char *)key.data);
    long long restoring_start_time = ustime();

    while (error != DB_NOTFOUND && server.instant_recovery_performing_stop == IR_OFF) {
        sdsfree(valueIR);
        valueIR = sdsnew("0");
        count_records_tuple = 0;

        //If a key (current_key) has alread been restored, shifs until to find a key not loaded.
        while(isRestoredTuple(current_key)){
          count_records++;
          error = cursorp->get(cursorp, &key, &data, DB_NEXT_NODUP);//DB_NEXT_NODUP gets the next non-duplicate record in the database. 
          if(error != DB_NOTFOUND){
              sdsfree(current_key);
              current_key = sdsnew((char *)key.data);
          }else{
            break;
          }

          displayRestorerInformation(&restoring_start_time, count_records,"1", "" );
        }

        if(error == DB_NOTFOUND)
            break;

        //long long command_load_start = usold_keytime();
        sdsfree(old_key);
        old_key = sdsnew(current_key);

        //Reads all the log records to restore a key and generats only one command to restore the key.
        while(1) {
            displayRestorerInformation(&restoring_start_time, count_records, current_key, old_key);

            //Gets out the loop when the key changes, i.e., all the log records to restore the key were read.
            if(sdscmp(current_key, old_key) != 0){
                sdsfree(old_key);
                old_key = sdsnew(current_key);
                break;
            }
            count_records++;
            count_records_tuple++;

            dataSds = sdsnew((char *)data.data);
            //array_log_record_lines = str_split(dataSds, '\n');
            //Watch out!!!! sdssplit() can be a source of memory overload. Always free the memory allocated through sdsfreesplitres()
            array_log_record_lines = sdssplitlen(dataSds, sdslen(dataSds), "\n", 1, &countArray);
            sdsfree(dataSds);

            sdsfree(commandIR);
            commandIR = sdsnew((char *) array_log_record_lines[2]);
            sdstoupper(commandIR);
            if(strcmp(commandIR, "SET") == 0){
                sdsfree(keyIR);
                keyIR = sdsnew(current_key);
                sdsfree(valueIR);
                valueIR = sdsnew((char *) array_log_record_lines[6]);
            }else{
                if(strcmp(commandIR, "INCR") == 0){
                    sdscpy(commandIR, "SET");
                    sdsfree(keyIR);
                    keyIR = sdsnew(current_key);
                    sprintf(valueIR, "%d",atoi(valueIR) + 1);
                }
            }

            //It is very important to free to avoid memory overhaed
            sdsfreesplitres(array_log_record_lines, countArray);
            
            error = cursorp->get(cursorp, &key, &data, DB_NEXT);
            if(error == DB_NOTFOUND)
                break;
            sdsfree(current_key);
            current_key = sdsnew((char *)key.data); 
        }

        sdstoupper(commandIR);
        if(strcmp(commandIR, "SET") == 0){
            //Executes the command SetIR that stores a key/value into memory with logging.
            reply = redisCommand(redisConnection,"setIR %s %s", keyIR, valueIR);
            addRestoredTuple(keyIR);
            if(reply->str != NULL){
              count_tuples_loaded++;
              server.count_tuples_loaded_incr = count_tuples_loaded;
            }
            else{
              count_inconsistent_load++;
            }
            
            count_records_tuple = 0;

            //Logs a information about the command executed to restore the database.
            /*if(server.generate_setir_executed_commands_csv == IR_ON) 
                addCommandExecuted(&last_cmd_executed_List, keyIR, "setIR", command_load_start, ustime(), 'I');*/
        }

        displayRestorerInformation(&restoring_start_time, count_records, "3", "");
    }

  server.count_tuples_loaded_incr = count_tuples_loaded;
  server.count_inconsistent_load_incr = count_inconsistent_load;

  sdsfree(current_key);
  sdsfree(old_key);
  sdsfree(keyIR);
  sdsfree(valueIR);
  sdsfree(commandIR);
  redisFree(redisConnection);
  if (cursorp != NULL)
    cursorp->close(cursorp);
  closeIndexedLog(dbp);
  clearHashRestoredTuples();

  server.recovery_end_time = ustime();

  serverLog(LL_NOTICE, "DB loaded from Indexed Log: %.3f seconds. Number of tuples loaded into memory: %llu "
                       "(inclementally = %llu, on-demand = %llu). "
                       "Number of records processed: %llu. Inconsistenes: %llu :)",
                        (float)(server.recovery_end_time-server.recovery_start_time)/1000000, 
                        server.count_tuples_loaded_incr + server.count_tuples_loaded_ondemand,
                        server.count_tuples_loaded_incr, server.count_tuples_loaded_ondemand,
                        count_records, server.count_inconsistent_load_incr);

  printRecoveryTimeToCSV();

  //Starts the Indexer
  if(server.instant_recovery_synchronous == IR_OFF){
    if(strcmp(server.starts_log_indexing, "A") == 0){
      pthread_create(&server.indexer_thread, NULL, indexesSequentialLogToIndexedLogV2, NULL);
      pthread_create(&server.checkpoint_thread, NULL, executeCheckpoint, NULL);
    }
  }

  server.instant_recovery_performing = IR_OFF;

  return (void *)count_records; 
}

void stop_loadDBFromIndexedLog(){
  server.instant_recovery_performing_stop = IR_ON;
}

/*
  Waits until the Indexer thread to finish
*/
void waitLoadDBFromIndexedLogFinish(){
  while(server.instant_recovery_performing == IR_ON){
    sleep(0.05);
  }
}

/*
    Reads the pointer to the last record read in the sequential log file before 
    starting the log indexing, log replica indexing, or indexed log rebuilding starting
    the last full checkpoing.
    Returns the number of the line to start the indexing. 
    If the binary file does not exist, returns -1.
    filename: can be one of the contants FINAL_LOG_SEEK, FINAL_LOG_SEEK_REPLICA, and 
              CHECKPOINT_LOG_SEEK defined in server.h file.
*/
long long int readFinalLogSeek(char *filename){
  FILE *binaryFile = fopen(filename, "rb"); 

    if (binaryFile == NULL){
        return -1;
    }

    unsigned long long int seek = 0;

    int result = fread( &seek, sizeof(unsigned long long int), 1, binaryFile );
    fclose(binaryFile);
    if(result == 0)
      seek =0;
    return seek;
}

/*
    Writes a pointer to the last record writed in the sequential log file of indexed log, 
    indexed log replica, or full checkpoint.
    If the binary file does not exist, returns -1. Otherwise, the written was done!
    filename: can be one of the contants FINAL_LOG_SEEK, FINAL_LOG_SEEK_REPLICA, and 
              CHECKPOINT_LOG_SEEK defined in server.h file.
    seek: a log record postion 
*/

int writeFinalLogSeek(char *filename, unsigned long long seek){
  FILE *binaryFile = fopen(filename, "wb");

    if (binaryFile == NULL){
      printf("Fail to open %s!\n", filename);
      return -1;
    }
    
  int result = fwrite (&seek, sizeof(unsigned long long int), 1, binaryFile);
  fclose(binaryFile);

  return result;
}

/*
    Finds pointer to  next log record to start the indexing.
    The pointer is written in file filename.
*/
void generateFinalLogSeek(char *filename){
  FILE *logFile = fopen(filename, "r"); 

    if (logFile == NULL){
        printf("Fail to open log file!\n");
    }else{
      fseek(logFile, 0, SEEK_END);
      writeFinalLogSeek(FINAL_LOG_SEEK, ftell(logFile));
  }
}

/*
  Creates a set log record.

  ***** ATENTION ******
  This function is a source of memory overhead. It creates many data into memory by malloc
  function and does not remove the data.
  So, we hadn't used this function.

  THIS FUNCTION SHOULD BE REMOVED IN THE FUTURE
*/
sds createSetLogRecord(sds key, sds value){
    char aux[200];

    sprintf(aux, "*3\n$3\nSET\n$%ld\n%s\n$%ld\n%s", sdslen(key), key, sdslen(value), value);

    sds record = sdsnew(aux);

    return record;
}

/*
  Creates a INCR log record.

  ***** ATENTION ******
  This function is a source of memory overhead. It creates many data into memory by malloc
  function and does not remove the data.
  So, we hadn't used this function.

  THIS FUNCTION SHOULD BE REMOVED IN THE FUTURE
*/
sds createIncrLogRecord(sds key){
    
    char aux[200];

    sprintf(aux, "*2\n$4\nINCR\n$%ld\n%s", sdslen(key), key);

    sds record = sdsnew(aux);

    return record;
}

//Display information about the log indexing in time intevals
void displayIndexerInformation(long long *indexing_start_time, unsigned long long int *count_records_aux, 
                                unsigned long long int *count_records_indexed_aux){
  if(server.display_indexer_information == IR_ON){
    if(server.indexer_information_time_interaval <= (ustime()-*indexing_start_time)/1000000){
      serverLog(LL_WARNING,"Indexer processed %llu log records and indexed %llu log records, since the last %llu seconds.", 
          *count_records_aux, *count_records_indexed_aux, server.indexer_information_time_interaval);
      *count_records_aux = 0;
      *count_records_indexed_aux = 0;
      *indexing_start_time = ustime();
    }
  }
}

/* 
    Copies the records from the sequential log file to the indexed log.
    It works with a B-tree or Hash and requires the extra-flag DB_DUP (allows duplicate keys) in openIndexedLog() function.
    This version stores the records to indexed log directly from sequential log.
    Returns the number of processed log records (unsigned long long int).

    ***** ATENTION ******
    This function is a source of memory overhead. It uses the createIncrLogRecord() and 
    createIncrLogRecordcreates() createIncrLogRecord that creates many data into memory by 
    malloc function and does not remove the data.
    So, we hadn't used this function.

    THIS FUNCTION SHOULD BE REMOVED IN THE FUTURE
*/
void *indexesSequentialLogToIndexedLogV1() {
    if(server.indexer_state == IR_ON)
      return (void *)0;

    server.indexer_state = IR_ON;
    server.indexer_performing = IR_ON;
    serverLog(LL_NOTICE,"Indexer thread V1 started!");

    unsigned long long seek_log_file = readFinalLogSeek(FINAL_LOG_SEEK);
    char *aof_filename = server.aof_filename;
    char *indexedlog_filename = server.indexedlog_filename;

    FILE *fp = fopen(aof_filename,"r");
    fseek(fp, seek_log_file, SEEK_SET);
    //struct redis_stat sb;
    int ret;
    unsigned long long int count_records = 0, //Counts the number of records processed from sequential log
                           count_records_indexed = 0, //Counts the number of records indexed from sequential log
                           count_records_aux = 0, count_records_indexed_aux = 0;

    if (fp == NULL) {
        serverLog(LL_WARNING,"Fatal error: can't open the append log file for reading: %s",strerror(errno));
        exit(1);
    }

    DB *dbp = openIndexedLog(indexedlog_filename, 'W', &ret);
    if(ret != 0){
        serverLog(LL_NOTICE,"Indexer cannot start! Cannot open the indexed log!");
        server.indexer_state = IR_OFF;
        server.indexer_performing = IR_OFF;
        return (void *)0;
    }

    sds log_record = sdsnew(""), key = sdsnew(""), command = sdsnew(""), value = sdsnew("");
    const sds SET_COMMAND  = sdsnew("SET"), INCR_COMMAND  = sdsnew("INCR"), DEL_COMMAND  = sdsnew("DEL"), 
              SETCHECKPOINT_COMMAND  = sdsnew("SETCHECKPOINT"), CHECKPOINTEND_COMMAND  = sdsnew("CHECKPOINTEND");
    //int last_checkpoint_indexed = 0, id_checkpoint;

    long long indexing_start_time = ustime();
    while(1) {
        int argc, j;
        unsigned long len;
        char buf[128];server.indexer_performing = IR_ON;
        sds argsds;

        if (fgets(buf,sizeof(buf),fp) == NULL) {
            fclose(fp);
            dbp->sync(dbp, 0);
            writeFinalLogSeek(FINAL_LOG_SEEK, seek_log_file);

            //Stores the time when the last checkpoint command log record is indexed
            /*if(last_checkpoint_indexed){
              long long time = ustime();
              setLastTupleCheckpointedTime(id_checkpoint, time);
              count_records_indexed++;
              count_records_indexed_aux++;
              if(server.display_checkpoint_information == IR_ON)
                serverLog(LL_WARNING,"Checkpoint %d had the last tuple checkpointed in %f seconds", 
                                      id_checkpoint, (float)(time-server.database_startup_time)/1000000); 
              last_checkpoint_indexed = 0;
            }*/

            //Sleeps until a new record is read in the sequential log.
            do{
              //Checks if the indexer recieved a stop signal and breaks the secondary loop if true
              if(server.indexer_state == IR_OFF)
                break;

              usleep(server.indexer_time_interval);

              fp = fopen(aof_filename, "r");
              fseek(fp, seek_log_file, SEEK_SET);
              if(fgets(buf,sizeof(buf),fp) == NULL)
                fclose(fp);
              else{
                break;
              }

              displayIndexerInformation(&indexing_start_time, &count_records_aux, &count_records_indexed_aux);

            }while(1);

            //Checks if the indexer recieved a stop signal and exits the main loop if true
            if(server.indexer_state == IR_OFF)
              break;
        }

        seek_log_file = seek_log_file + strlen(buf);

        log_record = sdscpy(log_record , buf); 
        
        if (buf[0] != '*'){  goto fmterr;}
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1){  goto fmterr;}
        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                goto readerr;
            }
            seek_log_file = seek_log_file + strlen(buf); 
            log_record = sdscat(log_record, buf);

            if (buf[0] != '$'){ goto fmterr;}
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(SDS_NOINIT,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                goto readerr;
            }
            seek_log_file = seek_log_file +  len;
            log_record = sdscatsds(log_record, argsds);
            if(j+1<argc)
                log_record = sdscat(log_record, "\n");

            if(j==0)//Get the command
                command = sdscpy(command, argsds);
            if(j==1)//Get the key
                key = sdscpy(key, argsds);
            if(j==2)//Get the value
                value = sdscpy(value, argsds);

            if (fread(buf,2,1,fp) == 0) {
                goto readerr; /* discard CRLF */
            }
            seek_log_file = seek_log_file + 2;
            sdsfree(argsds);
        }

        count_records++;
        count_records_aux++;

        sdstoupper(command);
        if(sdscmp(command, SET_COMMAND) == 0){
          //delRecordIndexdLog(dbp, key);
          addRecordIndexedLog(dbp, key, log_record);
          count_records_indexed++;
          count_records_indexed_aux++;
        }else{
          if(sdscmp(command, INCR_COMMAND) == 0){
            addRecordIndexedLog(dbp, key, log_record);
            count_records_indexed++;
            count_records_indexed_aux++;
          }else{
            if(sdscmp(command, DEL_COMMAND) == 0){
              delRecordIndexdLog(dbp, key);
              count_records_indexed++;
              count_records_indexed_aux++;
            }else{
              if(sdscmp(command, SETCHECKPOINT_COMMAND) == 0){
                delRecordIndexdLog(dbp, key);
                addRecordIndexedLog(dbp, key, createSetLogRecord(key, value));
                count_records_indexed++;
                count_records_indexed_aux++;
              }else{
                if(sdscmp(command, CHECKPOINTEND_COMMAND) == 0){
                  ;//id_checkpoint = atoi(key);
                  //last_checkpoint_indexed = 1;
                }
              }
            }
          }
        }

        //Checks if the indexer recieved a stop signal and breaks the secondary loop if true
        if(server.indexer_state == IR_OFF)
          break;

        displayIndexerInformation(&indexing_start_time, &count_records_aux, &count_records_indexed_aux);
    }
    sdsfree(key);
    sdsfree(log_record);
    sdsfree(SET_COMMAND);
    sdsfree(SETCHECKPOINT_COMMAND);
    sdsfree(INCR_COMMAND);
    sdsfree(DEL_COMMAND);
    sdsfree(command);
    closeIndexedLog(dbp);

    server.indexer_state = IR_OFF;
    server.indexer_performing = IR_OFF;

    serverLog(LL_WARNING,"Indexer thread stopped! Number of log records processed = %llu. "
      "Number of log records indexed = %llu", count_records, count_records_indexed);

    return (void *)count_records;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    server.indexer_state = IR_OFF;

    if (!feof(fp)) {
        serverLog(LL_WARNING,"Indexing error! Unrecoverable error reading the append only file wh: %s", strerror(errno));
        exit(1);
    }

fmterr: /* Format error. */
    serverLog(LL_WARNING,"Indexing error! Bad file format reading the sequential file. Last log record read: %s", log_record);
    exit(1);
}


/*
    Contains information about the log record that shoud be flushed to indexed log.
    It is used to handle a linked list.
*/
typedef struct recordToIndex_type {
  char command[20];
  char key[50];         
  char value[50];           
  struct recordToIndex_type *next;
}recordToIndex;

recordToIndex *first_recordToIndex = NULL, *last_recordToIndex = NULL;

/*
    Inserts the first record in the linked list that is not used. 
    It is added only to optmize the addRecordToIndex() function.
*/
void insertFirstRecordToIndex (recordToIndex **first_recordToIndex, recordToIndex **last_recordToIndex){
   recordToIndex *new = (recordToIndex *) zmalloc(sizeof(recordToIndex));
   
   new->next = NULL;
   
   *first_recordToIndex = new;
   *last_recordToIndex = new;
}

/*
    Inserts a record at the end of a linked list .  
*/
void addRecordToIndex (recordToIndex **last_recordToIndex, char command[20], char key[50], char value[50]){
  recordToIndex *new = (recordToIndex *) zmalloc(sizeof(recordToIndex));

  strcpy(new->command, command);
  strcpy(new->key, key);
  if(value != NULL)
    strcpy(new->value, value);
  new->next = NULL;

  (*last_recordToIndex)->next = new;
  *last_recordToIndex = new;
}

/*
    Freeup the memory from all records on the linked list of log records to be flused to
    indexed log.
*/
int clearListRecordToIndex(recordToIndex **first_recordToIndex, recordToIndex **last_recordToIndex){
    //If the fourth node is NULL the linked list is empty, since it is the frist command.
    if(*first_recordToIndex == NULL)
        return 0;

    recordToIndex *c = *first_recordToIndex, *aux;
    while(c != NULL){
        aux = c;
        c = c->next;
        zfree(aux);
    }
    *first_recordToIndex = NULL;
    *last_recordToIndex = NULL;

    return 1;
}

/*  YOU SHOULD UPDATE THIS FUNCTION IN THE FUTURE, it is nececessary

  Writes and fluhses log records to the indexed log file replica from a linked list.
    dbp: pointer to the indexed log.
    ri: linked list of log records to index.
  ***** ATENTION ******
    This function is a source of memory overhead. It uses the createIncrLogRecord() and 
    createIncrLogRecordcreates() createIncrLogRecord that creates many data into memory by 
    malloc function and does not remove the data.
    So, we hadn't used this function.

    THIS FUNCTION SHOULD BE updated IN THE FUTURE
*/
void replicateIndexedLog(DB *dbp, recordToIndex *ri, unsigned long long seek_log_file){
  const char SET_COMMAND[5]  = "SET", INCR_COMMAND[6]  = "INCR", DEL_COMMAND[5]  = "DEL", 
              SETCHECKPOINT_COMMAND[15]  = "SETCHECKPOINT", CHECKPOINTEND_COMMAND[15]  = "CHECKPOINTEND";

  while(ri != NULL){
    //Checks if the indexer recieved a stop signal and exits the loop if true
    /*if(server.indexer_state == IR_OFF){
      //Flushes de records to disk and sets position of the last record indexed in sequential log before exit.
      dbp->sync(dbp, 0);
      writeFinalLogSeek(FINAL_LOG_SEEK_REPLICA, seek_log_file);
      return IR_OFF;
    }*/

    if(strcmp(ri->command, SET_COMMAND) == 0){
      //delRecordIndexdLog(dbp, key);
      addRecordIndexedLog(dbp, ri->key, createSetLogRecord(ri->key, ri->value));
      //printf("set indexed\n");
    }else{
      if(strcmp(ri->command, INCR_COMMAND) == 0){
        addRecordIndexedLog(dbp, ri->key, createIncrLogRecord(ri->key));
      }else{
        if(strcmp(ri->command, DEL_COMMAND) == 0){
          delRecordIndexdLog(dbp, ri->key);
        }else{
          if(strcmp(ri->command, SETCHECKPOINT_COMMAND) == 0){
            delRecordIndexdLog(dbp, ri->key);
            addRecordIndexedLog(dbp, ri->key, createSetLogRecord(ri->key, ri->value));
          }else{
            if(strcmp(ri->command, CHECKPOINTEND_COMMAND) == 0){
              ;
            }
          }
        }
      }
    }

    ri = ri->next;
  }
  //Flushes de records to disk and sets position of the last record indexed in sequential log.
  dbp->sync(dbp, 0);
  writeFinalLogSeek(FINAL_LOG_SEEK_REPLICA, seek_log_file);
}

/*
  Writes and fluhses log records to the indexed log from a linked list.
    dbp: pointer to the indexed log.
    ri: linked list of log records to index.
    count_records: returns the number of log records processed.
    count_records_indexed: returns the number o log records indexed.
  The function returns IR_OFF if the funciton recived a signal to exit the indexing processing.
  Otherwise, returns IR_ON.
*/
int writeToIndexedLog(DB *dbp, recordToIndex *ri, unsigned long long seek_log_file,
 unsigned long long int *count_records, unsigned long long int *count_records_indexed){
  const char SET_COMMAND[5]  = "SET", INCR_COMMAND[6]  = "INCR", DEL_COMMAND[5]  = "DEL", 
              SETCHECKPOINT_COMMAND[15]  = "SETCHECKPOINT", CHECKPOINTEND_COMMAND[15]  = "CHECKPOINTEND";
  *count_records = 0;
  *count_records_indexed = 0;

  while(ri != NULL){
    //Checks if the indexer recieved a stop signal and exits the loop if true
    if(server.indexer_state == IR_OFF){
      //Flushes de records to disk and sets position of the last record indexed in sequential log before exit.
      dbp->sync(dbp, 0);
      writeFinalLogSeek(FINAL_LOG_SEEK, seek_log_file);
      return IR_OFF;
    }

    if(strcmp(ri->command, SET_COMMAND) == 0){
      //delRecordIndexdLog(dbp, ri->key);
      char aux[200];
      sprintf(aux, "*3\n$3\nSET\n$%ld\n%s\n$%ld\n%s", sdslen(ri->key), ri->key, sdslen(ri->value), ri->value);
      addRecordIndexedLog(dbp, ri->key, aux);///createSetLogRecord(ri->key, ri->value)); //the function createSetLogRecord() is a source of memory overhead. So, we had to creat a record manually.
      *count_records_indexed = *count_records_indexed + 1;
      //printf("set indexed\n");
    }else{
      if(strcmp(ri->command, INCR_COMMAND) == 0){
        char aux[200];
        sprintf(aux, "*3\n$3\nSET\n$%ld\n%s\n$%ld\n%s", sdslen(ri->key), ri->key, sdslen(ri->value), ri->value);
        addRecordIndexedLog(dbp, ri->key, aux);///createSetLogRecord(ri->key, ri->value));
        *count_records_indexed = *count_records_indexed + 1;
      }else{
        if(strcmp(ri->command, DEL_COMMAND) == 0){
          delRecordIndexdLog(dbp, ri->key);
          *count_records_indexed = *count_records_indexed + 1;
        }else{
          if(strcmp(ri->command, SETCHECKPOINT_COMMAND) == 0){
            delRecordIndexdLog(dbp, ri->key);
            char aux[200];
            sprintf(aux, "*3\n$3\nSET\n$%ld\n%s\n$%ld\n%s", sdslen(ri->key), ri->key, sdslen(ri->value), ri->value);
            addRecordIndexedLog(dbp, ri->key, aux);///createSetLogRecord(ri->key, ri->value));
            *count_records_indexed = *count_records_indexed + 1;
          }else{
            if(strcmp(ri->command, CHECKPOINTEND_COMMAND) == 0){
              ;//Fazer Checkpoint End aqui
            }
          }
        }
      }
    }

    *count_records = *count_records+1;
    ri = ri->next;
  }
  //Flushes de records to disk and sets position of the last record indexed in sequential log.
  dbp->sync(dbp, 0);
  writeFinalLogSeek(FINAL_LOG_SEEK, seek_log_file);
  server.seek_log_file = seek_log_file;

  return IR_ON;
}

/*
  Copies the records from the sequential log file to the indexed log.
  It works with a B-tree or Hash and requires the extra-flag DB_DUP (allows duplicate keys) 
  in openIndexedLog() function.
  This version first stores the records in a linked list and after copies the records to 
  the indexed log.
  Returns the number of processed log records (unsigned long long int).
*/
void *indexesSequentialLogToIndexedLogV2() {
    if(server.indexer_state == IR_ON)
      return (void *)0;

    server.indexer_state = IR_ON;
    server.indexer_performing = IR_ON;
    serverLog(LL_NOTICE,"Indexer thread V2 started!");

    unsigned long long seek_log_file = readFinalLogSeek(FINAL_LOG_SEEK);
    char *aof_filename = server.aof_filename;
    char *indexedlog_filename = server.indexedlog_filename;

    FILE *fp = fopen(aof_filename,"r");
    fseek(fp, seek_log_file, SEEK_SET);
    if (fp == NULL) {
        serverLog(LL_WARNING,"Fatal error: can't open the append log file for reading: %s",strerror(errno));
        exit(1);
    }
    fclose(fp);
    
    int ret;
    DB *dbp = openIndexedLog(indexedlog_filename, 'W', &ret);
    if(ret != 0){
        serverLog(LL_NOTICE,"Indexer cannot start! Cannot open the indexed log!");
        server.indexer_state = IR_OFF;
        server.indexer_performing = IR_OFF;
        return (void *)0;
    }
    
  /*  THE REPLICATION SHOULD BE IMPLEMENTED IF IT IS NECESSARY
      SO SEE THE FUNCION replicateIndexedLog()

    DB *dbp_replica = NULL;
    if(server.indexedlog_replicated == IR_ON){
      dbp_replica = openIndexedLog(server.indexedlog_replicated_filename, 'W', &ret);
      if(ret != 0){
        serverLog(LL_NOTICE,"Replication cannot start! Cannot open the indexed log filereplica!");
        server.indexedlog_replicated = IR_OFF;
      }else
        serverLog(LL_NOTICE,"Indexed log file replication started!");
    }
  */

    unsigned long long int count_records = 0, //Counts the number of records processed from sequential log
                           count_records_indexed = 0, //Counts the number of records indexed from sequential log
                           count_records_ToDiplay = 0, count_records_indexed_ToDiplay = 0;//Counts record to displayIndexerInformation() function
                           long long indexing_start_time_ToDiplay; //Time to displayIndexerInformation() function
    sds log_record = sdsnew(""), key = sdsnew(""), command = sdsnew(""), value = sdsnew("");
    recordToIndex *ri;
    insertFirstRecordToIndex (&first_recordToIndex, &last_recordToIndex);

    long long indexing_start_time;
    int argc, j;
    unsigned long len;
    char buf[128];
    sds argsds;
    indexing_start_time_ToDiplay = ustime();

    server.indexer_performing = IR_ON;
    while(1) {
      //Sleeps until a new record is read in the sequential log.
      do{
        //Checks if the indexer recieved a stop signal and breaks the secondary loop if true
        if(server.indexer_state == IR_OFF)
          break;

        usleep(server.indexer_time_interval);

        fp = fopen(aof_filename, "r");
        fseek(fp, seek_log_file, SEEK_SET);
        if(fgets(buf,sizeof(buf),fp) == NULL)
          fclose(fp);
        else{
          break;
        }

        displayIndexerInformation(&indexing_start_time_ToDiplay, &count_records_ToDiplay, &count_records_indexed_ToDiplay);
      }while(1);

      //Checks if the indexer recieved a stop signal and exits the main loop if true
      if(server.indexer_state == IR_OFF)
        break;

      indexing_start_time = ustime();
      do{
        //Checks if the indexer recieved a stop signal and exits the secondary loop if true
        if(server.indexer_state == IR_OFF)
          break;

        seek_log_file = seek_log_file + strlen(buf);

        log_record = sdscpy(log_record , buf); 
        
        if (buf[0] != '*'){  goto fmterr;}
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1){  goto fmterr;}
        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                goto readerr;
            }
            seek_log_file = seek_log_file + strlen(buf); 
            log_record = sdscat(log_record, buf);

            if (buf[0] != '$'){ goto fmterr;}
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(SDS_NOINIT,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                goto readerr;
            }
            seek_log_file = seek_log_file +  len;
            log_record = sdscatsds(log_record, argsds);
            if(j+1<argc)
                log_record = sdscat(log_record, "\n");

            if(j==0){//Get the command
                command = sdscpy(command, argsds);
                sdstoupper(command);
            }
            if(j==1)//Get the key
                key = sdscpy(key, argsds);
            if(j==2)//Get the value
                value = sdscpy(value, argsds);

            if (fread(buf,2,1,fp) == 0) {
                goto readerr; /* discard CRLF */
            }
            seek_log_file = seek_log_file + 2;
            sdsfree(argsds);
        }
        
        addRecordToIndex(&last_recordToIndex, command, key, value);
        //printf("processed! c=%s\n", command);
      }while(fgets(buf,sizeof(buf),fp) != NULL);
      fclose(fp);

      //Checks if the indexer recieved a stop signal and exits the main loop if true
      if(server.indexer_state == IR_OFF)
        break;

      ri = first_recordToIndex->next;
      if(ri != NULL){
        unsigned long long int count_recs, count_recs_indexed;
        
        int signal = writeToIndexedLog(dbp, ri, seek_log_file, &count_recs, &count_recs_indexed);
        
        //Stores information to generate indexing report
        if(server.generate_indexing_report_csv == IR_ON)
          addIndexingReport(&last_indexing_report, indexing_start_time, ustime(), 
                              count_recs, count_recs_indexed);
      /*  THE REPLICATION SHOULD BE IMPLEMENTED IF IT IS NECESSARY
        if(server.indexedlog_replicated == IR_ON)
          replicateIndexedLog(dbp_replica, ri, seek_log_file);
      */
        
        count_records = count_records + count_recs;
        count_records_indexed = count_records_indexed +count_recs_indexed;
        count_records_ToDiplay = count_records_ToDiplay + count_recs;
        count_records_indexed_ToDiplay = count_records_indexed_ToDiplay + count_recs_indexed;

        //Checks if the indexer recieved a stop signal and exits the main loop if true
        if(signal == IR_OFF)
          break;

        clearListRecordToIndex(&first_recordToIndex, &last_recordToIndex);
        insertFirstRecordToIndex (&first_recordToIndex, &last_recordToIndex);
      }

      displayIndexerInformation(&indexing_start_time_ToDiplay, &count_records_ToDiplay, &count_records_indexed_ToDiplay);
    }

    sdsfree(key);
    sdsfree(log_record);
    sdsfree(command);
    closeIndexedLog(dbp);
    /*  THE REPLICATION SHOULD BE IMPLEMENTED IF IT IS NECESSARY
    if(server.indexedlog_replicated == IR_ON)
      closeIndexedLog(dbp_replica);
    */
    clearListRecordToIndex(&first_recordToIndex, &last_recordToIndex);

    server.indexer_state = IR_OFF;
    server.indexer_performing = IR_OFF;

    serverLog(LL_WARNING,"Indexer thread stopped! Number of log records processed = %llu. "
      "Number of log records indexed = %llu", count_records, count_records_indexed);

    return (void *)count_records;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    server.indexer_state = IR_OFF;

    if (!feof(fp)) {
        serverLog(LL_WARNING,"Indexing error! Unrecoverable error reading the append only file wh: %s", strerror(errno));
        exit(1);
    }

fmterr: /* Format error. */
    serverLog(LL_WARNING," aqui Indexing error! Bad file format reading the sequential file. Last log record read: %s", log_record);
    exit(1);
}

/*
    Sends a signal to stop the indexer proforming.
*/
void stopIndexing(){
  server.indexer_state = IR_OFF;
}

/*
  Waits until the Indexer thread to finish
*/
void waitIndexerFinish(){
  while(server.indexer_performing == IR_ON){
    sleep(0.05);
  }
}

/* 
    Applyed only on restart.
    Copies the remain records from the sequential log file to the indexed log on database restart.
    When the database crashes, log record could not be indexed because the de indexing is asynchronous.
    This version allows duplicate keys, i.e. a key can be more than a log record on indexed log.
    It works with a B-tree or Hase and requires the extra-flag DB_DUP (allows duplicate keys) 
    in openIndexedLog() function.
*/
unsigned long long initialIndexesSequentialLogToIndexedLog() {
    server.initial_indexing_start_time = ustime();

    int errorLog, errorSync = 0;
    //Reads the position of the last log record indexed on sequential log.
    long long int seek_log_file = readFinalLogSeek(FINAL_LOG_SEEK);
    if(seek_log_file == -1){
        serverLog(LL_NOTICE, "Fail to open 'finalLogSeek.dat' file!");
        errorSync = 1;
        seek_log_file = 0;
    }
    
    //Open/create the indexed log to start the indexing.
    DB *dbp = openIndexedLog(server.indexedlog_filename, 'R', &errorLog);
    closeIndexedLog(dbp);
    if(errorSync != 0)
      serverLog(LL_NOTICE,"Cannot open the indexed log1!");

    if(errorLog != 0 || errorSync != 0){
      //Tries to use the indexed log file replica    
      if(server.indexedlog_replicated == IR_ON){
        serverLog(LL_NOTICE,"The system will try to use the indexed log file replica!");
        remove(server.indexedlog_filename);
        if(rename(server.indexedlog_replicated_filename, server.indexedlog_filename) == 0){
            seek_log_file = readFinalLogSeek(FINAL_LOG_SEEK_REPLICA);
            server.indexedlog_replicated = IR_OFF;
            errorLog = 1;
            serverLog(LL_NOTICE,"Indexed log file replica found!");
          }else{
            serverLog(LL_NOTICE,"Cannot open the indexed log file replica!");
            errorLog = 0;
          }
      }

      if(errorLog == 0){
        //Tries to find the last checkpoint begining position
        seek_log_file = readFinalLogSeek(CHECKPOINT_LOG_SEEK);
        if(seek_log_file == -1){
          seek_log_file = 0;
        }
        else
          serverLog(LL_NOTICE,"The indexed log will be rebuild from the last checkpoint!");
      }
    }

    DB *dbp_replica = NULL;
    if(server.indexedlog_replicated == IR_ON){
      dbp_replica = openIndexedLog(server.indexedlog_replicated_filename, 'W', &errorLog);
      //if(errorLog != 0)
      //  serverLog(LL_NOTICE,"Cannot open the indexed log file replica!");
    }

    dbp = openIndexedLog(server.indexedlog_filename, 'W', &errorLog);
    if(errorLog != 0){
      serverLog(LL_NOTICE,"Cannot open the indexed log! The initial indexing could not start!");
      return 0;
    }

    //Opens the sequential log file in the posistion of the last record indexed.
    FILE *fp = fopen(server.aof_filename,"r");
    fseek(fp, seek_log_file, SEEK_SET);
    
    struct redis_stat sb;
    unsigned long long int count_records = 0, //Counts the number of records processed from sequential log
                           count_records_indexed = 0; //Counts the number of records indexed from sequential log

    if (fp == NULL) {
        serverLog(LL_WARNING,"Fatal error: can't open the append log file for reading: %s",strerror(errno));
        exit(1);
    }

    /* Handle a zero-length AOF file as a special case. An empty AOF file
     * is a valid AOF because an empty server with AOF enabled will create
     * a zero length file at startup, that will remain like that if no write
     * operation is received. */
    if (fp && redis_fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        server.aof_current_size = 0;
        fclose(fp);
        //if the indexed log exists and the sequential log was removed.
        if(seek_log_file > 0){
            serverLog(LL_NOTICE, "The initial indexing could not start since sequential log was removed! "
                "However, it is still possible to recover the database from indexed log.");
        }else//If the indexed and sequential log files are empty ou don't exist.
            serverLog(LL_NOTICE,"The indexing could not start since sequential log file is empty!");
        seek_log_file = 0;
        writeFinalLogSeek(FINAL_LOG_SEEK, seek_log_file);
        closeIndexedLog(dbp);
        return 0;
    }

    serverLog(LL_NOTICE,"Indexing the remaining log records after the last shutdown/crash ... Wait!");

    sds log_record = sdsnew(""), key = sdsnew(""), command = sdsnew("");
    const sds SET_COMMAND  = sdsnew("SET"), 
          INCR_COMMAND  = sdsnew("INCR"), 
          DEL_COMMAND  = sdsnew("DEL");
    /* Read the actual AOF file, in REPL format, command by command. */
    while(1) {
        int argc, j;
        unsigned long len;
        char buf[128];
        sds argsds;

        if (fgets(buf,sizeof(buf),fp) == NULL) {
            break;
        }
        count_records++;

        seek_log_file = seek_log_file +  strlen(buf);
        log_record = sdscpy(log_record , buf);
        
        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1) goto fmterr;

        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                printf("\n\n Error on Log indexing! Error1\n");
                goto readerr;
            }
            seek_log_file = seek_log_file +  strlen(buf); 
            log_record = sdscat(log_record, buf);

            if (buf[0] != '$') goto fmterr;
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(SDS_NOINIT,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                printf("\n\n Error on Log indexing! Error2\n");
                goto readerr;
            }
            seek_log_file = seek_log_file +  len;
            log_record = sdscatsds(log_record, argsds);
            if(j+1<argc)
                log_record = sdscat(log_record, "\n");

            if(j==0)//Get the command
                command = sdscpy(command, argsds);
            if(j==1)//Get the key
                key = sdscpy(key, argsds);

            if (fread(buf,2,1,fp) == 0) {
                printf("\n\n Error on Log indexing! Error3\n");
                goto readerr; /* discard CRLF */
            }
            seek_log_file = seek_log_file +  2;
            sdsfree(argsds);
        } 
        
        sdstoupper(command);
        if(sdscmp(command, SET_COMMAND) == 0){
            delRecordIndexdLog(dbp, key);
            addRecordIndexedLog(dbp, key, log_record);
            count_records_indexed++;
            if(server.indexedlog_replicated == IR_ON){
              delRecordIndexdLog(dbp_replica, key);
              addRecordIndexedLog(dbp_replica, key, log_record);
            }
        }else{
            if(sdscmp(command, INCR_COMMAND) == 0){
                addRecordIndexedLog(dbp, key, log_record);
                count_records_indexed++;
                if(server.indexedlog_replicated == IR_ON)
                  addRecordIndexedLog(dbp_replica, key, log_record);
            }else{
                if(sdscmp(command, DEL_COMMAND) == 0){
                    delRecordIndexdLog(dbp, key);
                    count_records_indexed++;
                    if(server.indexedlog_replicated == IR_ON)
                      delRecordIndexdLog(dbp_replica, key);
                }
            }
        }

    }
    server.initial_indexing_end_time = ustime();
    server.count_initial_records_proc = count_records;
    server.initial_indexed_records = count_records_indexed;

    writeFinalLogSeek(FINAL_LOG_SEEK, seek_log_file);
    server.seek_log_file = seek_log_file;
    
    sdsfree(key);
    sdsfree(log_record);
    sdsfree(SET_COMMAND);
    sdsfree(INCR_COMMAND);
    sdsfree(DEL_COMMAND);
    sdsfree(command);
    fclose(fp);
    closeIndexedLog(dbp);

    serverLog(LL_NOTICE,"Initial log indexing finished: %.3f seconds. Number of log records processed = %llu."
      " Number of records on indexed log = %llu.",
        (float)(server.initial_indexing_end_time-server.initial_indexing_start_time)/1000000, 
        count_records, count_records_indexed);

    if(server.indexedlog_replicated == IR_ON){
      writeFinalLogSeek(FINAL_LOG_SEEK_REPLICA, seek_log_file);
      closeIndexedLog(dbp_replica);
      serverLog(LL_NOTICE,"The indexed log file replica was updated!");
    }

    return count_records;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    if (!feof(fp)) {
        serverLog(LL_WARNING,"Indexing error! Unrecoverable error reading the append only file wh: %s", strerror(errno));
        exit(1);
    }

fmterr: /* Format error. */
    serverLog(LL_WARNING,"Indexing error! Bad file format reading the sequential log file. Last log record read: %s", log_record);
    stopMemtierBenchmark();
    exit(1);
} 


/*
    Indexes a log record on indexed log sychronously, i.e., the transaction should wait to an
    insertion in the indexed log. This fucntion is called by the aofWrite() function on aof.c .
    buf: string containing the log records
*/
void synchronousIndexing(const char *buf){
  /* If it is a synchronous IR (i.e. the transactions wait the indexing), 
              it is necessary to index the log records now. */
  if(server.instant_recovery_state == IR_ON){
      if(server.instant_recovery_synchronous == IR_ON){
          //printf("record = %s, len = %ld\n", buf, strlen(buf));
          sds dataSds;
          int ret, countArray;
          DB *dbp = openIndexedLog(server.indexedlog_filename, 'W', &ret);
          if(ret != 0){
              serverLog(LL_NOTICE,"Cannot open the indexed log! Cannot index the log record synchronously!");
          }else{
              //char **array_log_record_lines = str_split((char *)buf, '\n');

              dataSds = sdsnew((char *)buf);
              char ** array_log_record_lines = sdssplitlen(dataSds, sdslen(dataSds), "\n", 1, &countArray);
              sdsfree(dataSds);

              /*printf("array count = %d\n", countArray);
              for(int j=0; j<=countArray; j++){
                
                if(array_log_record_lines[j] == NULL){
                  printf("break line %d\n", j);
                }else
                  if(strlen(array_log_record_lines[j]) == 0){
                    printf("empty line %d\n", j);
                  }else
                   printf("line %d: %s\n", j, array_log_record_lines[j]);
              }*/
              

              int k = 0, argc;
              char log_record[600], key[50], value[50], command [50], str_aux[50];

              int i = 1;
              while( k <  countArray){
                if(array_log_record_lines[k] == NULL){
                  //printf("break line %d\n", k);
                  break;
                }
                if(strlen(array_log_record_lines[k]) == 0){
                  //printf("empty line %d\n", k);
                  break;
                }
                  //printf("i = %d, k = %d => ", i, k);
                  strcpy(str_aux, array_log_record_lines[k]);
                  argc = atoi(str_aux+1); //number of log record parameters
                  
                  strcpy(command, array_log_record_lines[k+2]);
                  command[strlen(command)-1] = '\0';
                  if(strlen(command) == 0)
                    break;
                  strcpy(key, array_log_record_lines[k+4]);
                  key[strlen(key)-1] = '\0';
                  //printf("command = %s, key = %s\n", command, key);
                  
                  if(strcasecmp(command, "SET") == 0){
                      strcpy(value, array_log_record_lines[k+6]);
                      value[strlen(value)-1] = '\0';

                      //generates da log record
                      strcpy(log_record, "*3\n$3\nSET\n$");
                      sprintf(str_aux, "%li", strlen(key));
                      strcat(log_record, str_aux);
                      strcat(log_record, "\n");
                      strcat(log_record, key);
                      strcat(log_record, "\n$");
                      sprintf(str_aux, "%li", strlen(value));
                      strcat(log_record, str_aux);
                      strcat(log_record, "\n");
                      strcat(log_record, value);

                      //indexes the log record
                      delRecordIndexdLog(dbp, key);
                      addRecordIndexedLog(dbp, key, log_record);
                  }

                  //skips to the next log record
                  k = k + 2*argc + 1;
                  i++;
              }

              closeIndexedLog(dbp); 
          }
      }
  }
}
// ==================================================================================
/* Checkpoint functions */

/*
    Defines an in-memory hash that stores accesses to tuples. Each record contains the 
    number of accesses to a tuple.
    This hash is used by the Checkpoint component to know the most frequently used tuples.
    id: key of the tuple accessed
    count: number of accesses to the tuple
*/
typedef struct accessed_tuples_log_type {
    sds id;                   //key of the tuple
    unsigned long long count; //number of accesses to a tuple
    UT_hash_handle hh; 
}accessed_tuples_log;

accessed_tuples_log *hash_keys_access = NULL;

/*
    Deletes a record of tuple access information.
*/
void delAccessedTuple(accessed_tuples_log *accessedTuple) {
    HASH_DEL( hash_keys_access, accessedTuple);
}

/*
    Returns the object that contains a tuple access information.
*/
accessed_tuples_log *getAccessedTuple(char *key) {
    accessed_tuples_log *s = NULL;

    HASH_FIND_STR( hash_keys_access, key, s );

    return s;
}

/*
    Returns the count of accesses to a tuple using its key.
*/
unsigned long long getCountAccessedTuple(char *key) {
    accessed_tuples_log *s = NULL;

    HASH_FIND_STR( hash_keys_access, key, s );
    if(s == NULL)
        return 0;
    else
        return s->count;
}

/*
    Increments the number of accesses to a tuple using its key.
*/
void incrementAccessedTuple(char *key) {
    //unsigned long long count = 0;
    accessed_tuples_log *counter = getAccessedTuple(key);

    if(counter != NULL){
        counter-> count = counter-> count + 1;
    }else{
      accessed_tuples_log *rr;
      rr = zmalloc(sizeof(accessed_tuples_log));
      rr->id = sdsnew(key);
      rr->count = 1;
      HASH_ADD_STR( hash_keys_access, id, rr);
    }
}

/*
    Removes all the hash records.
*/
void clearHashAccessedTuples() {
  accessed_tuples_log *current_key, *tmp;

  HASH_ITER(hh, hash_keys_access, current_key, tmp) {
    HASH_DEL(hash_keys_access, current_key);  /* delete; users advances to next */
    zfree(current_key->id);
    zfree(current_key);            /* optional- if you want to free  */
  }
}

/*
    Prints the key of all accessed tuples and the number of accesses.
*/
void printAccessedTuples() {
    unsigned long long n = 0;
    accessed_tuples_log *s;

    printf("List of accessed keys: \n");
    for(s=hash_keys_access; s != NULL; s=s->hh.next) {
        printf("%llu: %s -> %llu\n", ++n, s->id, s->count);
    }
}

/*
    Return the number of tuples accessed.
*/
unsigned long long countAccessedKeys(){
    return HASH_COUNT(hash_keys_access);
}

/*
    Performs a checkpoint process.
*/
void checkpointProcess(int idCheckpoint){
    if(server.checkpoint_state == IR_OFF)
      return;

    long long startTime = ustime();
    //Gets the checkpoing beggining position in the sequential log
    unsigned long long seek_log_file = server.seek_log_file;

    int error;
    redisContext *redisConnection = openRedisClient(&error);
    if(error != 0){
      server.checkpoint_state = IR_OFF;
      server.checkpint_performing = IR_OFF;
      serverLog(LL_NOTICE, "The checkpoint can not start! Error connecting to redis server.");
      return;
    }

    //int id = addCheckpointReport(startTime);

    if(server.display_checkpoint_information == IR_ON)
      serverLog(LL_NOTICE,"Checkpoint process %d started! Ratio between log records and tuples in the indexed log = "
                          "Checkpointing ...", idCheckpoint);
    else
      serverLog(LL_NOTICE,"Checkpoint process %d started! Checkpointing ...", idCheckpoint);

    unsigned long long keysCheckpointed = 0;

    //Performs a MFU checkpoint
    if(server.checkpoints_only_mfu == IR_ON){
      accessed_tuples_log *s;
      server.accessed_tuples_logger_state = IR_OFF;
      for(s=hash_keys_access; s != NULL; s=s->hh.next) {
        //If recieves a signal to stop the checkpoint than breaks
        if(server.checkpoint_state != IR_ON){
          serverLog(LL_NOTICE,"The checkpoint process was stopped before finishing! ");
          break;
        }
        redisCommand(redisConnection,"SETCHECKPOINT %s %s", s->id, "NULL");
        keysCheckpointed++;
      }
      clearHashAccessedTuples();
      server.accessed_tuples_logger_state = IR_ON;
    }//Performs Full checkpoint
    else{
      dictIterator *di;
      dictEntry *de;
      //Gets the keys of tuples in the memory and generates setCheckpoint commands.    
      di = dictGetSafeIterator(server.db->dict);
      while((de = dictNext(di)) != NULL ) {
        //If recieves a signal to stop the checkpoint than breaks
        if(server.checkpoint_state != IR_ON){
          serverLog(LL_NOTICE,"The checkpoint process was stopped before finishing! ");
          break;
        }
        redisCommand(redisConnection,"SETCHECKPOINT %s %s", dictGetKey(de), "NULL");
        keysCheckpointed++;
      }
      dictReleaseIterator(di);
    }
    
    //Sends a command to flush a end checkpoint log record
    char id_str[50];
    sprintf(id_str, "%d", idCheckpoint);
    redisCommand(redisConnection,"checkpointEnd %s %s", id_str, "NULL");
    redisFree(redisConnection);

    //Marks the checkpoint begginig postion in the sequential log
    if(server.checkpoint_state == IR_ON && server.checkpoints_only_mfu == IR_OFF && server.checkpoint_state == IR_ON)
      writeFinalLogSeek(CHECKPOINT_LOG_SEEK, seek_log_file);
 
    long long endTime = ustime();
    printCheckpointTimeToCSV(idCheckpoint, startTime, endTime);
    //updateCheckpointReport(id, endTime, keysCheckpointed);
    if(server.display_checkpoint_information == IR_ON)
      serverLog(LL_NOTICE,"Checkpoint process finished! ");
    else
      serverLog(LL_NOTICE,"Checkpoint process %d finished!", idCheckpoint);

    selfTuneCheckpointTimeInterval((endTime-startTime)/1000000);
}

/*
  Performs checkpoint processes in time intevals.
*/
void *executeCheckpoint(){
    if(server.checkpoint_state == IR_OFF)
      return (void *)0;

    server.checkpint_performing = IR_ON;
    if(server.checkpoints_only_mfu == IR_OFF)
      serverLog(LL_NOTICE,"Checkpointer thread started!"
                          " The first checkpointed wiil start in %d seconds.", 
                          server.first_checkpoint_start_time);
    else
      serverLog(LL_NOTICE,"Checkpointer thread started!"
                          " MFU is ON! The first checkpointed wiil start in %d seconds.", 
                          server.first_checkpoint_start_time);

    //Start the first checkpoint after an time defined in first_checkpoint_start_time
    if(server.first_checkpoint_start_time > 0)
      sleep(server.first_checkpoint_start_time);

    int count_checkpoint = 0;
    if(server.checkpoint_state == IR_ON){
      //Performs checkpoints at time intevals
      do{
        checkpointProcess(count_checkpoint+1);

        //Stops the checkpoint cycles if recieves a signal do stop
        if(server.checkpoint_state == IR_OFF)
          break;

        sleep(server.checkpoint_time_interval);

        count_checkpoint++;
      }while(server.checkpoint_state == IR_ON && server.number_checkpoints != count_checkpoint);
    }

    serverLog(LL_NOTICE,"Checkpointer thread finished!");

    server.checkpint_performing = IR_OFF;

    return (void *)1;  
}

/*
    Sends a signal to stop the indexer proforming.
*/
void stopCheckpointProcess(){
  if(server.checkpoint_state == IR_ON){
    server.checkpoint_state = IR_OFF;

    //Stores the end time of the checkpoint process if it is stopped after finishing
    //if(lastCheckpointReport != NULL && lastCheckpointReport->checkpoint_end_time == 0)
      //updateCheckpointReport(lastCheckpointReport->id, ustime(), 0);
    
    serverLog(LL_NOTICE,"Checkpoint is disabled and the Checkpointer thread will stop in few minutes!");
    //pthread_cancel(server.checkpoint_thread);
  }
}

/*
  Waits until the Checkpointer thread to finish
*/
void waitCheckpointerFinish(){
  if(server.checkpint_performing == IR_ON){

    if(server.display_checkpoint_information == IR_ON)
      serverLog(LL_NOTICE,"Waiting the checkpoint to finish ...");

    while(server.checkpint_performing == IR_ON){
      //printf("server.checkpint_performing = %d, server.checkpoint_state = %d \n", server.checkpint_performing, server.checkpoint_state);
      sleep(0.5);
    }
  }
}

/*
  Self tune the time interval between checkpoints.
  time_interval: a previous time used to tune the time interval.
*/
void selfTuneCheckpointTimeInterval(int time_interval){
  if(server.selftune_checkpoint_time_interval == IR_ON){
    time_interval = time_interval/2;
    if(time_interval < 60)
      time_interval = 60;
    server.checkpoint_time_interval = time_interval;
    if(server.display_checkpoint_information == IR_ON)
      serverLog(LL_NOTICE, "Checkpoint time inteval was tunned to %d seconds.", time_interval);
  }
}

//==========================================================================================
// Restart functions. These functions are used to simulate system failures.

/*
    Returns the counter of sysmtem restarts stored in a binary file. 
    If the binary file does not exist, returns -1.
*/
int readRestartCounter(){
  FILE *binaryFile = fopen(RESTART_COUNTER, "rb"); 

    if (binaryFile == NULL){
        return -1;
    }

    int counter = 0;

    int result = fread( &counter, sizeof(int), 1, binaryFile );
    fclose(binaryFile);
    if(result == 0)
      counter = 0;
    return counter;
}

/*
    Writes in the file 'restartCounter.dat' the counter of system restarts.
    Returns true if the wrinting was successful. 
    If the binary file does not exist, returns -1.
*/
int writeRestartCounter(int counter){
  FILE *binaryFile = fopen(RESTART_COUNTER, "wb");

  if (binaryFile == NULL){
    printf("Fail to open %s!\n", RESTART_COUNTER);
    return -1;
  }
    
  int result = fwrite (&counter, sizeof(unsigned long long int), 1, binaryFile);
  fclose(binaryFile);

  return result;
}

/*
  Restarts the system a number of times after the benchmark performing
*/
int restartAfterBechmarking(){
  if(server.restart_after_benchmarking == 0)
    return 0;
    
  int counter = readFile(RESTART_COUNTER);
  //If the number of times to restart is over , removes the file restartCounter.dat
  if(counter == 0){
    removeFile(RESTART_COUNTER);
    return 0;
  }
  //If it is the first time to restart, initiates te counter
  if(counter == -1)
    counter = server.restart_after_benchmarking-1;
  else{
    //Decrements the counter o times to restart
    if(counter > 0)
      counter--;
  }
  writeFile(RESTART_COUNTER, counter);

  serverLog(LL_NOTICE, "Restarting de server %d of %d ...", 
    server.restart_after_benchmarking-counter, server.restart_after_benchmarking);

  //Stores the shutdown time for generatins reports
  long long int time = ustime();
  //if(server.generate_executed_commands_csv == IR_ON)
  printShutdownTimeToCSV(time);
  //if(server.generate_indexing_report_csv == IR_ON)
  //printShutdownTimeToCSV(server.indexing_report_csv_filename, time);
  
  restartServer(RESTART_SERVER_GRACEFULLY, server.restart_daley_time);

  return 1;
}

void *restartAfterTime(){
  serverLog(LL_NOTICE,"The system is programmed to restart in %llu seconds!", 
                        server.restart_after_time);
  sleep(server.restart_after_time);
  serverLog(LL_NOTICE,"Preparing to restart ...");

  server.recovery_start_time = -1;
  server.recovery_end_time = -1;
  server.database_startup_time = -1;

  //stopThredas();

  ///cancelIRThreads();

  serverLog(LL_NOTICE,"Restarting the system now!");

  //Stores the shutdown time for generatins reports
  long long int time = ustime();
  //if(server.generate_executed_commands_csv == IR_ON)
  printShutdownTimeToCSV(time);
  //if(server.generate_indexing_report_csv == IR_ON)
  //printShutdownTimeToCSV(server.indexing_report_csv_filename, time);

  restartServer(RESTART_SERVER_GRACEFULLY, server.restart_daley_time);

  return (void *)1;
}


int restartSystem(){
  if(server.number_restarts_after_time == 0)
    return 0;
    
  int counter = readFile(RESTART_COUNTER2);
  //If the number of timesrestartSystem to restart is over, removes the file restartCounter.dat and exit
  if(counter == 0){
    removeFile(RESTART_COUNTER2);
    return 0;
  }
  //If it is the first time to restart, initiates te counter
  if(counter == -1)
    counter = server.number_restarts_after_time-1;
  else{
    //Decrements the counter of times to restart
    if(counter > 0)
      counter--;
  }
  writeFile(RESTART_COUNTER2, counter);

  //stopIndexing();
  //stopCheckpointProcess();

  serverLog(LL_NOTICE, "The server will be restarted %d times!", counter+1);

  pthread_create(&server.restartAfterTime_thread, NULL, restartAfterTime, NULL);

  return 1;
}

/*
  Loads the database into memory and restart the system after a time
*/
int preloadDatabaseAndRestart(){
  if(server.preload_database_and_restart == 0)
    return 0;

  int counter = readFile(DATABASE_PRELOAD_FILE);

  //If the counter is zero, it is the last time running.
  if(counter == 0){
    removeFile(DATABASE_PRELOAD_FILE);
    return 0;
  }

  int preloded = 0;
  //If the counter is -1, it is the first time running. Then the system should be preloaded and restarted later
  if(counter == -1){
    writeFile(DATABASE_PRELOAD_FILE, server.number_restarts_after_preloading-1);

    serverLog(LL_NOTICE,"Preloading the database ...");

    //server.checkpoint_time_interval = 10;
    //server.selftune_checkpoint_time_interval = IR_OFF;
    
    loadDataFromDisk();
    preloded = 1;
    counter = server.number_restarts_after_preloading - 1;

    //set the current time as databas startup
    server.database_startup_time = ustime();
  }else{
    removeFile(DATABASE_PRELOAD_FILE);
    counter = counter -1;
    writeFile(DATABASE_PRELOAD_FILE, counter);
  }

  //the server should be restarted 
  if(server.number_restarts_after_preloading > 0){
    server.restart_after_time = server.preload_database_and_restart;
    serverLog(LL_NOTICE, "The server will be restarted %d time(s)!", counter+1);

    pthread_create(&server.restartAfterTime_thread, NULL, restartAfterTime, NULL);
  }else{
    removeFile(DATABASE_PRELOAD_FILE);
  }

  if(server.instant_recovery_state == IR_ON && server.instant_recovery_synchronous == IR_OFF){
    pthread_create(&server.indexer_thread, NULL, indexesSequentialLogToIndexedLogV2, NULL);
    pthread_create(&server.checkpoint_thread, NULL, executeCheckpoint, NULL);
  }

  return preloded;
}


void *corruptIndexedLog(){
  if(server.log_corruption == 0)
    return (void *)0;

  printf("here\n");

  int restart = 0;
  
  //Restarts to simulate a failure and use the log replica
  if(server.indexedlog_replicated == IR_ON && access(server.indexedlog_replicated_filename, F_OK) != -1){
    restart = 1;
    printf("indexed prog\n");
  }else{//Restarts to use the indexed log rebuilding
    printf("log rebuild prog\n");
    int counter = readFile(RESTART_COUNTER3);
    //If it is the first time to restart, initiates te counter
    if(counter == -1){
      writeFile(RESTART_COUNTER3, 0);
      restart = 1;
    }else{//If the system was previouly restarted, it is not necessary to restart again.
      removeFile(RESTART_COUNTER3);
    }
  }

  if(restart){
    serverLog(LL_NOTICE,"The system is programmed to restart in %llu seconds!"
                        " The system will simulate a log corruption!", 
                        server.log_corruption);
    sleep(server.log_corruption);
    serverLog(LL_NOTICE,"Preparing to restart ...");

    server.recovery_start_time = -1;
    server.recovery_end_time = -1;
    server.database_startup_time = -1;
    stopCheckpointProcess();
    //clrCheckpointReport();
    stopMemtierBenchmark();
    stopIndexing();

    //Simulate a log corruption
    if(removeFile(server.indexedlog_filename) == 0)
      serverLog(LL_NOTICE,"The indexed log was removed! %s", 
                          server.indexedlog_replicated_filename);
    else
      serverLog(LL_NOTICE,"The indexed log was not removed! %s",
                          server.indexedlog_replicated_filename);

    waitMemtierBenchmarkFinish();
    waitIndexerFinish();

    serverLog(LL_NOTICE,"Restarting the system now here!");

    //Stores the shutdown time for generatins reports
    long long int time = ustime();
    //if(server.generate_executed_commands_csv == IR_ON)
    printShutdownTimeToCSV(time);
    //if(server.generate_indexing_report_csv == IR_ON)
    //printShutdownTimeToCSV(server.indexing_report_csv_filename, time);

    restartServer(RESTART_SERVER_GRACEFULLY, server.restart_daley_time);

    return (void *)1;
  }

  return (void *)0;
}

void stopThredas(){
  //Stops the running of the threads
  stop_loadDBFromIndexedLog();
  stopCommandsExecuted();
  stopMemtierBenchmark();
  stopCheckpointProcess();
  stopIndexing();
  stopSystemMonitoringFinish();
  stopIndexingReport();

  //clrCheckpointReport();

  //Waits until the threads to fininsh to avoid log corruption
  //waitCheckpointerFinish();
  waitLoadDBFromIndexedLogFinish();
  waitMemtierBenchmarkFinish();
  waitIndexerFinish();
  waitSystemMonitoringFinish();
  waitCommandsExecutedFinish();
  waitIndexingReportFinish();
}

void cancelIRThreads(){
  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
  pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

  pthread_cancel(server.indexer_thread);
  pthread_cancel(server.checkpoint_thread);
  pthread_cancel(server.load_data_incrementally_thread);
  pthread_cancel(server.generate_executed_commands_csv_thread);
  pthread_cancel(server.memtier_benchmark_thread);
  pthread_cancel(server.system_monitoring_thread);
  pthread_cancel(server.log_corruption_thread);
  //pthread_cancel(server.stop_memtier_benchmark);
  //pthread_cancel(server.restartAfterTime_thread);

  serverLog(LL_NOTICE,"The RedisIR threads were canceled! ");
}

//==========================================================================================
// Memtier benchmark functions

/*
  Executes the memtier bechmark automatically
*/
void *executeMemtierBenchmark(){
    server.memtier_benchmark_performing = IR_ON;
    server.memtier_benchmark_start_time = ustime();

    char program[1000];
    int i = 0,error = 0;
    do{
      i++;
      strcpy(program, "cd memtier_benchmark; memtier_benchmark  ");
      strcat(program, server.memtier_benchmark_parameters);
      serverLog(LL_NOTICE, "Memtier benchmark (round %d) started! Parameters = \"%s\"", 
                            i, server.memtier_benchmark_parameters);

      error = system(program);
    }while(server.memtier_benchmark_workload_run_times != i && error != -1 && server.memtier_benchmark_state == IR_ON);

    server.memtier_benchmark_end_time = ustime();

    if(error != -1)
        serverLog(LL_NOTICE, "Memtier benchmark execution finished: %.3f seconds.", 
            (float)(server.memtier_benchmark_end_time-server.memtier_benchmark_start_time)/1000000);
    else
        serverLog(LL_NOTICE, "Memtier benchmark could not be executed!");

    printBenchmarkTimeToCSV();
    
    if(server.stop_checkpoint_after_benchmark == IR_ON){
      stopCheckpointProcess();
      //waitCheckpointerFinish();
    }

    if(server.stop_system_monitoring_end_benckmark == IR_ON)
      stopSystemMonitoringFinish();

    //Stores information about the recovery.
    if(server.generate_recovery_report == IR_ON && server.generate_report_file_after_benchmarking == IR_ON)
      printRecoveryReportToFile();

    server.memtier_benchmark_performing = IR_OFF;

    restartAfterBechmarking();

    return (void *)1;
}

/*
    Kills the Memtier Benchmark performing.
*/
int stopMemtierBenchmark(){
    if(server.memtier_benchmark_state == IR_ON){
        server.memtier_benchmark_state = IR_OFF;
        int error = system("pkill -f memtier_benchmark");
        if(error == -1){
            serverLog(LL_NOTICE, "Error while killing the Memtier Bechmark!"
                                " The benchmarking will only stop at the execution end!");
            return 0;
        }else{
          serverLog(LL_NOTICE, "Memtier benchmark process was interrupted before its execution was finished !");
        }
        return 1;
    }
    return 0;
}

/*
  Waits until the Memtier thread to finish
*/
void waitMemtierBenchmarkFinish(){
  while(server.memtier_benchmark_performing == IR_ON){
    sleep(0.05);
  }
}

/*
  Stops the Memtier performing after a time from the database startup
*/
void *stopMemtierBenchmarkAfterTimeAlways(){
  if(server.memtier_benchmark_state == IR_OFF)
    return (void *)0;

  if(server.time_tostop_benchmarking == 0)
    return (void *)0;

  serverLog(LL_NOTICE,"Memtier Bechmark is programmed to stop after %d seconds!", 
                      server.time_tostop_benchmarking);
  
  sleep(server.time_tostop_benchmarking);

  if(!stopMemtierBenchmark())
    serverLog(LL_NOTICE,"Memtier Bechmark could not be stopped!");  

  return (void *)1;  
}
