#include <string.h>
#include <stdlib.h>
#include <db.h>




DB* openBerkeleyDB(char* file_name, u_int32_t flags, u_int32_t duplicates, u_int32_t data_structure, int *result){
    DB *BDB_database;//A pointer to the database

    int ret = db_create(&BDB_database, NULL, 0);
    *result = ret;
    if (ret != 0) {
        printf("Error while creating the BerkeleyDB database! \n");
        return BDB_database;
    }
    *result = ret;
  
  /* We want to support duplicates keys*/
    if(duplicates == DB_DUP || duplicates == DB_DUPSORT){
        ret = BDB_database->set_flags(BDB_database, DB_DUP);
        if (ret != 0) {
            printf("Error while setting the DB_DUMP flag on BerkeleyDB! \n");
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
        printf("Error while openning the BerkeleyDB database!");
        return BDB_database;
    }

    return BDB_database;
}

/*
Opens the indexed log and returns a pointer to the database.
Returns a pointer to hadle the Berkeley database.
file_name: Berkeley database file name;
mode: 
    W: Create the underlying database and any necessary physical files.
    R: Treat the data base as read-only.
    T: The returned handle is free-threaded, that is, it can be used simultaneously by multiple threads within the process. 
retult: returns 0 (zero) if the indexed log is openned. If fail, returns a code error.
*/
DB* openIndexedLog(char* file_name, char mode, char *indexedlog_structure, int *result){
    u_int32_t flags;
    switch (mode){
        case 'W': flags = DB_CREATE; break;
        case 'R': flags = DB_RDONLY; break;
        case 'T': flags = DB_THREAD; break;
        default:
        printf("Invalide database openning mode! \n");
        exit(0);
    }

    u_int32_t data_structure;
    if(strcmp(indexedlog_structure,"BTREE") == 0)
        data_structure = DB_BTREE;
    else
        if(strcmp(indexedlog_structure,"HASH") == 0)
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
Insert a log record (sting) by its tupe key (string) to indexed log (BerkeleyDB)
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
    Delete a data on BerkeleyDB
    Return a non-zero DB->del() error if fail
*/
int delDataBerkeleyDB(DB *dbp, DBT key){
    int error;

    error = dbp->del(dbp, NULL, &key, 0);

    return error;
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

int main(void) {
char *indexedlog_filename ="indexedLog.db", *indexedlog_filename2 ="indexedLog_hash.db";

    DB *dbp;
    int error;
      
    dbp = openIndexedLog(indexedlog_filename, 'R', "BTREE", &error);
    if(error != 0){
        printf("⚠ ⚠ ⚠ ⚠ Database loading failed! Error when openning the Indexed Log. ⚠ ⚠ ⚠ ⚠ ");
        exit(0);
    }

    DB *dbp2;
      
    dbp2 = openIndexedLog(indexedlog_filename2, 'W', "HASH", &error);
    if(error != 0){
        printf("⚠ ⚠ ⚠ ⚠ Database loading failed 2! Error when openning the Indexed Log. ⚠ ⚠ ⚠ ⚠ ");
        exit(0);
    }

    printf( "Loading the database from indexed log ... \n");

    DBT key, data;
    /* Zero out the DBTs before using them. */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    DBC *cursorp;
    /* Get a cursor */
    dbp->cursor(dbp, NULL, &cursorp, 0); 

    char k[100], v[100];

    while (cursorp->get(cursorp, &key, &data, DB_NEXT) != DB_NOTFOUND) {
      //strcpy(k, (char *)key.data);
      //strcpy(v, (char *)data.data);
      //printf("(%s, %s) \n", k, v);
      addRecordIndexedLog(dbp2, (char *)key.data, (char *)data.data);
    }

  if (cursorp != NULL)
    cursorp->close(cursorp); 
  closeIndexedLog(dbp);
  closeIndexedLog(dbp2);

  return 0;
}
