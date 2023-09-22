/*
 gcc indexedLog_features.c -o indexedLog_features.o -ldb

*/

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <db.h>
#include <time.h>

#include "uthash.h"


char *DATABASE = "../logs/indexedLog.db";

/*
Opens a Berkeley DB and returns a pointer to the database.
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

  //if(mode == 'R')
  //  flags = DB_RDONLY; /* Treat the database as read-only.*/ 

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
retult: returns 0 (zero) if the indexel log is openned. If fail, returns a code error.
*/
DB* openIndexedLog(char* file_name, char mode, int *result){
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
    //data_structure = DB_HASH;
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
    Returns the number of tuple in the indexed log.
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


int main(void) {
	DB *dbp;
	int ret;
	

	dbp = openIndexedLog(DATABASE, 'R', &ret);

  if(ret != 0){
    printf("Error on openning the database!\n");
    exit(0);
  }
  
  printf("Counting the records in the indexed log ...\n" );
  time_t start = time(NULL);
  unsigned long long count = countRecordsIndexedLog(dbp);
  time_t end = time(NULL);
  printf("Number of log records = %llu. Finished in %ld seconds.\n\n", count, end-start);

  printf("Calculating the number of tuples in the indexed log...\n" );
  start = time(NULL);
  unsigned long long tuples = countTuplesIndexedLog(dbp);
  end = time(NULL);
  printf("Number of tuples = %llu. Finished in %ld seconds!\n\n", tuples, end-start);

  
  printf("Average of log records per tuple = %f\n", (float)count/tuples);

	closeIndexedLog(dbp);
  return 0;
}
