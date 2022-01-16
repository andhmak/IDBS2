#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "sht_file.h"

#define RECORDS_NUM 128 // you can change it if you want
#define GLOBAL_DEPT_1 2 // you can change it if you want
#define GLOBAL_DEPT_2 1 // you can change it if you want
#define FILE_NAME_1 "data_7.db"
#define FILE_NAME_2 "data_8.db"
#define FILE_NAME_3 "data_9.db"

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {
  BF_Init(LRU);
  CALL_OR_DIE(HT_Init());

  printf("Create the primary file\n");
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_1, GLOBAL_DEPT_1));
  printf("Create the surname secondary file\n");
  CALL_OR_DIE(SHT_CreateSecondaryIndex(FILE_NAME_2, "surname", 20, GLOBAL_DEPT_2, FILE_NAME_1));
  printf("Create the city secondary file\n");
  CALL_OR_DIE(SHT_CreateSecondaryIndex(FILE_NAME_3, "city", 20, GLOBAL_DEPT_2, FILE_NAME_1));
  printf("Open the primary file\n");
  int indexDesc1;
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME_1, &indexDesc1));
  printf("Open the surname secondary file\n");
  int indexDesc2;
  CALL_OR_DIE(SHT_OpenSecondaryIndex(FILE_NAME_2, &indexDesc2));
  printf("Open the city secondary file\n");
  int indexDesc3;
  CALL_OR_DIE(SHT_OpenSecondaryIndex(FILE_NAME_3, &indexDesc3));
  Record record;
  SecondaryRecord secRecord1, secRecord2;
  int r;
  tTuple rec_pos;
  UpdateRecordArray updateArray;
  printf("Insert Entries with different IDs on the primary and secondary files\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    // create a record
    record.id = id;
    r = id % 11;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = id % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    memcpy(secRecord1.index_key, surnames[r], strlen(surnames[r]) + 1);
    r = id % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);
    memcpy(secRecord2.index_key, cities[r], strlen(cities[r]) + 1);
    CALL_OR_DIE(HT_InsertEntry(indexDesc1, record, &rec_pos, &updateArray));
    secRecord1.tupleId.block_num = rec_pos.block_num;
    secRecord1.tupleId.record_num = rec_pos.record_num;
    secRecord2.tupleId.block_num = rec_pos.block_num;
    secRecord2.tupleId.record_num = rec_pos.record_num;
    CALL_OR_DIE(SHT_SecondaryInsertEntry(indexDesc2, secRecord1));
    CALL_OR_DIE(SHT_SecondaryUpdateEntry(indexDesc2, &updateArray));
    CALL_OR_DIE(SHT_SecondaryInsertEntry(indexDesc3, secRecord2));
    CALL_OR_DIE(SHT_SecondaryUpdateEntry(indexDesc3, &updateArray));
    free(updateArray.record);
    free(updateArray.newTuple);
    free(updateArray.oldTuple);
  }
  
  printf("RUN PrintAllEntries with specific key on the surname secondary file\n");
  CALL_OR_DIE(SHT_PrintAllEntries(indexDesc2, "Ioannidis"));
  
  printf("RUN PrintAllEntries without key on the surname secondary file\n");
  CALL_OR_DIE(SHT_PrintAllEntries(indexDesc2, NULL));
  
  printf("RUN PrintAllEntries with specific key on the city secondary file\n");
  CALL_OR_DIE(SHT_PrintAllEntries(indexDesc3, "Athens"));
  
  printf("RUN PrintAllEntries without key on the city secondary file\n");
  CALL_OR_DIE(SHT_PrintAllEntries(indexDesc3, NULL));

  printf("RUN HashStatistics on the surname secondary open file\n");
  CALL_OR_DIE(HashStatistics(FILE_NAME_2));

  printf("RUN HashStatistics on the city secondary open file\n");
  CALL_OR_DIE(HashStatistics(FILE_NAME_3));

  printf("RUN InnerJoin on the surname with specific key\n");
  CALL_OR_DIE(SHT_InnerJoin(indexDesc2, indexDesc2, "Ioannidis"));

  printf("RUN InnerJoin on the surname without key\n");
  CALL_OR_DIE(SHT_InnerJoin(indexDesc2, indexDesc2, NULL));

  printf("RUN InnerJoin on the city with specific key\n");
  CALL_OR_DIE(SHT_InnerJoin(indexDesc3, indexDesc3, "Athens"));

  printf("RUN InnerJoin on the city without key\n");
  CALL_OR_DIE(SHT_InnerJoin(indexDesc3, indexDesc3, NULL));

  printf("Close the primary file\n");
  CALL_OR_DIE(HT_CloseFile(indexDesc1));
  printf("Close the surname secondary file\n");
  CALL_OR_DIE(SHT_CloseSecondaryIndex(indexDesc2));
  printf("Close the city secondary file\n");
  CALL_OR_DIE(SHT_CloseSecondaryIndex(indexDesc3));

  printf("RUN HashStatistics on the surname secondary closed file\n");
  CALL_OR_DIE(HashStatistics(FILE_NAME_2));
  printf("RUN HashStatistics on the city secondary closed file\n");
  CALL_OR_DIE(HashStatistics(FILE_NAME_3));
  
  BF_Close();
}