#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "sht_file.h"
#define GLOBAL_DEPT_1 2 // you can change it if you want
#define FILE_NAME_1 "data_10.db"
#define FILE_NAME_2 "data_11.db"
#define FILE_NAME_3 "data_12.db"

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

  // Create the primary file
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME_1, GLOBAL_DEPT_1));
  
  int r;

  printf("Choose a wrong function call:\n");
  printf("SHT_CreateSecondaryIndex with non-supported attribute (1)\n");
  printf("SHT_InnerJoin with incompatible attribute types (2)\n");
  if (scanf("%d", &r) == EOF) {
    printf("scanf failed");
    exit(1);
  }
  int indexDesc1;
  tTuple rec_pos;
  UpdateRecordArray updateArray;
  switch(r) {
    case 1:
      CALL_OR_DIE(SHT_CreateSecondaryIndex(FILE_NAME_2, "other", 20, GLOBAL_DEPT_1, FILE_NAME_1));
      break;
    case 2:
      CALL_OR_DIE(SHT_CreateSecondaryIndex(FILE_NAME_2, "surname", 20, GLOBAL_DEPT_1, FILE_NAME_1));
      int indexDesc1;
      CALL_OR_DIE(HT_OpenIndex(FILE_NAME_2, &indexDesc1));
      CALL_OR_DIE(SHT_CreateSecondaryIndex(FILE_NAME_3, "city", 20, GLOBAL_DEPT_1, FILE_NAME_1));
      int indexDesc2;
      CALL_OR_DIE(HT_OpenIndex(FILE_NAME_3, &indexDesc2));
      CALL_OR_DIE(SHT_InnerJoin(indexDesc1, indexDesc2, NULL));
      break;
    default:
      printf("Not an option\n");
      break;
  }

  BF_Close();
}