#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "bf.h"
#include "sht_file.h"

#define INDEX_ARRAY_SIZE ((BF_BLOCK_SIZE-sizeof(int))/sizeof(int))      // Amount of buckets per block of index
#define PRIMARY_DATA_ARRAY_SIZE ((BF_BLOCK_SIZE-3*sizeof(int))/sizeof(Record))  // Amount of primary records per bucket
#define DATA_ARRAY_SIZE ((BF_BLOCK_SIZE-3*sizeof(int))/sizeof(SecondaryRecord))  // Amount of secondary records per bucket
#define MAX_DEPTH (8*sizeof(int)-8) // Maximum global depth of the hash table
#define SHIFT_CONST (8*sizeof(uint))

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

// Statistical data to be stored in the first block of the file
typedef struct StatBlock {
  int total_recs;
  int total_buckets;
  int globalDepth;
  int attribType;  // 0 for city, 1 for surname
  char mainFileName[NAME_BUF];
} StatBlock;

// Parts of the array to be stored in blocks in the disk
typedef struct IndexBlock {
  int nextBlock;
  int index[INDEX_ARRAY_SIZE];
} IndexBlock;

// For blocks acting as buckets
typedef struct DataBlock {
  int localDepth;
  int lastEmpty;
  int nextBlock;
  SecondaryRecord index[DATA_ARRAY_SIZE];
} DataBlock;

// For blocks acting as buckets
typedef struct PrimaryDataBlock {
  int localDepth;
  int lastEmpty;
  int nextBlock;
  Record index[DATA_ARRAY_SIZE];
} PrimaryDataBlock;

uint hash_string(char* string) {
  uint hash = 5381;

  for (char* s = string; *s != '\0'; s++)
        hash = (hash * 33)+ *s;

  return hash;
}

// Array of open files in memory
extern OpenFileData open_files[MAX_OPEN_FILES];

HT_ErrorCode SHT_Init() {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth, char *fileName) {
  //insert code here
  // Create block file
  CALL_BF(BF_CreateFile(sfileName));
  int fileDesc;
  // Open file
  CALL_BF(BF_OpenFile(sfileName, &fileDesc));

  // Initialise statistics block
  int arraySize = 1 << depth;

  BF_Block* block;
  BF_Block_Init(&block);

  CALL_BF(BF_AllocateBlock(fileDesc, block));

  StatBlock* stat = (StatBlock*) BF_Block_GetData(block);


  stat->globalDepth = depth;
  stat->total_buckets = arraySize;
  stat->total_recs = 0;
  if (!strcmp(attrName, "city")) {
    stat->attribType = 0;
  }
  else if (!strcmp(attrName, "surname")) {
    stat->attribType = 1;
  }
  else {
    fprintf(stderr, "No such attribute");
    CALL_BF(BF_CloseFile(fileDesc));
    return HT_ERROR;
  }
  strcpy(stat->mainFileName, fileName);

  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  // Initialise index blocks
  int indexBlockAmount = ((arraySize - 1) / INDEX_ARRAY_SIZE) + 1;
  for (int i = 0; i < indexBlockAmount; i++){
    CALL_BF(BF_AllocateBlock(fileDesc, block));
    IndexBlock* data = (IndexBlock*) BF_Block_GetData(block);
    if (i+1 < indexBlockAmount) {
      data->nextBlock = i+2;
    }
    else {
      data->nextBlock = -1;
    }
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }

  // Initialise buckets
  for (int i = 0; i < arraySize; i++) {
    CALL_BF(BF_AllocateBlock(fileDesc, block));
    DataBlock* dataBlockData = (DataBlock*) BF_Block_GetData(block);
    dataBlockData->localDepth = depth;
    dataBlockData->lastEmpty = 0;
    dataBlockData->nextBlock = -1;
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }

  // Map index to buckets
  int dataBlockCounter = indexBlockAmount + 1;
  for (int i = 1; i < indexBlockAmount + 1; i++){
    CALL_BF(BF_GetBlock(fileDesc, i, block));
    IndexBlock* data = (IndexBlock*) BF_Block_GetData(block);
    for (int j = 0; j < INDEX_ARRAY_SIZE; j++){
      if (dataBlockCounter < indexBlockAmount + arraySize + 1) {
        data->index[j] = dataBlockCounter;
      }
      else {
        data->index[j] = -1;
      }
      dataBlockCounter++;      
    }
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }

  BF_Block_Destroy(&block);

  // Close file
  CALL_BF(BF_CloseFile(fileDesc));
  return HT_OK;
  return HT_OK;
}

HT_ErrorCode SHT_OpenSecondaryIndex(const char *sfileName, int *indexDesc) {
  //insert code here
  // Check for empty position in open files array
  int i;
  for (i = 0 ; i < MAX_OPEN_FILES ; i++) {
    if(open_files[i].fileDesc == -1) {
      break;
    }
  }
  if (i == MAX_OPEN_FILES) {
    printf("Too many open files\n");
    return HT_ERROR;
  }

  *indexDesc = i;

  // Check if file is already open
  for (int j = 0 ; j < MAX_OPEN_FILES ; j++) {
    if((strcmp(open_files[j].filename, sfileName) == 0) && (open_files[j].mainPos == -1)) {
      strcpy(open_files[i].filename, sfileName);
      open_files[i].mainPos = j;
      open_files[i].fileDesc = open_files[j].fileDesc;
      open_files[i].index = open_files[j].index;
      return HT_OK;
    }
  }
  
  // Else open it bring everything necessary to memory
  strcpy(open_files[i].filename, sfileName);
  open_files[i].mainPos = -1;
  int fd;
  CALL_BF(BF_OpenFile(sfileName, &fd));
  open_files[i].fileDesc = fd;
  
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(fd, 0, block));
  StatBlock* stat = (StatBlock*) BF_Block_GetData(block);
  open_files[i].globalDepth = stat->globalDepth;
  CALL_BF(BF_UnpinBlock(block));
  int indexSize = 1;
  for (int j = 0; j < open_files[i].globalDepth; j++) {
    indexSize *= 2;
  }
  if ((open_files[i].index = malloc(indexSize*sizeof(int))) == NULL) {
    CALL_BF(BF_Close(fd));
    return HT_ERROR;
  }
  CALL_BF(BF_GetBlock(fd, 1, block));
  IndexBlock* data = (IndexBlock*) BF_Block_GetData(block);
  int nextBlock;
  int j = 0;
  do {
    for (int k = 0 ; (k < INDEX_ARRAY_SIZE) && (j < indexSize); k++, j++) {
      open_files[i].index[j] = data->index[k];
    }
    nextBlock = data->nextBlock;
    CALL_BF(BF_UnpinBlock(block));
    if (nextBlock != -1) {
      CALL_BF(BF_GetBlock(fd, nextBlock, block));
      data = (IndexBlock*) BF_Block_GetData(block);
    }
  } while (nextBlock != -1);
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
  //insert code here
  // Check if indexDesc valid
  if ((indexDesc < 0) || (indexDesc >= MAX_OPEN_FILES) || (open_files[indexDesc].fileDesc == -1)) {
    printf("Invalid indexDesc\n");
    return HT_ERROR;
  }

  // Check if secondary entry
  if (open_files[indexDesc].mainPos != -1) {
    open_files[indexDesc].fileDesc = -1;
    strcpy(open_files[indexDesc].filename, "");
    return HT_OK;
  }

  // Check if file is already open elsewhere as secondary entry
  for (int j = 0 ; j < MAX_OPEN_FILES ; j++) {
    if((strcmp(open_files[j].filename, open_files[indexDesc].filename) == 0) && (j != indexDesc)) {
      open_files[indexDesc].fileDesc = -1;
      strcpy(open_files[indexDesc].filename, "");
      open_files[j].mainPos = -1;
      open_files[j].globalDepth = open_files[indexDesc].globalDepth;
      open_files[j].index = open_files[indexDesc].index;
      for (int k = 0 ; k < MAX_OPEN_FILES ; k++) {
        if((strcmp(open_files[j].filename, open_files[k].filename) == 0) && (k != j)) {
          open_files[k].mainPos = j;
        }
      }
      return HT_OK;
    }
  }

  // Else write index information to disk close it completely
  int fd = open_files[indexDesc].fileDesc;
  BF_Block* block;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(fd, 0, block));
  StatBlock* stat = (StatBlock*) BF_Block_GetData(block);
  stat->globalDepth = open_files[indexDesc].globalDepth;
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  CALL_BF(BF_GetBlock(fd, 1, block));
  IndexBlock* data = (IndexBlock*) BF_Block_GetData(block);
  int indexSize = 1;
  for (int j = 0; j < open_files[indexDesc].globalDepth; j++) {
    indexSize *= 2;
  }
  int blockAmount;
  CALL_BF(BF_GetBlockCounter(fd, &blockAmount));
  int nextBlock;
  for (int j = 0 ; j < indexSize ; ) {
    for (int k = 0 ; k < INDEX_ARRAY_SIZE; k++, j++) {
      if (j < indexSize) {
        data->index[k] = open_files[indexDesc].index[j];
      }
      else {
        data->index[k] = -1;
      }
    }
    nextBlock = data->nextBlock;
    if (nextBlock == -1) {
      if (j < indexSize - 1) {
        data->nextBlock = blockAmount++;
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));
        CALL_BF(BF_AllocateBlock(fd, block));
        data = (IndexBlock*) BF_Block_GetData(block);
        data->nextBlock = -1;
      }
      else {
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));
      }
    }
    else {
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));
      if (j < indexSize - 1) {
        CALL_BF(BF_GetBlock(fd, nextBlock, block));
        data = (IndexBlock*) BF_Block_GetData(block);
      }
    }
  }
  BF_Block_Destroy(&block);

  CALL_BF(BF_CloseFile(open_files[indexDesc].fileDesc));
  free(open_files[indexDesc].index);
  open_files[indexDesc].fileDesc = -1;
  strcpy(open_files[indexDesc].filename, "");
  return HT_OK;
}

HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc,SecondaryRecord record) {// Check if indexDesc valid
  if ((indexDesc < 0) || (indexDesc >= MAX_OPEN_FILES) || (open_files[indexDesc].fileDesc == -1)) {
    printf("Invalied indexDesc\n");
    return HT_ERROR;
  }

  // Check if secondary entry
  if (open_files[indexDesc].mainPos != -1) {
    indexDesc = open_files[indexDesc].mainPos;
  }

  
  int hashID = (hash_string(record.index_key) >> (SHIFT_CONST - open_files[indexDesc].globalDepth));


  BF_Block *targetBlock;
  BF_Block_Init(&targetBlock);

  CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc,open_files[indexDesc].index[hashID],targetBlock));
  DataBlock *targetData = (DataBlock *)BF_Block_GetData(targetBlock);

  if(targetData->nextBlock!=-1){ //overflow
    //removed multiblock buckets to simplify the implementation of the secondery index
    printf("This should not be used in project 2");
    return HT_ERROR;
  }
  else/*if(targetData->nextBlock==-1)*/{  //only one block

    if (targetData->lastEmpty<DATA_ARRAY_SIZE){
      //insert
      targetData->index[targetData->lastEmpty].tupleId.block_num = record.tupleId.block_num;
      targetData->index[targetData->lastEmpty].tupleId.record_num = record.tupleId.record_num;
      strcpy(targetData->index[targetData->lastEmpty].index_key,record.index_key);      


      targetData->lastEmpty++;
      BF_Block_SetDirty(targetBlock);
      CALL_BF(BF_UnpinBlock(targetBlock));

      CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc, 0, targetBlock));
      StatBlock* statData = (StatBlock*) BF_Block_GetData(targetBlock);
      statData->total_recs++;
      BF_Block_SetDirty(targetBlock);
      CALL_BF(BF_UnpinBlock(targetBlock));

      BF_Block_Destroy(&targetBlock);

      return HT_OK;
    }
    else if(targetData->localDepth==MAX_DEPTH){    //reached MAX depth
      printf("Can't insert record: Block full and index at max depth");
      return HT_ERROR;
    }
    else{
      //split
      //making an array with all the entries of this block
      int entryAmount = 1+targetData->lastEmpty;
      SecondaryRecord *entryArray=malloc(entryAmount*sizeof(SecondaryRecord));  //1 for the new entry and all the entries of the block
      entryArray[0].tupleId.block_num = record.tupleId.block_num;
      entryArray[0].tupleId.record_num = record.tupleId.record_num;
      strcpy(entryArray[0].index_key,record.index_key);


      for (int i = 0; i < targetData->lastEmpty; i++){
        entryArray[i+1].tupleId.block_num  = targetData->index[i].tupleId.block_num;
        entryArray[i+1].tupleId.record_num = targetData->index[i].tupleId.record_num;
        strcpy(entryArray[i+1].index_key   , targetData->index[i].index_key);
      }
      CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc, 0, targetBlock));
      StatBlock* statData = (StatBlock*) BF_Block_GetData(targetBlock);
      statData->total_buckets++;
      statData->total_recs -= entryAmount - 1;
      BF_Block_SetDirty(targetBlock);
      CALL_BF(BF_UnpinBlock(targetBlock));

      if(open_files[indexDesc].globalDepth==targetData->localDepth){
        open_files[indexDesc].globalDepth++;

        int *newIndex = malloc((1<<open_files[indexDesc].globalDepth)*sizeof(int));
        for (int i=0,j=0;i<(1<<(open_files[indexDesc].globalDepth-1)) && j<(1<<(open_files[indexDesc].globalDepth));i++,j+=2){
          newIndex[j]=open_files[indexDesc].index[i];
          newIndex[j+1]=open_files[indexDesc].index[i];
        }
        //open_files[indexDesc].index[(2*hashID)+1] will have the same block as in the last index but it will be empty
        targetData->localDepth = open_files[indexDesc].globalDepth;
        targetData->lastEmpty = 0;
        targetData->nextBlock = -1;

        BF_Block_SetDirty(targetBlock);
        CALL_BF(BF_UnpinBlock(targetBlock));

        BF_Block *newBlock;
        BF_Block_Init(&newBlock);
        DataBlock *newBlockData;
        CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
        newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

        CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,&newIndex[(2*hashID)+1]));
        newIndex[(2*hashID)+1]--;
        newBlockData->localDepth = open_files[indexDesc].globalDepth;
        newBlockData->lastEmpty = 0;
        newBlockData->nextBlock = -1;

        BF_Block_SetDirty(newBlock);
        CALL_BF(BF_UnpinBlock(newBlock));
        BF_Block_Destroy(&newBlock);
        BF_Block_Destroy(&targetBlock);

        free(open_files[indexDesc].index);
        open_files[indexDesc].index=newIndex;
        for (int i=0;i<entryAmount;i++){
          HT_SeconderyInsertEntry(open_files[indexDesc].fileDesc,entryArray[i]);
        }
        free(entryArray);

        return HT_OK; 
      }
      else if(open_files[indexDesc].globalDepth>targetData->localDepth){
        int shift_amt = open_files[indexDesc].globalDepth - targetData->localDepth;
        int firstIDtoBlock=((hashID >> shift_amt) << shift_amt);
        
        int dataBlock;
        BF_Block *newBlock;
        BF_Block_Init(&newBlock);
        DataBlock *newBlockData;
        CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
        newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

        CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,&dataBlock));
        newBlockData->localDepth = targetData->localDepth+1;
        newBlockData->lastEmpty = 0;
        newBlockData->nextBlock = -1;
        
        CALL_BF(BF_UnpinBlock(targetBlock));
        for (int i=firstIDtoBlock;i<firstIDtoBlock+(1<<(shift_amt-1));i++){
          open_files[indexDesc].index[i]=dataBlock-1;
        }
        

        //the second half of the hashIDs will have the same block as in the last index but it will be empty
        targetData->localDepth++;
        targetData->lastEmpty = 0;
        targetData->nextBlock = -1;

        BF_Block_SetDirty(targetBlock);
        CALL_BF(BF_UnpinBlock(targetBlock));
        BF_Block_Destroy(&targetBlock);

        BF_Block_SetDirty(newBlock);
        CALL_BF(BF_UnpinBlock(newBlock));
        BF_Block_Destroy(&newBlock);

        for (int i=0;i<entryAmount;i++){
          HT_SecondaryInsertEntry(open_files[indexDesc].fileDesc,entryArray[i]);
        }
        free(entryArray);

        return HT_OK; 
      }
    }
  }

  return HT_OK;
}

// Statistical data to be stored in the first block of the file
typedef struct StatBlock {
  int total_recs;
  int total_buckets;
  int globalDepth;
  int attribType;  // 0 for city, 1 for surname
  char mainFileName[NAME_BUF];
} StatBlock;

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray) {
  BF_Block* block;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc, 0, block));
  StatBlock* stat = (StatBlock*) BF_Block_GetData(block);

  if (stat->attribType == 0) {
    CALL_BF(BF_UnpinBlock(block));
    for (int i = 0 ; i < PRIMARY_DATA_ARRAY_SIZE ; i++) {
      CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc, open_files[indexDesc].index[hash_string(updateArray->record[i].city)], block));
      DataBlock* stat = (DataBlock*) BF_Block_GetData(block);
      for (int j = 0 ; j < DATA_ARRAY_SIZE ; j++) {
        if ((stat->index[j].tupleId.block_num == updateArray->oldTuple[i].block_num)
         && (stat->index[j].tupleId.record_num == updateArray->oldTuple[i].record_num)) {
           stat->index[j].tupleId.block_num = updateArray->newTuple[i].block_num;
           stat->index[j].tupleId.record_num = updateArray->newTuple[i].record_num;
        }
      }
      CALL_BF(BF_UnpinBlock(block));
    }
  }

  if (stat->attribType == 1) {
    CALL_BF(BF_UnpinBlock(block));
    for (int i = 0 ; i < PRIMARY_DATA_ARRAY_SIZE ; i++) {
      CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc, open_files[indexDesc].index[hash_string(updateArray->record[i].surname)], block));
      DataBlock* stat = (DataBlock*) BF_Block_GetData(block);
      for (int j = 0 ; j < DATA_ARRAY_SIZE ; j++) {
        if ((stat->index[j].tupleId.block_num == updateArray->oldTuple[i].block_num)
         && (stat->index[j].tupleId.record_num == updateArray->oldTuple[i].record_num)) {
           stat->index[j].tupleId.block_num = updateArray->newTuple[i].block_num;
           stat->index[j].tupleId.record_num = updateArray->newTuple[i].record_num;
        }
      }
      CALL_BF(BF_UnpinBlock(block));
    }
  }
  return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index_key ) {
  // Check if indexDesc valid
  if ((sindexDesc < 0) || (sindexDesc >= MAX_OPEN_FILES) || (open_files[sindexDesc].fileDesc == -1)) {
    printf("Invalied indexDesc\n");
    return HT_ERROR;
  }

  // Check if secondary entry
  if (open_files[sindexDesc].mainPos != -1) {
    sindexDesc = open_files[sindexDesc].mainPos;
  }
  
  if (index_key==NULL){
    printf("Printing all entries\n");
    
    BF_Block *targetBlock;
    BF_Block_Init(&targetBlock);

    int previous_bucket = -1;
    int current_bucket = -1;

    BF_Block *Stat;
    CALL_BF(BF_GetBlock(open_files[sindexDesc].fileDesc,0,Stat));
    StatBlock *stats = (StatBlock *)BF_Block_GetData(Stat);

    int indexDesc;
    CALL_BF(BF_OpenFile(stats->mainFileName,&indexDesc))

    for (int i=0;i<(1<<open_files[sindexDesc].globalDepth);i++){
      current_bucket = open_files[sindexDesc].index[i];
      if (current_bucket == previous_bucket) {
        previous_bucket = current_bucket;
        continue;
      }
      previous_bucket = current_bucket;
      CALL_BF(BF_GetBlock(open_files[sindexDesc].fileDesc,open_files[sindexDesc].index[i],targetBlock));
      DataBlock *targetData = (DataBlock *)BF_Block_GetData(targetBlock);

      for (int j = 0; j < targetData->lastEmpty; j++){
        BF_Block *mainTargetBlock;
        CALL_BF(BF_GetBlock(indexDesc,targetData->index[j].tupleId.block_num,mainTargetBlock));
        PrimaryDataBlock *mainTargetData = (PrimaryDataBlock *)BF_Block_GetData(mainTargetBlock);

        int k = targetData->index[j].tupleId.record_num;
        printf("{%i,%s,%s,%s}\n", mainTargetData->index[k].id, mainTargetData->index[k].name, mainTargetData->index[k].surname, mainTargetData->index[k].city);
        CALL_BF(BF_UnpinBlock(mainTargetBlock));
      }
      
      CALL_BF(BF_UnpinBlock(targetBlock));
    }
    BF_Block_Destroy(&targetBlock);
  }
  else{

    BF_Block *Stat;
    CALL_BF(BF_GetBlock(open_files[sindexDesc].fileDesc,0,Stat));
    StatBlock *stats = (StatBlock *)BF_Block_GetData(Stat);

    int indexDesc;
    CALL_BF(BF_OpenFile(stats->mainFileName,&indexDesc))

    printf("Printing entries with ID: %s\n", index_key);
    int hashID = hash_string(index_key) >> (SHIFT_CONST - open_files[sindexDesc].globalDepth);
    BF_Block *targetBlock;
    BF_Block_Init(&targetBlock);
    CALL_BF(BF_GetBlock(open_files[sindexDesc].fileDesc,open_files[sindexDesc].index[hashID],targetBlock));
    DataBlock *targetData = (DataBlock *)BF_Block_GetData(targetBlock);

    for (int i = 0; i < targetData->lastEmpty; i++){
      if (strcmp(index_key,targetData->index[i].index_key)){
        BF_Block *mainTargetBlock;
        CALL_BF(BF_GetBlock(indexDesc,targetData->index[i].tupleId.block_num,mainTargetBlock));
        PrimaryDataBlock *mainTargetData = (PrimaryDataBlock *)BF_Block_GetData(mainTargetBlock);

        int k = targetData->index[i].tupleId.record_num;
        printf("{%i,%s,%s,%s}\n", mainTargetData->index[k].id, mainTargetData->index[k].name, mainTargetData->index[k].surname, mainTargetData->index[k].city);
        CALL_BF(BF_UnpinBlock(mainTargetBlock));
      }
    }
    
    CALL_BF(BF_UnpinBlock(targetBlock));
    BF_Block_Destroy(&targetBlock);
  }
  return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename) {
  //insert code here
  // Check if file is open
  int i;
  for (i = 0 ; i < MAX_OPEN_FILES ; i++) {
    if ((strcmp(open_files[i].filename, filename) == 0) && (open_files[i].mainPos == -1)) {
      break;
    }
  }
  int blockAmount;
  int bucketAmount;
  int recordAmount;
  double average_recs_per_bucket;
  int max_recs_per_bucket = 0; 
  int min_recs_per_bucket = INT_MAX;

  // If it is, scan it using the index in the memory
  if (i != MAX_OPEN_FILES) {
    CALL_BF(BF_GetBlockCounter(open_files[i].fileDesc, &blockAmount));
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(open_files[i].fileDesc, 0, block));
    StatBlock* stat = (StatBlock*) BF_Block_GetData(block);
    average_recs_per_bucket = stat->total_recs/ (double) stat->total_buckets;
    bucketAmount = stat->total_buckets;
    recordAmount = stat->total_recs;
    CALL_BF(BF_UnpinBlock(block));
    int indexSize = 1 << open_files[i].globalDepth;
    for (int j = 0 ; j < indexSize ; j++) {
      CALL_BF(BF_GetBlock(open_files[i].fileDesc, open_files[i].index[j], block));
      DataBlock* data = (DataBlock*) BF_Block_GetData(block);
      max_recs_per_bucket = (data->lastEmpty > max_recs_per_bucket) ? data->lastEmpty : max_recs_per_bucket;
      min_recs_per_bucket = (data->lastEmpty < min_recs_per_bucket) ? data->lastEmpty : min_recs_per_bucket;
      int nextBlock = data->nextBlock;
      CALL_BF(BF_UnpinBlock(block));
      while (nextBlock != -1) {
        CALL_BF(BF_GetBlock(open_files[i].fileDesc, nextBlock, block));
        DataBlock* data = (DataBlock*) BF_Block_GetData(block);
        max_recs_per_bucket = (data->lastEmpty > max_recs_per_bucket) ? data->lastEmpty : max_recs_per_bucket;
        min_recs_per_bucket = (data->lastEmpty < min_recs_per_bucket) ? data->lastEmpty : min_recs_per_bucket;
        nextBlock = data->nextBlock;
        CALL_BF(BF_UnpinBlock(block));
      }
    }
    BF_Block_Destroy(&block);
  }

  // Else, scan it using the index in the disk
  else {
    // Open file
    int fd;
    CALL_BF(BF_OpenFile(filename, &fd));
    // Scan it
    CALL_BF(BF_GetBlockCounter(fd, &blockAmount));
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fd, 0, block));
    StatBlock* stat = (StatBlock*) BF_Block_GetData(block);
    average_recs_per_bucket = stat->total_recs/ (double) stat->total_buckets;
    bucketAmount = stat->total_buckets;
    recordAmount = stat->total_recs;
    
    CALL_BF(BF_UnpinBlock(block));
    BF_Block* indexBlock;
    BF_Block_Init(&indexBlock);
    CALL_BF(BF_GetBlock(fd, 1, indexBlock));
    IndexBlock* index = (IndexBlock*) BF_Block_GetData(indexBlock);
    int nextIndexBlock;
    do {
      for (int j = 0 ; (j < INDEX_ARRAY_SIZE) && (index->index[j] != -1); j++) {
        CALL_BF(BF_GetBlock(fd, index->index[j], block));
        DataBlock* data = (DataBlock*) BF_Block_GetData(block);
        max_recs_per_bucket = (data->lastEmpty > max_recs_per_bucket) ? data->lastEmpty : max_recs_per_bucket;
        min_recs_per_bucket = (data->lastEmpty < min_recs_per_bucket) ? data->lastEmpty : min_recs_per_bucket;
        int nextBlock = data->nextBlock;
        CALL_BF(BF_UnpinBlock(block));
        while (nextBlock != -1) {
          CALL_BF(BF_GetBlock(fd, nextBlock, block));
          DataBlock* data = (DataBlock*) BF_Block_GetData(block);
          max_recs_per_bucket = (data->lastEmpty > max_recs_per_bucket) ? data->lastEmpty : max_recs_per_bucket;
          min_recs_per_bucket = (data->lastEmpty < min_recs_per_bucket) ? data->lastEmpty : min_recs_per_bucket;
          nextBlock = data->nextBlock;
          CALL_BF(BF_UnpinBlock(block));
        }
      }
      nextIndexBlock = index->nextBlock;
      CALL_BF(BF_UnpinBlock(indexBlock));
      if (nextIndexBlock != -1) {
        CALL_BF(BF_GetBlock(fd, nextIndexBlock, indexBlock));
        index = (IndexBlock*) BF_Block_GetData(indexBlock);
      }
    } while (nextIndexBlock != -1);

    BF_Block_Destroy(&indexBlock);
    BF_Block_Destroy(&block);
    // Close file
    CALL_BF(BF_CloseFile(fd));
  }

  // Print the results
  printf("Block amount: %d\n", blockAmount);
  printf("Minimum records per bucket: %d\n", min_recs_per_bucket);
  printf("Average records per bucket: %f\n", average_recs_per_bucket);
  printf("Maximum records per bucket: %d\n", max_recs_per_bucket);
  return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2, char *index_key) {
  // Check if indexDesc valid
  if (((sindexDesc1 < 0) || (sindexDesc1 >= MAX_OPEN_FILES) || (open_files[sindexDesc1].fileDesc == -1))
  ||  ((sindexDesc2 < 0) || (sindexDesc2 >= MAX_OPEN_FILES) || (open_files[sindexDesc2].fileDesc == -1))) {
    printf("Invalied indexDesc\n");
    return HT_ERROR;
  }

  // Check if secondary entry
  if (open_files[sindexDesc1].mainPos != -1) {
    sindexDesc1 = open_files[sindexDesc1].mainPos;
  }

  // Check if secondary entry
  if (open_files[sindexDesc2].mainPos != -1) {
    sindexDesc2 = open_files[sindexDesc2].mainPos;
  }

  // Check if same attribute
  BF_Block* block;
  BF_Block_Init(&block);
  BF_Block* block2;
  BF_Block_Init(&block2);

  CALL_BF(BF_GetBlock(open_files[sindexDesc1].fileDesc, 0, block));
  StatBlock* stat1 = (StatBlock*) BF_Block_GetData(block);
  CALL_BF(BF_GetBlock(open_files[sindexDesc2].fileDesc, 0, block2));
  StatBlock* stat2 = (StatBlock*) BF_Block_GetData(block2);
  if (stat1->attribType != stat2->attribType) {
    printf("Incompatible attribute types\n");
    CALL_BF(BF_UnpinBlock(block));
    CALL_BF(BF_UnpinBlock(block2));
    BF_Block_Destroy(&block);
    BF_Block_Destroy(&block2);
    return HT_ERROR;
  }

  // Get main files
  int mainFileDesc1;
  int mainFileDesc2;
  CALL_BF(BF_OpenFile(stat1->mainFileName, &mainFileDesc1));
  CALL_BF(BF_OpenFile(stat2->mainFileName, &mainFileDesc2));

  CALL_BF(BF_UnpinBlock(block));
  CALL_BF(BF_UnpinBlock(block2));

  // Join
  BF_Block* block3;
  BF_Block_Init(&block3);

  int indexSize = 1 << open_files[sindexDesc1].globalDepth;
  for (int i = 0 ; i < indexSize ; i++) {
    CALL_BF(BF_GetBlock(open_files[sindexDesc1].fileDesc, open_files[sindexDesc1].index[i], block));
    DataBlock* data = (DataBlock*) BF_Block_GetData(block);
    for (int j = 0 ; j < data->lastEmpty ; j++) {
      CALL_BF(BF_GetBlock(open_files[sindexDesc2].fileDesc, open_files[sindexDesc2].index[hash_string(data->index[j].index_key)], block2));
      DataBlock* data2 = (DataBlock*) BF_Block_GetData(block2);
      for (int k = 0 ; k < data2->lastEmpty ; k++) {
        if ((!strcmp(data->index[j].index_key, data2->index[k].index_key)) && ((index_key == NULL) || (!strcmp(data->index[j].index_key, index_key)))) {
          CALL_BF(BF_GetBlock(mainFileDesc1, data->index[j].tupleId.block_num, block3));
          PrimaryDataBlock* data3 = (PrimaryDataBlock*) BF_Block_GetData(block3);
          Record rec = data3->index[data->index[j].tupleId.record_num];
          printf("{%i,%s,%s,%s} - ", rec.id, rec.name, rec.surname, rec.city);
          CALL_BF(BF_UnpinBlock(block3));
          CALL_BF(BF_GetBlock(mainFileDesc2, data2->index[k].tupleId.block_num, block3));
          data3 = (PrimaryDataBlock*) BF_Block_GetData(block3);
          rec = data3->index[data2->index[k].tupleId.record_num];
          printf("{%i,%s,%s,%s}\n", rec.id, rec.name, rec.surname, rec.city);
          CALL_BF(BF_UnpinBlock(block3));
        }
      }
      CALL_BF(BF_UnpinBlock(block2));
    }
    CALL_BF(BF_UnpinBlock(block));
  }
  BF_Block_Destroy(&block);
  BF_Block_Destroy(&block2);
  BF_Block_Destroy(&block3);

  return HT_OK;
}