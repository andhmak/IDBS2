#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_file.h"

#define INDEX_ARRAY_SIZE ((BF_BLOCK_SIZE-sizeof(int))/sizeof(int))      // Amount of buckets per block of index
#define DATA_ARRAY_SIZE ((BF_BLOCK_SIZE-3*sizeof(int))/sizeof(SecondaryRecord))  // Amount of records per bucket

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
  Record index[DATA_ARRAY_SIZE];
} DataBlock;

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
  return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc,SecondaryRecord record) {
  // Check if indexDesc valid
  if ((indexDesc < 0) || (indexDesc >= MAX_OPEN_FILES) || (open_files[indexDesc].fileDesc == -1)) {
    printf("Invalied indexDesc\n");
    return HT_ERROR;
  }

  // Check if secondary entry
  if (open_files[indexDesc].mainPos != -1) {
    indexDesc = open_files[indexDesc].mainPos;
  }

  
  int hashID = ((hash_func(record.id) & INT_MAX) >> (SHIFT_CONST - open_files[indexDesc].globalDepth));


  BF_Block *targetBlock;
  BF_Block_Init(&targetBlock);

  CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc,open_files[indexDesc].index[hashID],targetBlock));
  DataBlock *targetData = (DataBlock *)BF_Block_GetData(targetBlock);

  if(targetData->nextBlock!=-1){ //overflow
    //removed multiblock buckets to simplify the implementation of the secondery index
    printf("This should not be used in project 2");
    return HT_ERROR;

    /*int next = targetData->nextBlock;
    while(next != -1) {
      CALL_BF(BF_UnpinBlock(targetBlock));
      CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc, next, targetBlock));
      targetData = (DataBlock*) BF_Block_GetData(targetBlock);
      next = targetData->nextBlock;
    }    

    if (targetData->lastEmpty<DATA_ARRAY_SIZE){ //last block has space
      //insert
      targetData->index[targetData->lastEmpty].id = record.id;
      strcpy(targetData->index[targetData->lastEmpty].name,record.name);
      strcpy(targetData->index[targetData->lastEmpty].surname,record.surname);
      strcpy(targetData->index[targetData->lastEmpty].city,record.city);
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
    else{
      //make next block
      BF_Block *newBlock;
      BF_Block_Init(&newBlock);
      DataBlock *newBlockData;
      CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
      newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

      CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,&(targetData->nextBlock)));
      targetData->nextBlock--;
      newBlockData->localDepth = targetData->localDepth;
      newBlockData->index[0].id = record.id;
      strcpy(newBlockData->index[0].name,record.name);
      strcpy(newBlockData->index[0].surname,record.surname);
      strcpy(newBlockData->index[0].city,record.city);
      newBlockData->lastEmpty = 1;
      newBlockData->nextBlock = -1;

      BF_Block_SetDirty(targetBlock);
      BF_Block_SetDirty(newBlock);
      CALL_BF(BF_UnpinBlock(targetBlock));
      CALL_BF(BF_UnpinBlock(newBlock));

      CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc, 0, targetBlock));
      StatBlock* statData = (StatBlock*) BF_Block_GetData(targetBlock);
      statData->total_recs++;
      statData->total_buckets++;
      BF_Block_SetDirty(targetBlock);
      CALL_BF(BF_UnpinBlock(targetBlock));

      BF_Block_Destroy(&targetBlock);
      BF_Block_Destroy(&newBlock);

      return HT_OK;
    }
    */
  }
  else/*if(targetData->nextBlock==-1)*/{  //only one block

    if (targetData->lastEmpty<DATA_ARRAY_SIZE){
      //insert
      targetData->index[targetData->lastEmpty].id = record.id;
      strcpy(targetData->index[targetData->lastEmpty].name,record.name);
      strcpy(targetData->index[targetData->lastEmpty].surname,record.surname);
      strcpy(targetData->index[targetData->lastEmpty].city,record.city);

      tTuple *record_pos;
      record_pos->block_num = open_files[indexDesc].index[hashID];
      record_pos->record_num = targetData->lastEmpty;
      tupleId = (int)record_pos;

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
      /*//make next block
      BF_Block *newBlock;
      BF_Block_Init(&newBlock);
      DataBlock *newBlockData;
      CALL_BF(BF_AllocateBlock(open_files[indexDesc].fileDesc,newBlock));
      newBlockData = (DataBlock *)BF_Block_GetData(newBlock);

      CALL_BF(BF_GetBlockCounter(open_files[indexDesc].fileDesc,&(targetData->nextBlock)));
      targetData->nextBlock--;
      newBlockData->localDepth = targetData->localDepth;
      newBlockData->index[0].id = record.id;
      strcpy(newBlockData->index[0].name,record.name);
      strcpy(newBlockData->index[0].surname,record.surname);
      strcpy(newBlockData->index[0].city,record.city);
      newBlockData->lastEmpty = 1;
      newBlockData->nextBlock = -1;

      BF_Block_SetDirty(targetBlock);
      BF_Block_SetDirty(newBlock);
      CALL_BF(BF_UnpinBlock(targetBlock));
      CALL_BF(BF_UnpinBlock(newBlock));

      CALL_BF(BF_GetBlock(open_files[indexDesc].fileDesc, 0, targetBlock));
      StatBlock* statData = (StatBlock*) BF_Block_GetData(targetBlock);
      statData->total_recs++;
      statData->total_buckets++;
      BF_Block_SetDirty(targetBlock);
      CALL_BF(BF_UnpinBlock(targetBlock));

      BF_Block_Destroy(&targetBlock);
      BF_Block_Destroy(&newBlock);

      return HT_OK;*/
    }
    else{
      //split
      //making an array with all the entries of this block
      int entryAmount = 1+targetData->lastEmpty;
      Record *entryArray=malloc(entryAmount*sizeof(Record));  //1 for the new entry and all the entries of the block
      updateArray = malloc(entryAmount*sizeof(UpdateRecordArray));
      entryArray[0].id = record.id;
      strcpy(entryArray[0].name,record.name);
      strcpy(entryArray[0].surname,record.surname);
      strcpy(entryArray[0].city,record.city);

      updateArray[0].record = record;
      updateArray[0].oldTuple = NULL;

      for (int i = 0; i < targetData->lastEmpty; i++){
        entryArray[i+1].id = targetData->index[i].id;
        strcpy(entryArray[i+1].name,targetData->index[i].name);
        strcpy(entryArray[i+1].surname,targetData->index[i].surname);
        strcpy(entryArray[i+1].city,targetData->index[i].city);

        updateArray[i+1].record.id = targetData->index[i].id;
        strcpy(updateArray[i+1].record.name,targetData->index[i].name);
        strcpy(updateArray[i+1].record.surname,targetData->index[i].surname);
        strcpy(updateArray[i+1].record.city,targetData->index[i].city);

        updateArray[i+1].oldTuple->block_num = open_files[indexDesc].index[hashID];
        updateArray[i+1].oldTuple->record_num = i;
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
          int temp_tuple;
          HT_InsertEntry(open_files[indexDesc].fileDesc,entryArray[i],temp_tuple,updateArray);
          updateArray[i].newTuple = (tTuple *)temp_tuple;
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
          int temp_tuple;
          HT_InsertEntry(open_files[indexDesc].fileDesc,entryArray[i],temp_tuple,updateArray);
          updateArray[i].newTuple = (tTuple *)temp_tuple;
        }
        free(entryArray);

        return HT_OK; 
      }
    }
  }

  return HT_OK;
}

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index-key ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2,  char *index-key ) {
  //insert code here
  return HT_OK;
}