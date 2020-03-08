/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_VNODEQUERYIO_H
#define TDENGINE_VNODEQUERYIO_H

#include "os.h"
#include "tarray.h"

enum {
  DISK_BLOCK_NO_NEED_TO_LOAD = 0,
  DISK_BLOCK_LOAD_TS = 1,
  DISK_BLOCK_LOAD_BLOCK = 2,
};

enum {
  QUERY_RANGE_LESS_EQUAL = 0,
  QUERY_RANGE_GREATER_EQUAL = 1,
};

typedef struct SPointInterpoSupporter {
  int32_t numOfCols;
  char ** pPrevPoint;
  char ** pNextPoint;
} SPointInterpoSupporter;

typedef int (*__block_search_fn_t)(char *data, int num, int64_t key, int order);

#define IS_DISK_DATA_BLOCK(q) ((q)->fileId >= 0)

char *getDataBlocks(SQueryRuntimeEnv *pRuntimeEnv, SArithmeticSupport *sas, int32_t col, int32_t size);

char *doGetDataBlocks(SQuery *pQuery, SData **data, int32_t colIdx);
void  doCloseQueryFiles(SQueryFilesInfo *pVnodeFileInfo);

void    initQueryFileInfoFD(SQueryFilesInfo *pVnodeFilesInfo);
void    vnodeInitDataBlockLoadInfo(SDataBlockLoadInfo *pBlockLoadInfo);
int64_t getOldestKey(int32_t numOfFiles, int64_t fileId, SVnodeCfg *pCfg);
void    vnodeFreeFieldsEx(SQueryRuntimeEnv *pRuntimeEnv);

int32_t validateHeaderOffsetSegment(SQInfo *pQInfo, char *filePath, int32_t vid, char *data, int32_t size);

int32_t getCompHeaderStartPosition(SVnodeCfg *pCfg);

int32_t validateCompBlockOffset(SQInfo *pQInfo, SMeterObj *pMeterObj, SCompHeader *pCompHeader,
                                SQueryFilesInfo *pQueryFileInfo, int32_t headerSize);

int32_t vnodeIsDatablockLoaded(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t fileIndex, bool loadTS);

int32_t getCompHeaderSegSize(SVnodeCfg *pCfg);

int32_t binarySearchForBlockImpl(SCompBlock *pBlock, int32_t numOfBlocks, TSKEY skey, int32_t order);

void vnodeRecordAllFiles(SQInfo *pQInfo, int32_t vnodeId);

bool getQualifiedDataBlock(SMeterObj *pMeterObj, SQueryRuntimeEnv *pRuntimeEnv, int32_t type,
                           __block_search_fn_t searchFn);

bool getNeighborPoints(STableQuerySupportObj *pSupporter, SMeterObj *pMeterObj,
                       SPointInterpoSupporter *pPointInterpSupporter);

void vnodeCheckIfDataExists(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, bool *dataInDisk, bool *dataInCache);

int64_t loadRequiredBlockIntoMem(SQueryRuntimeEnv *pRuntimeEnv, SPositionInfo *position);

void *getGenericDataBlock(SMeterObj *pMeterObj, SQueryRuntimeEnv *pRuntimeEnv, int32_t slot);

bool isIntervalQuery(SQuery *pQuery);

int32_t moveToNextBlock(SQueryRuntimeEnv *pRuntimeEnv, int32_t step, __block_search_fn_t searchFn, bool loadData);

void vnodeInitCompBlockLoadInfo(SLoadCompBlockInfo *pCompBlockLoadInfo);

void getBasicCacheInfoSnapshot(SQuery *pQuery, SCacheInfo *pCacheInfo, int32_t vid);

TSKEY getQueryPositionForCacheInvalid(SQueryRuntimeEnv *pRuntimeEnv, __block_search_fn_t searchFn);

TSKEY getTimestampInDiskBlock(SQueryRuntimeEnv *pRuntimeEnv, int32_t index);
TSKEY getTimestampInCacheBlock(SQueryRuntimeEnv *pRuntimeEnv, SCacheBlock *pBlock, int32_t index);
void  savePointPosition(SPositionInfo *position, int32_t fileId, int32_t slot, int32_t pos);

////////////////////////////////////////////////////////////////////////////////////////////////
// the following are new IO api in ver 2.0
// query condition to build vnode iterator
typedef struct STsdbQueryCond {
  STimeWindow   window;
  int32_t       order;  // desc/asc order to iterate the data block
  SColumnInfoEx colList;
} STsdbQueryCond;

typedef struct STableId {
  int32_t sid;
  int64_t uid;
} STableId;

typedef struct SQueryHandlePos {
  int32_t fileId;
  int32_t slot;
  int32_t pos;
  int32_t fileIndex;
} SQueryHandlePos;

typedef struct STsdbQueryHandle {
  SQueryHandlePos cur;   // current position
  SQueryHandlePos start; /* the start position, used for secondary/third iteration */
  SQueryHandlePos end;   /* the last access position in query, served as the start pos of reversed order query */

  uint8_t blockStatus;  // Indicate if data block is loaded, the block is first/last/internal block
  int32_t unzipBufSize;

  SData *tsBuf;   //primary time stamp columns
  char * unzipBuffer;
  char * secondaryUnzipBuffer;

  SDataBlockLoadInfo_ dataBlockLoadInfo; /* record current block load information */
  SLoadCompBlockInfo compBlockLoadInfo; /* record current compblock information in SQuery */

  SQueryFilesInfo_rv vnodeFileInfo;
  int16_t            numOfRowsPerPage;
  uint16_t           flag;  // denotes reversed scan of data or not
  int16_t            order;
  int16_t            traverseOrder;
  STimeWindow        window;  // the primary query time window that applies to all queries
  TSKEY              lastKey;
  int32_t            blockBufferSize;
  SCompBlock *       pBlock;
  int32_t            numOfBlocks;
  SField **          pFields;
  SArray *           pColumns;  // column list, SColumnInfoEx_ array list
  SArray *           pTableList;  // table object list
  bool               locateStart;

  int32_t     realNumOfRows;
  bool        loadDataAfterSeek;// load data after seek.
  
  int32_t     currentSlot;
  int32_t     numOfCacheBlocks;
  int32_t     firstSlot;
  int32_t     commitSlot;
  int32_t     commitPoint;
  int32_t     blockId;
  SCacheBlock cacheBlock;
  bool        cacheBlockLoaded;
} STsdbQueryHandle;

typedef struct SDataBlockInfo {
  STimeWindow window;

  int32_t size;
  int32_t numOfCols;

  int64_t uid;
  int32_t sid;
} SDataBlockInfo;

typedef struct SDataStatis {
  int16_t colId;
  int64_t sum;
  int64_t max;
  int64_t min;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNullPoints;
} SDataStatis;

typedef void* tsdbPos_t;

/**
 * Get the data block iterator, starting from position according to the query condition
 * @param pCond  query condition, only includes the filter on primary time stamp
 * @param pTableList    table sid list
 * @return
 */
STsdbQueryHandle *tsdbQueryByTableId(STsdbQueryCond *pCond, SArray *idList, SArray *pColumnInfo);

/**
 * move to next block
 * @param pQueryHandle
 * @return
 */
bool tsdbNextDataBlock(STsdbQueryHandle *pQueryHandle);

/**
 * Get current data block information
 *
 * @param pQueryHandle
 * @return
 */
SDataBlockInfo tsdbRetrieveDataBlockInfo(STsdbQueryHandle *pQueryHandle);

/**
 *
 * Get the pre-calculated information w.r.t. current data block.
 *
 * In case of data block in cache, the pBlockStatis will always be NULL.
 * If a block is not completed loaded from disk, the pBlockStatis will be NULL.

 * @pBlockStatis the pre-calculated value for current data blocks. if the block is a cache block, always return 0
 * @return
 */
int32_t tsdbRetrieveDataBlockStatisInfo(STsdbQueryHandle *pQueryHandle, SDataStatis **pBlockStatis);

/**
 * The query condition with primary timestamp is passed to iterator during its constructor function,
 * the returned data block must be satisfied with the time window condition in any cases,
 * which means the SData data block is not actually the completed disk data blocks.
 *
 * @param pQueryHandle
 * @return
 */
SArray* tsdbRetrieveDataBlock(STsdbQueryHandle *pQueryHandle, SArray *pIdList);

/**
 *  Reset to the start(end) position of current query, from which the iterator starts.
 *
 * @param pQueryHandle
 * @param position  set the iterator traverses position
 * @param order ascending order or descending order
 * @return
 */
int32_t tsdbResetQuery(STsdbQueryHandle *pQueryHandle, void* position, int16_t order);

/**
 * return the access position of current query handle
 * @param pQueryHandle
 * @return
 */
int32_t tsdbDataBlockSeek(STsdbQueryHandle *pQueryHandle, tsdbPos_t pos);

/**
 *
 * @param pQueryHandle
 * @return
 */
tsdbPos_t tsdbDataBlockTell(STsdbQueryHandle* pQueryHandle);


char *getDataBlocks_(SQueryRuntimeEnv *pRuntimeEnv, SArithmeticSupport *sas, int32_t col, int32_t size, SArray* pDataBlock);

#endif  // TDENGINE_VNODEQUERYIO_H
