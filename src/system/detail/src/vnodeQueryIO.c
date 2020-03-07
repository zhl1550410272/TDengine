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

#include "hash.h"
#include "hashutil.h"
#include "os.h"
#include "taosmsg.h"
#include "textbuffer.h"
#include "ttime.h"

#include "tinterpolation.h"
#include "tscJoinProcess.h"
#include "tscSecondaryMerge.h"
#include "tscompression.h"
#include "ttime.h"
#include "vnode.h"
#include "vnodeRead.h"
#include "vnodeUtil.h"

#include "vnodeCache.h"
#include "vnodeDataFilterFunc.h"
#include "vnodeFile.h"
#include "vnodeQueryIO.h"
#include "vnodeQueryImpl.h"
#include "vnodeStatus.h"

#define PRIMARY_TSCOL_REQUIRED(c) (((SColumnInfoEx *)taosArrayGet(c, 0))->data.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX)
#define QUERY_IS_ASC_QUERY_RV(o) (o == TSQL_SO_ASC)
#define QH_GET_NUM_OF_COLS(handle) (taosArrayGetSize((handle)->pColumns))

// check the offset value integrity
int32_t validateHeaderOffsetSegment(SQInfo *pQInfo, char *filePath, int32_t vid, char *data, int32_t size) {
  if (!taosCheckChecksumWhole((uint8_t *)data + TSDB_FILE_HEADER_LEN, size)) {
    dLError("QInfo:%p vid:%d, failed to read header file:%s, file offset area is broken", pQInfo, vid, filePath);
    return -1;
  }
  return 0;
}

int32_t validateHeaderOffsetSegment_(char *filePath, int32_t vid, char *data, int32_t size) {
  if (!taosCheckChecksumWhole((uint8_t *)data + TSDB_FILE_HEADER_LEN, size)) {
    //    dLError("QInfo:%p vid:%d, failed to read header file:%s, file offset area is broken", pQInfo, vid, filePath);
    return -1;
  }
  return 0;
}

int32_t getCompHeaderSegSize(SVnodeCfg *pCfg) { return pCfg->maxSessions * sizeof(SCompHeader) + sizeof(TSCKSUM); }

int32_t getCompHeaderStartPosition(SVnodeCfg *pCfg) { return TSDB_FILE_HEADER_LEN + getCompHeaderSegSize(pCfg); }

int32_t validateCompBlockOffset(SQInfo *pQInfo, SMeterObj *pMeterObj, SCompHeader *pCompHeader,
                                SQueryFilesInfo *pQueryFileInfo, int32_t headerSize) {
  if (pCompHeader->compInfoOffset < headerSize || pCompHeader->compInfoOffset > pQueryFileInfo->headerFileSize) {
    dError("QInfo:%p vid:%d sid:%d id:%s, compInfoOffset:%" PRId64 " is not valid, size:%" PRId64, pQInfo,
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pCompHeader->compInfoOffset,
           pQueryFileInfo->headerFileSize);

    return -1;
  }

  return 0;
}

int32_t validateCompBlockOffset_(SMeterObj *pMeterObj, SCompHeader *pCompHeader, SQueryFilesInfo *pQueryFileInfo,
                                 int32_t headerSize) {
  if (pCompHeader->compInfoOffset < headerSize || pCompHeader->compInfoOffset > pQueryFileInfo->headerFileSize) {
    //    dError("QInfo:%p vid:%d sid:%d id:%s, compInfoOffset:%" PRId64 " is not valid, size:%" PRId64, pQInfo,
    //           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pCompHeader->compInfoOffset,
    //           pQueryFileInfo->headerFileSize);

    return -1;
  }

  return 0;
}

// check compinfo integrity
static FORCE_INLINE int32_t validateCompBlockInfoSegment(SQInfo *pQInfo, const char *filePath, int32_t vid,
                                                         SCompInfo *compInfo, int64_t offset) {
  if (!taosCheckChecksumWhole((uint8_t *)compInfo, sizeof(SCompInfo))) {
    dLError("QInfo:%p vid:%d, failed to read header file:%s, file compInfo broken, offset:%" PRId64, pQInfo, vid,
            filePath, offset);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t validateCompBlockInfoSegment_(const char *filePath, int32_t vid, SCompInfo *compInfo,
                                                          int64_t offset) {
  if (!taosCheckChecksumWhole((uint8_t *)compInfo, sizeof(SCompInfo))) {
    //    dLError("QInfo:%p vid:%d, failed to read header file:%s, file compInfo broken, offset:%" PRId64, pQInfo, vid,
    //            filePath, offset);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t validateCompBlockSegment(SQInfo *pQInfo, const char *filePath, SCompInfo *compInfo,
                                                     char *pBlock, int32_t vid, TSCKSUM checksum) {
  uint32_t size = compInfo->numOfBlocks * sizeof(SCompBlock);

  if (checksum != taosCalcChecksum(0, (uint8_t *)pBlock, size)) {
    dLError("QInfo:%p vid:%d, failed to read header file:%s, file compblock is broken:%zu", pQInfo, vid, filePath,
            (char *)compInfo + sizeof(SCompInfo));
    return -1;
  }

  return 0;
}

static FORCE_INLINE int32_t validateCompBlockSegment_(const char *filePath, SCompInfo *compInfo, char *pBlock,
                                                      int32_t vid, TSCKSUM checksum) {
  uint32_t size = compInfo->numOfBlocks * sizeof(SCompBlock);

  if (checksum != taosCalcChecksum(0, (uint8_t *)pBlock, size)) {
    //    dLError("QInfo:%p vid:%d, failed to read header file:%s, file compblock is broken:%zu", pQInfo, vid, filePath,
    //            (char *)compInfo + sizeof(SCompInfo));
    return -1;
  }

  return 0;
}

/**
 * if the header is smaller than a threshold value(header size + initial offset value)
 *
 * @param vnodeId
 * @param headerFileSize
 * @return
 */
static FORCE_INLINE bool isHeaderFileEmpty(int32_t vnodeId, size_t headerFileSize) {
  SVnodeCfg *pVnodeCfg = &vnodeList[vnodeId].cfg;
  return headerFileSize <= getCompHeaderStartPosition(pVnodeCfg);
}

/**
 * open a data files and header file for metric meta query
 *
 * @param pVnodeFiles
 * @param fid
 * @param index
 */
static FORCE_INLINE void vnodeStoreFileId(SQueryFilesInfo *pVnodeFiles, int32_t fid, int32_t index) {
  pVnodeFiles->pFileInfo[index].fileID = fid;
}

static bool checkIsHeaderFileEmpty(SQueryFilesInfo *pVnodeFilesInfo) {
  struct stat fstat = {0};
  if (stat(pVnodeFilesInfo->headerFilePath, &fstat) < 0) {
    return true;
  }

  pVnodeFilesInfo->headerFileSize = fstat.st_size;
  return isHeaderFileEmpty(pVnodeFilesInfo->vnodeId, pVnodeFilesInfo->headerFileSize);
}

static bool checkIsHeaderFileEmpty_(SQueryFilesInfo_rv *pVnodeFilesInfo) {
  struct stat fstat = {0};
  if (stat(pVnodeFilesInfo->headerFilePath, &fstat) < 0) {
    return true;
  }

  pVnodeFilesInfo->headerFileSize = fstat.st_size;
  return isHeaderFileEmpty(pVnodeFilesInfo->vnodeId, pVnodeFilesInfo->headerFileSize);
}

static int32_t vnodeGetVnodeHeaderFileIndex_(int32_t *fid, int32_t order, SQueryFilesInfo_rv *pVnodeFiles);

static void    doCloseQueryFileInfoFD(SQueryFilesInfo *pVnodeFilesInfo);
static void    vnodeSetCurrentFileNames(SQueryFilesInfo *pVnodeFilesInfo);
static int32_t doOpenQueryFile(SQInfo *pQInfo, SQueryFilesInfo *pVnodeFileInfo);
static bool    vnodeIsCompBlockInfoLoaded(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t fileIndex);
static int32_t moveToNextBlockInCache(SQueryRuntimeEnv *pRuntimeEnv, int32_t step, __block_search_fn_t searchFn);
static int32_t loadDataBlockIntoMem(SCompBlock *pBlock, SField **pField, SQueryRuntimeEnv *pRuntimeEnv, int32_t fileIdx,
                                    bool loadPrimaryCol, bool loadSField);

static bool getValidDataBlocksRangeIndex(SMeterDataInfo *pMeterDataInfo, SQuery *pQuery, SCompBlock *pCompBlock,
                                         int64_t numOfBlocks, TSKEY minval, TSKEY maxval, int32_t *end);

static bool setCurrentQueryRange(SMeterDataInfo *pMeterDataInfo, SQuery *pQuery, TSKEY endKey, TSKEY *minval,
                                 TSKEY *maxval);

static SCompBlock *getDiskDataBlock(SQuery *pQuery, int32_t slot);
static int32_t     binarySearchInCacheBlk(SCacheInfo *pCacheInfo, SQuery *pQuery, int32_t keyLen, int32_t firstSlot,
                                          int32_t lastSlot);
static bool        isCacheBlockValid(SQuery *pQuery, SCacheBlock *pBlock, SMeterObj *pMeterObj, int32_t slot);

static void setTimestampRange(SQueryRuntimeEnv *pRuntimeEnv, int64_t stime, int64_t etime);

static int32_t getNextDataFileCompInfo(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t step);

// static int32_t doOpenQueryFile_(SQueryFilesInfo *pVnodeFileInfo);
static bool vnodeIsCompBlockInfoLoaded_(STsdbQueryHandle *pQueryHandle);

static int32_t doOpenQueryFile_(SQueryFilesInfo_rv *pVnodeFileInfo);

static void vnodeSetCurrentFileNames_(SQueryFilesInfo_rv *pVnodeFilesInfo);

static void doCloseQueryFiles_(SQueryFilesInfo_rv *pVnodeFileInfo);

static SCacheBlock *getCacheDataBlock_(STsdbQueryHandle *pQueryHandle, SMeterObj *pMeterObj, int32_t slot);

static int32_t binarySearchInCacheBlk_(SCacheInfo *pCacheInfo, int32_t order, TSKEY skey, int32_t keyLen,
                                       int32_t firstSlot, int32_t lastSlot);

static SCompBlock *getDiskDataBlock_(STsdbQueryHandle *pQueryHandle, int32_t slot);

static int32_t loadDataBlockIntoMem_(STsdbQueryHandle *pQueryHandle, SCompBlock *pBlock, SField **pField,
                                     int32_t fileIdx, SArray *pColIdList);

static int32_t getNextDataFileCompInfo_(STsdbQueryHandle *pQueryHandle, SQueryHandlePos *pCur,
                                        SQueryFilesInfo_rv *pVnodeFilesInfo, int32_t step);

static TSKEY getFirstDataBlockInCache_(STsdbQueryHandle *pQueryHandle);

static SArray *getDefaultLoadColumns(STsdbQueryHandle *pQueryHandle, bool loadTS);

/**
 * save triple tuple of (fileId, slot, pos) to SPositionInfo
 */
void savePointPosition(SPositionInfo *position, int32_t fileId, int32_t slot, int32_t pos) {
  /*
   * slot == -1 && pos == -1 means no data left anymore
   */
  assert(fileId >= -1 && slot >= -1 && pos >= -1);

  position->fileId = fileId;
  position->slot = slot;
  position->pos = pos;
}

static void getQueryRange(SQuery *pQuery, TSKEY *min, TSKEY *max) {
  *min = pQuery->lastKey < pQuery->ekey ? pQuery->lastKey : pQuery->ekey;
  *max = pQuery->lastKey >= pQuery->ekey ? pQuery->lastKey : pQuery->ekey;
}

int32_t binarySearchForBlockImpl(SCompBlock *pBlock, int32_t numOfBlocks, TSKEY skey, int32_t order) {
  int32_t firstSlot = 0;
  int32_t lastSlot = numOfBlocks - 1;

  int32_t midSlot = firstSlot;

  while (1) {
    numOfBlocks = lastSlot - firstSlot + 1;
    midSlot = (firstSlot + (numOfBlocks >> 1));

    if (numOfBlocks == 1) break;

    if (skey > pBlock[midSlot].keyLast) {
      if (numOfBlocks == 2) break;
      if ((order == TSQL_SO_DESC) && (skey < pBlock[midSlot + 1].keyFirst)) break;
      firstSlot = midSlot + 1;
    } else if (skey < pBlock[midSlot].keyFirst) {
      if ((order == TSQL_SO_ASC) && (skey > pBlock[midSlot - 1].keyLast)) break;
      lastSlot = midSlot - 1;
    } else {
      break;  // got the slot
    }
  }

  return midSlot;
}

static int32_t binarySearchForBlock(SQuery *pQuery, int64_t key) {
  return binarySearchForBlockImpl(pQuery->pBlock, pQuery->numOfBlocks, key, pQuery->order.order);
}

static int32_t binarySearchForBlock_(SCompBlock *pBlock, int32_t numOfBlocks, int32_t order, int64_t key) {
  return binarySearchForBlockImpl(pBlock, numOfBlocks, key, order);
}

static void getOneRowFromDataBlock(SQueryRuntimeEnv *pRuntimeEnv, char **dst, int32_t pos) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    int32_t bytes = pQuery->colList[i].data.bytes;
    memcpy(dst[i], pRuntimeEnv->colDataBuffer[i]->data + pos * bytes, bytes);
  }
}

// get maximum time interval in each file
int64_t getOldestKey(int32_t numOfFiles, int64_t fileId, SVnodeCfg *pCfg) {
  int64_t duration = pCfg->daysPerFile * tsMsPerDay[(uint8_t)pCfg->precision];
  return (fileId - numOfFiles + 1) * duration;
}

static int32_t getFirstCacheSlot(int32_t numOfBlocks, int32_t lastSlot, SCacheInfo *pCacheInfo) {
  return (lastSlot - numOfBlocks + 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
}

void vnodeFreeFieldsEx(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  vnodeFreeFields(pQuery);

  vnodeInitCompBlockLoadInfo(&pRuntimeEnv->compBlockLoadInfo);
}

static void vnodeSetCompBlockInfoLoaded(SQueryRuntimeEnv *pRuntimeEnv, int32_t fileIndex, int32_t sid) {
  SLoadCompBlockInfo *pCompBlockLoadInfo = &pRuntimeEnv->compBlockLoadInfo;

  pCompBlockLoadInfo->sid = sid;
  pCompBlockLoadInfo->fileListIndex = fileIndex;
  pCompBlockLoadInfo->fileId = pRuntimeEnv->vnodeFileInfo.pFileInfo[fileIndex].fileID;
}

static void vnodeSetCompBlockInfoLoaded_(SLoadCompBlockInfo *pCompBlockLoadInfo, int32_t fileIndex, int32_t sid,
                                         int32_t fileId) {
  pCompBlockLoadInfo->sid = sid;
  pCompBlockLoadInfo->fileListIndex = fileIndex;
  pCompBlockLoadInfo->fileId = fileId;
}

// todo remove this function
static TSKEY getFirstDataBlockInCache(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  assert(pQuery->fileId == -1 && QUERY_IS_ASC_QUERY(pQuery));

  /*
   * get the start position in cache according to the pQuery->lastkey
   *
   * In case of cache and disk file data overlaps and all required data are commit to disk file,
   * there are no qualified data available in cache, we need to set the QUERY_COMPLETED flag.
   *
   * If cache data and disk-based data are not completely overlapped, cacheBoundaryCheck function will set the
   * correct status flag.
   */
  TSKEY nextTimestamp = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, true);
  if (nextTimestamp < 0) {
    setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
  } else if (nextTimestamp > pQuery->ekey) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
  }

  return nextTimestamp;
}

int64_t getQueryStartPositionInCache_rv(STsdbQueryHandle *pQueryHandle, int32_t *slot, int32_t *pos,
                                        bool ignoreQueryRange);

static TSKEY getFirstDataBlockInCache_(STsdbQueryHandle *pQueryHandle) {
  //  assert(pQuery->fileId == -1 && QUERY_IS_ASC_QUERY(pQuery));

  /*
   * get the start position in cache according to the pQuery->lastkey
   *
   * In case of cache and disk file data overlaps and all required data are commit to disk file,
   * there are no qualified data available in cache, we need to set the QUERY_COMPLETED flag.
   *
   * If cache data and disk-based data are not completely overlapped, cacheBoundaryCheck function will set the
   * correct status flag.
   */
  TSKEY nextTimestamp =
      getQueryStartPositionInCache_rv(pQueryHandle, &pQueryHandle->cur.slot, &pQueryHandle->cur.pos, true);
  //  if (nextTimestamp < 0) {
  //    setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
  //  } else if (nextTimestamp > pQuery->ekey) {
  //    setQueryStatus(pQuery, QUERY_COMPLETED);
  //  }

  return nextTimestamp;
}

void vnodeInitCompBlockLoadInfo(SLoadCompBlockInfo *pCompBlockLoadInfo) {
  pCompBlockLoadInfo->sid = -1;
  pCompBlockLoadInfo->fileId = -1;
  pCompBlockLoadInfo->fileListIndex = -1;
}

static void clearAllMeterDataBlockInfo(SMeterDataInfo **pMeterDataInfo, int32_t start, int32_t end) {
  for (int32_t i = start; i < end; ++i) {
    tfree(pMeterDataInfo[i]->pBlock);
    pMeterDataInfo[i]->numOfBlocks = 0;
    pMeterDataInfo[i]->start = -1;
  }
}

int32_t vnodeIsDatablockLoaded(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t fileIndex, bool loadTS) {
  SQuery *            pQuery = pRuntimeEnv->pQuery;
  SDataBlockLoadInfo *pLoadInfo = &pRuntimeEnv->dataBlockLoadInfo;

  /* this block has been loaded into memory, return directly */
  if (pLoadInfo->fileId == pQuery->fileId && pLoadInfo->slotIdx == pQuery->slot && pQuery->slot != -1 &&
      pLoadInfo->sid == pMeterObj->sid && pLoadInfo->fileListIndex == fileIndex) {
    // previous load operation does not load the primary timestamp column, we only need to load the timestamp column
    if (pLoadInfo->tsLoaded == false && pLoadInfo->tsLoaded != loadTS) {
      return DISK_BLOCK_LOAD_TS;
    } else {
      return DISK_BLOCK_NO_NEED_TO_LOAD;
    }
  }

  return DISK_BLOCK_LOAD_BLOCK;
}

int32_t vnodeIsDatablockLoaded_(SQueryHandlePos *cur, SDataBlockLoadInfo_ *pLoadInfo, SMeterObj *pMeterObj,
                                int32_t fileIndex) {
  //  SDataBlockLoadInfo *pLoadInfo = &pRuntimeEnv->dataBlockLoadInfo;

  /* this block has been loaded into memory, return directly */
  if (pLoadInfo->fileId == cur->fileId && pLoadInfo->slotIdx == cur->slot && cur->slot != -1 &&
      pLoadInfo->sid == pMeterObj->sid && pLoadInfo->fileListIndex == fileIndex) {
    // previous load operation does not load the primary timestamp column, we only need to load the timestamp column
    //    if (pLoadInfo->tsLoaded == false && pLoadInfo->tsLoaded != loadTS) {
    //      return DISK_BLOCK_LOAD_TS;
    //    } else {
    return DISK_BLOCK_NO_NEED_TO_LOAD;
    //    }
  }

  return DISK_BLOCK_LOAD_BLOCK;
}

TSKEY getTimestampInCacheBlock(SQueryRuntimeEnv *pRuntimeEnv, SCacheBlock *pBlock, int32_t index) {
  if (pBlock == NULL || index >= pBlock->numOfPoints || index < 0) {
    return -1;
  }

  return ((TSKEY *)(pRuntimeEnv->primaryColBuffer->data))[index];
}

TSKEY getTimestampInCacheBlock_(SData *ptsBuf, SCacheBlock *pBlock, int32_t index) {
  if (pBlock == NULL || index >= pBlock->numOfPoints || index < 0) {
    return -1;
  }

  return ((TSKEY *)(ptsBuf->data))[index];
}

/*
 * NOTE: pQuery->pos will not change, the corresponding data block will be loaded into buffer
 * loadDataBlockOnDemand will change the value of pQuery->pos, according to the pQuery->lastKey
 */
TSKEY getTimestampInDiskBlock(SQueryRuntimeEnv *pRuntimeEnv, int32_t index) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  /*
   * the corresponding compblock info has been loaded already
   * todo add check for compblock loaded
   */
  SCompBlock *pBlock = getDiskDataBlock(pQuery, pQuery->slot);

  // this block must be loaded into buffer
  SDataBlockLoadInfo *pLoadInfo = &pRuntimeEnv->dataBlockLoadInfo;
  assert(pQuery->pos >= 0 && pQuery->pos < pBlock->numOfPoints);

  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  int32_t fileIndex = vnodeGetVnodeHeaderFileIndex(&pQuery->fileId, pRuntimeEnv, pQuery->order.order);

  dTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d, slot:%d load data block due to primary key required",
         GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, pQuery->slot);

  bool    loadTS = true;
  bool    loadFields = true;
  int32_t slot = pQuery->slot;

  int32_t ret = loadDataBlockIntoMem(pBlock, &pQuery->pFields[slot], pRuntimeEnv, fileIndex, loadTS, loadFields);
  if (ret != TSDB_CODE_SUCCESS) {
    return -1;
  }

  SET_DATA_BLOCK_LOADED(pRuntimeEnv->blockStatus);
  SET_FILE_BLOCK_FLAG(pRuntimeEnv->blockStatus);

  assert(pQuery->fileId == pLoadInfo->fileId && pQuery->slot == pLoadInfo->slotIdx);
  return ((TSKEY *)pRuntimeEnv->primaryColBuffer->data)[index];
}

TSKEY getTimestampInDiskBlock_(STsdbQueryHandle *pQueryHandle, int32_t index) {
  /*
   * the corresponding compblock info has been loaded already
   * todo add check for compblock loaded
   */
  SCompBlock *pBlock = getDiskDataBlock_(pQueryHandle, pQueryHandle->cur.slot);

  // this block must be loaded into buffer
  SDataBlockLoadInfo *pLoadInfo = &pQueryHandle->dataBlockLoadInfo;
  //  assert(pQuery->pos >= 0 && pQuery->pos < pBlock->numOfPoints);

  SMeterObj *pMeterObj = *(SMeterObj **)taosArrayGet(pQueryHandle->pTableList, 0);

  int32_t fileIndex =
      vnodeGetVnodeHeaderFileIndex_(&pQueryHandle->cur.fileId, pQueryHandle->order, &pQueryHandle->vnodeFileInfo);

  //  dTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d, slot:%d load data block due to primary key required",
  //         GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId,
  //         pQuery->slot);

  int32_t ret = loadDataBlockIntoMem_(pQueryHandle, pQueryHandle->pBlock,
                                      &pQueryHandle->pFields[pQueryHandle->cur.slot], fileIndex, NULL);
  if (ret != TSDB_CODE_SUCCESS) {
    return -1;
  }

  //  SET_DATA_BLOCK_LOADED(pRuntimeEnv->blockStatus);
  //  SET_FILE_BLOCK_FLAG(pRuntimeEnv->blockStatus);

  //  assert(pQuery->fileId == pLoadInfo->fileId && pQuery->slot == pLoadInfo->slotIdx);
  return ((TSKEY *)pQueryHandle->tsBuf->data)[index];
}

static bool setValidDataBlocks(SMeterDataInfo *pMeterDataInfo, int32_t end) {
  int32_t size = (end - pMeterDataInfo->start) + 1;
  assert(size > 0);

  if (size != pMeterDataInfo->numOfBlocks) {
    memmove(pMeterDataInfo->pBlock, &pMeterDataInfo->pBlock[pMeterDataInfo->start], size * sizeof(SCompBlock));

    char *tmp = realloc(pMeterDataInfo->pBlock, size * sizeof(SCompBlock));
    if (tmp == NULL) {
      return false;
    }

    pMeterDataInfo->pBlock = (SCompBlock *)tmp;
    pMeterDataInfo->numOfBlocks = size;
  }

  return true;
}

static void vnodeSetDataBlockInfoLoaded(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t fileIndex,
                                        bool tsLoaded) {
  SQuery *            pQuery = pRuntimeEnv->pQuery;
  SDataBlockLoadInfo *pLoadInfo = &pRuntimeEnv->dataBlockLoadInfo;

  pLoadInfo->fileId = pQuery->fileId;
  pLoadInfo->slotIdx = pQuery->slot;
  pLoadInfo->fileListIndex = fileIndex;
  pLoadInfo->sid = pMeterObj->sid;
  pLoadInfo->tsLoaded = tsLoaded;
}

static void vnodeSetDataBlockInfoLoaded_(STsdbQueryHandle *pQueryHandle, SMeterObj *pMeterObj, int32_t fileIndex,
                                         SArray *pLoadedCols, SQueryHandlePos *cur) {
  SDataBlockLoadInfo_ *pLoadInfo = &pQueryHandle->dataBlockLoadInfo;

  pLoadInfo->fileId = cur->fileId;
  pLoadInfo->slotIdx = cur->slot;
  pLoadInfo->fileListIndex = fileIndex;
  pLoadInfo->sid = pMeterObj->sid;
  pLoadInfo->pLoadedCols = pLoadedCols;
}

void vnodeInitDataBlockLoadInfo(SDataBlockLoadInfo *pBlockLoadInfo) {
  pBlockLoadInfo->slotIdx = -1;
  pBlockLoadInfo->fileId = -1;
  pBlockLoadInfo->sid = -1;
  pBlockLoadInfo->fileListIndex = -1;
}

static int32_t getFileIdFromKey(int32_t vid, TSKEY key) {
  SVnodeObj *pVnode = &vnodeList[vid];
  int64_t    delta = (int64_t)pVnode->cfg.daysPerFile * tsMsPerDay[(uint8_t)pVnode->cfg.precision];

  return (int32_t)(key / delta);  // set the starting fileId
}

/**
 * For each query, only one header file along with corresponding files is opened, in order to
 * avoid too many memory files opened at the same time.
 *
 * @param pRuntimeEnv
 * @param fileIndex
 * @return   -1 failed, 0 success
 */
int32_t vnodeGetHeaderFile(SQueryRuntimeEnv *pRuntimeEnv, int32_t fileIndex) {
  assert(fileIndex >= 0 && fileIndex < pRuntimeEnv->vnodeFileInfo.numOfFiles);

  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);  // only for log output

  SQueryFilesInfo *pVnodeFileInfo = &pRuntimeEnv->vnodeFileInfo;

  if (pVnodeFileInfo->current != fileIndex) {
    if (pVnodeFileInfo->current >= 0) {
      assert(pVnodeFileInfo->headerFileSize > 0);
    }

    // do close the current memory mapped header file and corresponding fd
    doCloseQueryFiles(pVnodeFileInfo);
    assert(pVnodeFileInfo->headerFileSize == -1);

    // set current opened file Index
    pVnodeFileInfo->current = fileIndex;

    // set the current opened files(header, data, last) path
    vnodeSetCurrentFileNames(pVnodeFileInfo);

    if (doOpenQueryFile(pQInfo, pVnodeFileInfo) != TSDB_CODE_SUCCESS) {
      doCloseQueryFiles(pVnodeFileInfo);  // all the fds may be partially opened, close them anyway.
      return -1;
    }
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * read comp block info from header file
 *
 */
static int vnodeGetCompBlockInfo(SMeterObj *pMeterObj, SQueryRuntimeEnv *pRuntimeEnv, int32_t fileIndex) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);

  SVnodeCfg *      pCfg = &vnodeList[pMeterObj->vnode].cfg;
  SHeaderFileInfo *pHeadeFileInfo = &pRuntimeEnv->vnodeFileInfo.pFileInfo[fileIndex];

  int64_t st = taosGetTimestampUs();

  // if the corresponding data/header files are already closed, re-open them here
  if (vnodeIsCompBlockInfoLoaded(pRuntimeEnv, pMeterObj, fileIndex) &&
      pRuntimeEnv->vnodeFileInfo.current == fileIndex) {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d compBlock info is loaded, not reload", GET_QINFO_ADDR(pQuery),
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pHeadeFileInfo->fileID);
    return pQuery->numOfBlocks;
  }

  SQueryCostSummary *pSummary = &pRuntimeEnv->summary;
  pSummary->readCompInfo++;
  pSummary->numOfSeek++;

  int32_t ret = vnodeGetHeaderFile(pRuntimeEnv, fileIndex);
  if (ret != TSDB_CODE_SUCCESS) {
    return -1;  // failed to load the header file data into memory
  }

  char *           buf = calloc(1, getCompHeaderSegSize(pCfg));
  SQueryFilesInfo *pVnodeFileInfo = &pRuntimeEnv->vnodeFileInfo;

  lseek(pVnodeFileInfo->headerFd, TSDB_FILE_HEADER_LEN, SEEK_SET);
  read(pVnodeFileInfo->headerFd, buf, getCompHeaderSegSize(pCfg));

  // check the offset value integrity
  if (validateHeaderOffsetSegment(pQInfo, pRuntimeEnv->vnodeFileInfo.headerFilePath, pMeterObj->vnode,
                                  buf - TSDB_FILE_HEADER_LEN, getCompHeaderSegSize(pCfg)) < 0) {
    free(buf);
    return -1;
  }

  SCompHeader *compHeader = (SCompHeader *)(buf + sizeof(SCompHeader) * pMeterObj->sid);

  // no data in this file for specified meter, abort
  if (compHeader->compInfoOffset == 0) {
    free(buf);
    return 0;
  }

  // corrupted file may cause the invalid compInfoOffset, check needs
  if (validateCompBlockOffset(pQInfo, pMeterObj, compHeader, &pRuntimeEnv->vnodeFileInfo,
                              getCompHeaderStartPosition(pCfg)) < 0) {
    free(buf);
    return -1;
  }

  lseek(pVnodeFileInfo->headerFd, compHeader->compInfoOffset, SEEK_SET);

  SCompInfo compInfo = {0};
  read(pVnodeFileInfo->headerFd, &compInfo, sizeof(SCompInfo));

  // check compblock info integrity
  if (validateCompBlockInfoSegment(pQInfo, pRuntimeEnv->vnodeFileInfo.headerFilePath, pMeterObj->vnode, &compInfo,
                                   compHeader->compInfoOffset) < 0) {
    free(buf);
    return -1;
  }

  if (compInfo.numOfBlocks <= 0 || compInfo.uid != pMeterObj->uid) {
    free(buf);
    return 0;
  }

  // free allocated SField data
  vnodeFreeFieldsEx(pRuntimeEnv);
  pQuery->numOfBlocks = (int32_t)compInfo.numOfBlocks;

  /*
   * +-------------+-----------+----------------+
   * | comp block  | checksum  | SField Pointer |
   * +-------------+-----------+----------------+
   */
  int32_t compBlockSize = compInfo.numOfBlocks * sizeof(SCompBlock);
  size_t  bufferSize = compBlockSize + sizeof(TSCKSUM) + POINTER_BYTES * pQuery->numOfBlocks;

  // prepare buffer to hold compblock data
  if (pQuery->blockBufferSize != bufferSize) {
    pQuery->pBlock = realloc(pQuery->pBlock, bufferSize);
    pQuery->blockBufferSize = (int32_t)bufferSize;
  }

  memset(pQuery->pBlock, 0, bufferSize);

  // read data: comp block + checksum
  read(pVnodeFileInfo->headerFd, pQuery->pBlock, compBlockSize + sizeof(TSCKSUM));
  TSCKSUM checksum = *(TSCKSUM *)((char *)pQuery->pBlock + compBlockSize);

  // check comp block integrity
  if (validateCompBlockSegment(pQInfo, pRuntimeEnv->vnodeFileInfo.headerFilePath, &compInfo, (char *)pQuery->pBlock,
                               pMeterObj->vnode, checksum) < 0) {
    free(buf);
    return -1;
  }

  pQuery->pFields = (SField **)((char *)pQuery->pBlock + compBlockSize + sizeof(TSCKSUM));
  vnodeSetCompBlockInfoLoaded(pRuntimeEnv, fileIndex, pMeterObj->sid);

  int64_t et = taosGetTimestampUs();
  qTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d, load compblock info, size:%d, elapsed:%f ms", pQInfo,
         pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pRuntimeEnv->vnodeFileInfo.pFileInfo[fileIndex].fileID,
         compBlockSize, (et - st) / 1000.0);

  pSummary->totalCompInfoSize += compBlockSize;
  pSummary->loadCompInfoUs += (et - st);

  free(buf);
  return pQuery->numOfBlocks;
}

int32_t vnodeGetHeaderFile_(SQueryFilesInfo_rv *pVnodeFileInfo, int32_t fileIndex) {
  //  assert(fileIndex >= 0 && fileIndex < pRuntimeEnv->vnodeFileInfo.numOfFiles);

  //  SQuery *pQuery = pRuntimeEnv->pQuery;
  //  SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);  // only for log output

  //  SQueryFilesInfo *pVnodeFileInfo = &pRuntimeEnv->vnodeFileInfo;

  if (pVnodeFileInfo->current != fileIndex) {
    if (pVnodeFileInfo->current >= 0) {
      assert(pVnodeFileInfo->headerFileSize > 0);
    }

    // do close the current memory mapped header file and corresponding fd
    doCloseQueryFiles_(pVnodeFileInfo);
    assert(pVnodeFileInfo->headerFileSize == -1);

    // set current opened file Index
    pVnodeFileInfo->current = fileIndex;

    // set the current opened files(header, data, last) path
    vnodeSetCurrentFileNames_(pVnodeFileInfo);

    if (doOpenQueryFile_(pVnodeFileInfo) != TSDB_CODE_SUCCESS) {
      doCloseQueryFiles_(pVnodeFileInfo);  // all the fds may be partially opened, close them anyway.
      return -1;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int vnodeGetCompBlockInfo_(STsdbQueryHandle *pQueryHandle, SMeterObj *pMeterObj, int32_t fileIndex) {
  SVnodeCfg *         pCfg = &vnodeList[pMeterObj->vnode].cfg;
  SQueryFilesInfo_rv *pVnodeFileInfo = &pQueryHandle->vnodeFileInfo;

  SHeaderFileInfo *pHeaderFileInfo = taosArrayGet(pVnodeFileInfo->pFileInfo, fileIndex);

  int64_t st = taosGetTimestampUs();

  // if the corresponding data/header files are already closed, re-open them here
  if (vnodeIsCompBlockInfoLoaded_(pQueryHandle) && pQueryHandle->vnodeFileInfo.current == fileIndex) {
    //    dTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d compBlock info is loaded, not reload", GET_QINFO_ADDR(pQuery),
    //           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pHeadeFileInfo->fileID);
    //    return pQuery->numOfBlocks;
  }

  //  SQueryCostSummary *pSummary = &pRuntimeEnv->summary;
  //  pSummary->readCompInfo++;
  //  pSummary->numOfSeek++;

  int32_t ret = vnodeGetHeaderFile_(pVnodeFileInfo, fileIndex);
  if (ret != TSDB_CODE_SUCCESS) {
    return -1;  // failed to load the header file data into memory
  }

  char *buf = calloc(1, getCompHeaderSegSize(pCfg));

  lseek(pVnodeFileInfo->headerFd, TSDB_FILE_HEADER_LEN, SEEK_SET);
  read(pVnodeFileInfo->headerFd, buf, getCompHeaderSegSize(pCfg));

  // check the offset value integrity
  if (validateHeaderOffsetSegment_(pVnodeFileInfo->headerFilePath, pMeterObj->vnode, buf - TSDB_FILE_HEADER_LEN,
                                   getCompHeaderSegSize(pCfg)) < 0) {
    free(buf);
    return -1;
  }

  SCompHeader *compHeader = (SCompHeader *)(buf + sizeof(SCompHeader) * pMeterObj->sid);

  // no data in this file for specified meter, abort
  if (compHeader->compInfoOffset == 0) {
    free(buf);
    return 0;
  }

  // corrupted file may cause the invalid compInfoOffset, check needs
  //  if (validateCompBlockOffset_(pMeterObj, compHeader, &pVnodeFileInfo->vnodeFileInfo,
  //                              getCompHeaderStartPosition(pCfg)) < 0) {
  //    free(buf);
  //    return -1;
  //  }

  lseek(pVnodeFileInfo->headerFd, compHeader->compInfoOffset, SEEK_SET);

  SCompInfo compInfo = {0};
  read(pVnodeFileInfo->headerFd, &compInfo, sizeof(SCompInfo));

  // check compblock info integrity
  if (validateCompBlockInfoSegment_(pHeaderFileInfo, pMeterObj->vnode, &compInfo, compHeader->compInfoOffset) < 0) {
    free(buf);
    return -1;
  }

  if (compInfo.numOfBlocks <= 0 || compInfo.uid != pMeterObj->uid) {
    free(buf);
    return 0;
  }

  pQueryHandle->numOfBlocks = compInfo.numOfBlocks;

  // free allocated SField data
  //  vnodeFreeFieldsEx(pRuntimeEnv);
  //  pQuery->numOfBlocks = (int32_t)compInfo.numOfBlocks;

  /*
   * +-------------+-----------+----------------+
   * | comp block  | checksum  | SField Pointer |
   * +-------------+-----------+----------------+
   */
  int32_t compBlockSize = compInfo.numOfBlocks * sizeof(SCompBlock);
  size_t  bufferSize = compBlockSize + sizeof(TSCKSUM) + POINTER_BYTES * pQueryHandle->numOfBlocks;

  // prepare buffer to hold compblock data
  if (pQueryHandle->blockBufferSize != bufferSize) {
    pQueryHandle->pBlock = realloc(pQueryHandle->pBlock, bufferSize);
    pQueryHandle->blockBufferSize = (int32_t)bufferSize;
  }

  memset(pQueryHandle->pBlock, 0, bufferSize);

  // read data: comp block + checksum
  read(pVnodeFileInfo->headerFd, pQueryHandle->pBlock, compBlockSize + sizeof(TSCKSUM));
  TSCKSUM checksum = *(TSCKSUM *)((char *)pQueryHandle->pBlock + compBlockSize);

  // check comp block integrity
  if (validateCompBlockSegment_(pVnodeFileInfo->headerFilePath, &compInfo, (char *)pQueryHandle->pBlock,
                                pMeterObj->vnode, checksum) < 0) {
    free(buf);
    return -1;
  }

  pQueryHandle->pFields = (SField **)((char *)pQueryHandle->pBlock + compBlockSize + sizeof(TSCKSUM));
  vnodeSetCompBlockInfoLoaded_(&pQueryHandle->compBlockLoadInfo, fileIndex, pMeterObj->sid, pHeaderFileInfo->fileID);

  int64_t et = taosGetTimestampUs();
  //  qTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d, load compblock info, size:%d, elapsed:%f ms", pQInfo,
  //         pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId,
  //         pRuntimeEnv->vnodeFileInfo.pFileInfo[fileIndex].fileID, compBlockSize, (et - st) / 1000.0);

  //  pSummary->totalCompInfoSize += compBlockSize;
  //  pSummary->loadCompInfoUs += (et - st);

  free(buf);
  return pQueryHandle->numOfBlocks;
}

/**
 *  move the cursor to next block and not load
 */
int32_t moveToNextBlock(SQueryRuntimeEnv *pRuntimeEnv, int32_t step, __block_search_fn_t searchFn, bool loadData) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  SET_DATA_BLOCK_NOT_LOADED(pRuntimeEnv->blockStatus);

  if (pQuery->fileId >= 0) {
    int32_t fileIndex = -1;

    /*
     * 1. ascending  order. The last data block of data file
     * 2. descending order. The first block of file
     */
    if ((step == QUERY_ASC_FORWARD_STEP && (pQuery->slot == pQuery->numOfBlocks - 1)) ||
        (step == QUERY_DESC_FORWARD_STEP && (pQuery->slot == 0))) {
      fileIndex = getNextDataFileCompInfo(pRuntimeEnv, pMeterObj, step);
      /* data maybe in cache */

      if (fileIndex >= 0) {  // next file
        pQuery->slot = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQuery->numOfBlocks - 1;
        pQuery->pos = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQuery->pBlock[pQuery->slot].numOfPoints - 1;
      } else {  // try data in cache
        assert(pQuery->fileId == -1);

        if (step == QUERY_ASC_FORWARD_STEP) {
          getFirstDataBlockInCache(pRuntimeEnv);
        } else {  // no data to check for desc order query
          setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
        }

        return DISK_DATA_LOADED;
      }
    } else {  // next block in the same file
      int32_t fid = pQuery->fileId;
      fileIndex = vnodeGetVnodeHeaderFileIndex(&fid, pRuntimeEnv, pQuery->order.order);

      pQuery->slot += step;
      pQuery->pos = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQuery->pBlock[pQuery->slot].numOfPoints - 1;
    }

    assert(pQuery->pBlock != NULL);

    /* no need to load data, return directly */
    if (!loadData) {
      return DISK_DATA_LOADED;
    }

    // load data block function will change the value of pQuery->pos
    int32_t ret =
        LoadDatablockOnDemand(&pQuery->pBlock[pQuery->slot], &pQuery->pFields[pQuery->slot], &pRuntimeEnv->blockStatus,
                              pRuntimeEnv, fileIndex, pQuery->slot, searchFn, true);
    if (ret != DISK_DATA_LOADED) {
      return ret;
    }
  } else {  // data in cache
    return moveToNextBlockInCache(pRuntimeEnv, step, searchFn);
  }

  return DISK_DATA_LOADED;
}

int32_t moveToNextBlock_(STsdbQueryHandle *pQueryHandle, int32_t step, bool loadData) {
  SQueryHandlePos *cur = &pQueryHandle->cur;

  if (pQueryHandle->cur.fileId >= 0) {
    int32_t fileIndex = -1;

    /*
     * 1. ascending  order. The last data block of data file
     * 2. descending order. The first block of file
     */
    if ((step == QUERY_ASC_FORWARD_STEP && (pQueryHandle->cur.slot == pQueryHandle->numOfBlocks - 1)) ||
        (step == QUERY_DESC_FORWARD_STEP && (pQueryHandle->cur.slot == 0))) {
      fileIndex = getNextDataFileCompInfo_(pQueryHandle, &pQueryHandle->cur, &pQueryHandle->vnodeFileInfo, step);

      /* data maybe in cache */
      if (fileIndex >= 0) {  // next file
        cur->slot = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQueryHandle->numOfBlocks - 1;
        cur->pos = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQueryHandle->pBlock[cur->slot].numOfPoints - 1;
      } else {  // try data in cache
        assert(cur->fileId == -1);

        if (step == QUERY_ASC_FORWARD_STEP) {
          return getFirstDataBlockInCache_(pQueryHandle);
        } else {  // no data to check for desc order query
                  //          setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
        }

        return 0;
      }
    } else {  // next block in the same file
      int32_t fid = cur->fileId;
      fileIndex = vnodeGetVnodeHeaderFileIndex_(&fid, pQueryHandle->order, &pQueryHandle->vnodeFileInfo);

      pQueryHandle->cur.slot += step;
      pQueryHandle->cur.pos = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQueryHandle->pBlock[cur->slot].numOfPoints - 1;
    }

    /* no need to load data, return directly */
    if (!loadData) {
      return 0;
    }

    // load data block function will change the value of pQuery->pos
    return 0;
  } else {  // data in cache
            //    return moveToNextBlockInCache_(pRuntimeEnv, step, searchFn);
  }

  return 0;
}

static int32_t readDataFromDiskFile(int fd, SQInfo *pQInfo, SQueryFilesInfo *pQueryFile, char *buf, uint64_t offset,
                                    int32_t size) {
  assert(size >= 0);

  int32_t ret = (int32_t)lseek(fd, offset, SEEK_SET);
  if (ret == -1) {
    //        qTrace("QInfo:%p seek failed, reason:%s", pQInfo, strerror(errno));
    return -1;
  }

  ret = read(fd, buf, size);
  //    qTrace("QInfo:%p read data %d completed", pQInfo, size);
  return 0;
}

static int32_t readDataFromDiskFile_(int fd, char *buf, uint64_t offset, int32_t size) {
  assert(size >= 0);

  int32_t ret = (int32_t)lseek(fd, offset, SEEK_SET);
  if (ret == -1) {
    return -1;
  }

  ret = read(fd, buf, size);
  return 0;
}

static int32_t loadColumnIntoMem(SQuery *pQuery, SQueryFilesInfo *pQueryFileInfo, SCompBlock *pBlock, SField *pFields,
                                 int32_t col, SData *sdata, void *tmpBuf, char *buffer, int32_t buffersize) {
  char *dst = (pBlock->algorithm) ? tmpBuf : sdata->data;

  int64_t offset = pBlock->offset + pFields[col].offset;
  SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);

  int     fd = pBlock->last ? pQueryFileInfo->lastFd : pQueryFileInfo->dataFd;
  int32_t ret = readDataFromDiskFile(fd, pQInfo, pQueryFileInfo, dst, offset, pFields[col].len);
  if (ret != 0) {
    return ret;
  }

  // load checksum
  TSCKSUM checksum = 0;
  ret = readDataFromDiskFile(fd, pQInfo, pQueryFileInfo, (char *)&checksum, offset + pFields[col].len, sizeof(TSCKSUM));
  if (ret != 0) {
    return ret;
  }

  // check column data integrity
  if (checksum != taosCalcChecksum(0, (const uint8_t *)dst, pFields[col].len)) {
    dLError("QInfo:%p, column data checksum error, file:%s, col: %d, offset:%" PRId64, GET_QINFO_ADDR(pQuery),
            pQueryFileInfo->dataFilePath, col, offset);

    return -1;
  }

  if (pBlock->algorithm) {
    (*pDecompFunc[pFields[col].type])(tmpBuf, pFields[col].len, pBlock->numOfPoints, sdata->data,
                                      pFields[col].bytes * pBlock->numOfPoints, pBlock->algorithm, buffer, buffersize);
  }

  return 0;
}

static int32_t loadColumnIntoMem_(SQueryFilesInfo_rv *pQueryFileInfo, SCompBlock *pBlock, SField *pFields, int32_t col,
                                  SData *sdata, void *tmpBuf, char *buffer, int32_t buffersize) {
  char *  dst = (pBlock->algorithm) ? tmpBuf : sdata->data;
  int64_t offset = pBlock->offset + pFields[col].offset;

  int     fd = pBlock->last ? pQueryFileInfo->lastFd : pQueryFileInfo->dataFd;
  int32_t ret = readDataFromDiskFile_(fd, dst, offset, pFields[col].len);
  if (ret != 0) {
    return ret;
  }

  // load checksum
  TSCKSUM checksum = 0;
  ret = readDataFromDiskFile_(fd, (char *)&checksum, offset + pFields[col].len, sizeof(TSCKSUM));
  if (ret != 0) {
    return ret;
  }

  // check column data integrity
  if (checksum != taosCalcChecksum(0, (const uint8_t *)dst, pFields[col].len)) {
    //    dLError("QInfo:%p, column data checksum error, file:%s, col: %d, offset:%" PRId64, GET_QINFO_ADDR(pQuery),
    //            pQueryFileInfo->dataFilePath, col, offset);

    return -1;
  }

  if (pBlock->algorithm) {
    (*pDecompFunc[pFields[col].type])(tmpBuf, pFields[col].len, pBlock->numOfPoints, sdata->data,
                                      pFields[col].bytes * pBlock->numOfPoints, pBlock->algorithm, buffer, buffersize);
  }

  return 0;
}

static int32_t loadDataBlockFieldsInfo(SQueryRuntimeEnv *pRuntimeEnv, SCompBlock *pBlock, SField **pField) {
  SQuery *         pQuery = pRuntimeEnv->pQuery;
  SQInfo *         pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);
  SMeterObj *      pMeterObj = pRuntimeEnv->pMeterObj;
  SQueryFilesInfo *pVnodeFilesInfo = &pRuntimeEnv->vnodeFileInfo;

  size_t size = sizeof(SField) * (pBlock->numOfCols) + sizeof(TSCKSUM);

  // if *pField != NULL, this block is loaded once, in current query do nothing
  if (*pField == NULL) {  // load the fields information once
    *pField = malloc(size);
  }

  SQueryCostSummary *pSummary = &pRuntimeEnv->summary;
  pSummary->totalFieldSize += size;
  pSummary->readField++;
  pSummary->numOfSeek++;

  int64_t st = taosGetTimestampUs();

  int     fd = pBlock->last ? pVnodeFilesInfo->lastFd : pVnodeFilesInfo->dataFd;
  int32_t ret = readDataFromDiskFile(fd, pQInfo, pVnodeFilesInfo, (char *)(*pField), pBlock->offset, size);
  if (ret != 0) {
    return ret;
  }

  // check fields integrity
  if (!taosCheckChecksumWhole((uint8_t *)(*pField), size)) {
    dLError("QInfo:%p vid:%d sid:%d id:%s, slot:%d, failed to read sfields, file:%s, sfields area broken:%" PRId64,
            pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot, pVnodeFilesInfo->dataFilePath,
            pBlock->offset);
    return -1;
  }

  int64_t et = taosGetTimestampUs();
  qTrace("QInfo:%p vid:%d sid:%d id:%s, slot:%d, load field info, size:%d, elapsed:%f ms", pQInfo, pMeterObj->vnode,
         pMeterObj->sid, pMeterObj->meterId, pQuery->slot, size, (et - st) / 1000.0);

  pSummary->loadFieldUs += (et - st);
  return 0;
}

static int32_t loadDataBlockFieldsInfo_(STsdbQueryHandle *pQueryHandle, SCompBlock *pBlock, SField **pField) {
  SQueryFilesInfo_rv *pVnodeFilesInfo = &pQueryHandle->vnodeFileInfo;

  size_t size = sizeof(SField) * (pBlock->numOfCols) + sizeof(TSCKSUM);

  // if *pField != NULL, this block is loaded once, in current query do nothing
  if (*pField == NULL) {  // load the fields information once
    *pField = malloc(size);
  }

  //  SQueryCostSummary *pSummary = &pRuntimeEnv->summary;
  //  pSummary->totalFieldSize += size;
  //  pSummary->readField++;
  //  pSummary->numOfSeek++;

  int64_t st = taosGetTimestampUs();

  int     fd = pBlock->last ? pVnodeFilesInfo->lastFd : pVnodeFilesInfo->dataFd;
  int32_t ret = readDataFromDiskFile_(fd, (char *)(*pField), pBlock->offset, size);
  if (ret != 0) {
    return ret;
  }

  // check fields integrity
  if (!taosCheckChecksumWhole((uint8_t *)(*pField), size)) {
    //    dLError("QInfo:%p vid:%d sid:%d id:%s, slot:%d, failed to read sfields, file:%s, sfields area broken:%"
    //    PRId64,
    //            pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot,
    //            pVnodeFilesInfo->dataFilePath, pBlock->offset);
    return -1;
  }

  int64_t et = taosGetTimestampUs();
  //  qTrace("QInfo:%p vid:%d sid:%d id:%s, slot:%d, load field info, size:%d, elapsed:%f ms", pQInfo, pMeterObj->vnode,
  //         pMeterObj->sid, pMeterObj->meterId, pQuery->slot, size, (et - st) / 1000.0);

  //  pSummary->loadFieldUs += (et - st);
  return 0;
}

static void fillWithNull(SQuery *pQuery, char *dst, int32_t col, int32_t numOfPoints) {
  int32_t bytes = pQuery->colList[col].data.bytes;
  int32_t type = pQuery->colList[col].data.type;

  setNullN(dst, type, bytes, numOfPoints);
}

static void fillWithNull_(SColumnInfo *pColumnInfo, char *dst, int32_t numOfPoints) {
  setNullN(dst, pColumnInfo->type, pColumnInfo->bytes, numOfPoints);
}

static int32_t loadPrimaryTSColumn(SQueryRuntimeEnv *pRuntimeEnv, SCompBlock *pBlock, SField **pField,
                                   int32_t *columnBytes) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  assert(PRIMARY_TSCOL_LOADED(pQuery) == false);

  if (columnBytes != NULL) {
    (*columnBytes) += (*pField)[PRIMARYKEY_TIMESTAMP_COL_INDEX].len + sizeof(TSCKSUM);
  }

  int32_t ret = loadColumnIntoMem(pQuery, &pRuntimeEnv->vnodeFileInfo, pBlock, *pField, PRIMARYKEY_TIMESTAMP_COL_INDEX,
                                  pRuntimeEnv->primaryColBuffer, pRuntimeEnv->unzipBuffer,
                                  pRuntimeEnv->secondaryUnzipBuffer, pRuntimeEnv->unzipBufSize);
  return ret;
}

static int32_t loadPrimaryTSColumn_(STsdbQueryHandle *pQueryHandle, SCompBlock *pBlock, SField **pField,
                                    int32_t *columnBytes) {
  //  assert(PRIMARY_TSCOL_LOADED(pQuery) == false);

  if (columnBytes != NULL) {
    (*columnBytes) += (*pField)[PRIMARYKEY_TIMESTAMP_COL_INDEX].len + sizeof(TSCKSUM);
  }

  int32_t ret = loadColumnIntoMem_(&pQueryHandle->vnodeFileInfo, pBlock, *pField, PRIMARYKEY_TIMESTAMP_COL_INDEX,
                                   pQueryHandle->tsBuf, pQueryHandle->unzipBuffer, pQueryHandle->secondaryUnzipBuffer,
                                   pQueryHandle->unzipBufSize);
  return ret;
}

static int32_t loadDataBlockIntoMem(SCompBlock *pBlock, SField **pField, SQueryRuntimeEnv *pRuntimeEnv, int32_t fileIdx,
                                    bool loadPrimaryCol, bool loadSField) {
  int32_t i = 0, j = 0;

  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;
  SData **   sdata = pRuntimeEnv->colDataBuffer;

  assert(fileIdx == pRuntimeEnv->vnodeFileInfo.current);

  SData **primaryTSBuf = &pRuntimeEnv->primaryColBuffer;
  void *  tmpBuf = pRuntimeEnv->unzipBuffer;
  int32_t columnBytes = 0;

  SQueryCostSummary *pSummary = &pRuntimeEnv->summary;

  int32_t status = vnodeIsDatablockLoaded(pRuntimeEnv, pMeterObj, fileIdx, loadPrimaryCol);
  if (status == DISK_BLOCK_NO_NEED_TO_LOAD) {
    dTrace(
        "QInfo:%p vid:%d sid:%d id:%s, fileId:%d, data block has been loaded, no need to load again, ts:%d, slot:%d,"
        " brange:%lld-%lld, rows:%d",
        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, loadPrimaryCol,
        pQuery->slot, pBlock->keyFirst, pBlock->keyLast, pBlock->numOfPoints);

    if (loadSField && (pQuery->pFields == NULL || pQuery->pFields[pQuery->slot] == NULL)) {
      loadDataBlockFieldsInfo(pRuntimeEnv, pBlock, &pQuery->pFields[pQuery->slot]);
    }

    return TSDB_CODE_SUCCESS;
  } else if (status == DISK_BLOCK_LOAD_TS) {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d, data block has been loaded, incrementally load ts",
           GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId);

    assert(PRIMARY_TSCOL_LOADED(pQuery) == false && loadSField == true);
    if (pQuery->pFields == NULL || pQuery->pFields[pQuery->slot] == NULL) {
      loadDataBlockFieldsInfo(pRuntimeEnv, pBlock, &pQuery->pFields[pQuery->slot]);
    }

    // load primary timestamp
    int32_t ret = loadPrimaryTSColumn(pRuntimeEnv, pBlock, pField, &columnBytes);

    vnodeSetDataBlockInfoLoaded(pRuntimeEnv, pMeterObj, fileIdx, loadPrimaryCol);
    return ret;
  }

  /* failed to load fields info, return with error info */
  if (loadSField && (loadDataBlockFieldsInfo(pRuntimeEnv, pBlock, pField) != 0)) {
    return -1;
  }

  int64_t st = taosGetTimestampUs();

  if (loadPrimaryCol) {
    if (PRIMARY_TSCOL_LOADED(pQuery)) {
      *primaryTSBuf = sdata[0];
    } else {
      int32_t ret = loadPrimaryTSColumn(pRuntimeEnv, pBlock, pField, &columnBytes);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      pSummary->numOfSeek++;
      j += 1;  // first column of timestamp is not needed to be read again
    }
  }

  int32_t ret = 0;

  /* the first round always be 1, the secondary round is determined by queried function */
  int32_t round = (IS_MASTER_SCAN(pRuntimeEnv)) ? 0 : 1;

  while (j < pBlock->numOfCols && i < pQuery->numOfCols) {
    if ((*pField)[j].colId < pQuery->colList[i].data.colId) {
      ++j;
    } else if ((*pField)[j].colId == pQuery->colList[i].data.colId) {
      // add additional check for data type
      if ((*pField)[j].type != pQuery->colList[i].data.type) {
        ret = TSDB_CODE_INVALID_QUERY_MSG;
        break;
      }

      /*
       * during supplementary scan:
       * 1. primary ts column (always loaded)
       * 2. query specified columns
       * 3. in case of filter column required, filter columns must be loaded.
       */
      if (pQuery->colList[i].req[round] == 1 || pQuery->colList[i].data.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        // if data of this column in current block are all null, do NOT read it from disk
        if ((*pField)[j].numOfNullPoints == pBlock->numOfPoints) {
          fillWithNull(pQuery, sdata[i]->data, i, pBlock->numOfPoints);
        } else {
          columnBytes += (*pField)[j].len + sizeof(TSCKSUM);
          ret = loadColumnIntoMem(pQuery, &pRuntimeEnv->vnodeFileInfo, pBlock, *pField, j, sdata[i], tmpBuf,
                                  pRuntimeEnv->secondaryUnzipBuffer, pRuntimeEnv->unzipBufSize);

          pSummary->numOfSeek++;
        }
      }
      ++i;
      ++j;
    } else {
      /*
       * pQuery->colList[i].colIdx < (*pFields)[j].colId this column is not existed in current block,
       * fill with NULL value
       */
      fillWithNull(pQuery, sdata[i]->data, i, pBlock->numOfPoints);

      pSummary->totalGenData += (pBlock->numOfPoints * pQuery->colList[i].data.bytes);
      ++i;
    }
  }

  if (j >= pBlock->numOfCols && i < pQuery->numOfCols) {
    // remain columns need to set null value
    while (i < pQuery->numOfCols) {
      fillWithNull(pQuery, sdata[i]->data, i, pBlock->numOfPoints);

      pSummary->totalGenData += (pBlock->numOfPoints * pQuery->colList[i].data.bytes);
      ++i;
    }
  }

  int64_t et = taosGetTimestampUs();
  qTrace("QInfo:%p vid:%d sid:%d id:%s, slot:%d, load block completed, ts loaded:%d, rec:%d, elapsed:%f ms",
         GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot, loadPrimaryCol,
         pBlock->numOfPoints, (et - st) / 1000.0);

  pSummary->totalBlockSize += columnBytes;
  pSummary->loadBlocksUs += (et - st);
  pSummary->readDiskBlocks++;

  vnodeSetDataBlockInfoLoaded(pRuntimeEnv, pMeterObj, fileIdx, loadPrimaryCol);
  return ret;
}

static int32_t loadDataBlockIntoMem_(STsdbQueryHandle *pQueryHandle, SCompBlock *pBlock, SField **pField,
                                     int32_t fileIdx, SArray *pColIdList) {
  int32_t i = 0, j = 0;

  // todo compared with the loaded columns, and only thoes not loaded column only.
  SMeterObj *pMeterObj = taosArrayGetP(pQueryHandle->pTableList, 0);

  SData **primaryTSBuf = &pQueryHandle->tsBuf;
  void *  tmpBuf = pQueryHandle->unzipBuffer;
  int32_t columnBytes = 0;

  // record the loaded columns and compare with prev
  int32_t status = vnodeIsDatablockLoaded_(&pQueryHandle->cur, &pQueryHandle->dataBlockLoadInfo, pMeterObj, fileIdx);
  if (status == DISK_BLOCK_NO_NEED_TO_LOAD) {
    //    dTrace(
    //        "QInfo:%p vid:%d sid:%d id:%s, fileId:%d, data block has been loaded, no need to load again, ts:%d,
    //        slot:%d," " brange:%lld-%lld, rows:%d", GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid,
    //        pMeterObj->meterId, pQuery->fileId, loadPrimaryCol, pQuery->slot, pBlock->keyFirst, pBlock->keyLast,
    //        pBlock->numOfPoints);
    return TSDB_CODE_SUCCESS;
  }

  // failed to load fields info, return with error info
  if (loadDataBlockFieldsInfo_(pQueryHandle, pBlock, pField) != 0) {
    return -1;
  }

  int16_t colId = *(int16_t *)taosArrayGet(pColIdList, 0);
  if (colId == 0) {
    if (PRIMARY_TSCOL_REQUIRED(pQueryHandle->pColumns)) {
      SColumnInfoEx_ *pColInfo = taosArrayGet(pQueryHandle->pColumns, 0);
      *primaryTSBuf = pColInfo->pData;
    } else {
      int32_t ret = loadPrimaryTSColumn_(pQueryHandle, pBlock, pField, &columnBytes);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      j += 1;  // first column of timestamp is not needed to be read again
      i += 1;
    }
  }

  int32_t ret = 0;

  /* the first round always be 1, the secondary round is determined by queried function */
  while (j < pBlock->numOfCols && i < taosArrayGetSize(pColIdList)) {
    colId = *(int16_t *)taosArrayGet(pColIdList, i);
    if ((*pField)[j].colId < colId) {
      ++j;
    } else if ((*pField)[j].colId == colId) {
      // add additional check for data type
      //      if ((*pField)[j].type != pColumn->data.type) {
      //        ret = TSDB_CODE_INVALID_QUERY_MSG;
      //        break;
      //      }

      /*
       * during reversed scan:
       * 1. primary ts column (always loaded)
       * 2. query specified columns
       * 3. in case of filter column required, filter columns must be loaded.
       */
      SData *         sdata = NULL;
      SColumnInfoEx_ *pColumn = NULL;
      for (int32_t k = 0; k < QH_GET_NUM_OF_COLS(pQueryHandle); ++k) {
        pColumn = taosArrayGet(pQueryHandle->pColumns, k);
        if (pColumn->info.colId == colId) {
          sdata = pColumn->pData;
          break;
        }
      }

      assert(sdata != NULL && pColumn != NULL);

      // if data of this column in current block are all null, do NOT read it from disk
      if ((*pField)[j].numOfNullPoints == pBlock->numOfPoints) {
        fillWithNull_(&pColumn->info, sdata->data, pBlock->numOfPoints);
      } else {
        columnBytes += (*pField)[j].len + sizeof(TSCKSUM);
        ret = loadColumnIntoMem_(&pQueryHandle->vnodeFileInfo, pBlock, *pField, j, sdata, tmpBuf,
                                 pQueryHandle->secondaryUnzipBuffer, pQueryHandle->unzipBufSize);
      }
      ++i;
      ++j;
    } else {
      SData *         sdata = NULL;
      SColumnInfoEx_ *pColumn = NULL;
      for (int32_t k = 0; k < QH_GET_NUM_OF_COLS(pQueryHandle); ++k) {
        pColumn = taosArrayGet(pQueryHandle->pColumns, i);
        if (pColumn->info.colId == colId) {
          sdata = pColumn->pData;
          break;
        }
      }

      // this column is not existed in current block, fill with NULL value
      fillWithNull_(&pColumn->info, sdata->data, pBlock->numOfPoints);
      ++i;
    }
  }

  // remain columns need to set null value
  if (j >= pBlock->numOfCols && i < taosArrayGetSize(pColIdList)) {
    while (i < taosArrayGetSize(pColIdList)) {
      SData *         sdata = NULL;
      SColumnInfoEx_ *pColumn = NULL;
      for (int32_t k = 0; k < QH_GET_NUM_OF_COLS(pQueryHandle); ++k) {
        pColumn = taosArrayGet(pQueryHandle->pColumns, k);
        if (pColumn->info.colId == colId) {
          sdata = pColumn->pData;
          break;
        }
      }

      fillWithNull_(&pColumn->info, sdata->data, pBlock->numOfPoints);
      ++i;
    }
  }

  //  qTrace("QInfo:%p vid:%d sid:%d id:%s, slot:%d, load block completed, ts loaded:%d, rec:%d, elapsed:%f ms",
  //         GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot, loadPrimaryCol,
  //         pBlock->numOfPoints, (et - st) / 1000.0);

  vnodeSetDataBlockInfoLoaded_(pQueryHandle, pMeterObj, fileIdx, pColIdList, &pQueryHandle->cur);
  return ret;
}

bool vnodeIsCompBlockInfoLoaded(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t fileIndex) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // check if data file header of this table has been loaded into memory, avoid to reloaded comp Block info
  SLoadCompBlockInfo *pLoadCompBlockInfo = &pRuntimeEnv->compBlockLoadInfo;

  // if vnodeFreeFields is called, the pQuery->pFields is NULL
  if (pLoadCompBlockInfo->fileListIndex == fileIndex && pLoadCompBlockInfo->sid == pMeterObj->sid &&
      pQuery->pFields != NULL && pQuery->fileId > 0) {
    assert(pRuntimeEnv->vnodeFileInfo.pFileInfo[fileIndex].fileID == pLoadCompBlockInfo->fileId &&
           pQuery->numOfBlocks > 0);
    return true;
  }

  return false;
}

bool vnodeIsCompBlockInfoLoaded_(STsdbQueryHandle *pQueryHandle) {
  // check if data file header of this table has been loaded into memory, avoid to reloaded comp Block info
  SLoadCompBlockInfo *pLoadCompBlockInfo = &pQueryHandle->compBlockLoadInfo;

  // if vnodeFreeFields is called, the pQuery->pFields is NULL
  //  if (pLoadCompBlockInfo->fileListIndex == fileIndex && pLoadCompBlockInfo->sid == pMeterObj->sid &&
  //      pQuery->pFields != NULL && pQuery->fileId > 0) {
  //    assert(pRuntimeEnv->vnodeFileInfo.pFileInfo[fileIndex].fileID == pLoadCompBlockInfo->fileId &&
  //        pQuery->numOfBlocks > 0);
  //    return true;
  //  }

  return false;
}

/*
 * close the opened fd are delegated to invoker
 */
int32_t doOpenQueryFile(SQInfo *pQInfo, SQueryFilesInfo *pVnodeFileInfo) {
  SHeaderFileInfo *pHeaderFileInfo = &pVnodeFileInfo->pFileInfo[pVnodeFileInfo->current];

  /*
   * current header file is empty or broken, return directly.
   *
   * if the header is smaller than or equals to the minimum file size value, this file is empty. No need to open this
   * file and the corresponding files.
   */
  if (checkIsHeaderFileEmpty(pVnodeFileInfo)) {
    qTrace("QInfo:%p vid:%d, fileId:%d, index:%d, size:%d, ignore file, empty or broken", pQInfo,
           pVnodeFileInfo->vnodeId, pHeaderFileInfo->fileID, pVnodeFileInfo->current, pVnodeFileInfo->headerFileSize);

    return -1;
  }

  pVnodeFileInfo->headerFd = open(pVnodeFileInfo->headerFilePath, O_RDONLY);
  if (!FD_VALID(pVnodeFileInfo->headerFd)) {
    dError("QInfo:%p failed open head file:%s reason:%s", pQInfo, pVnodeFileInfo->headerFilePath, strerror(errno));
    return -1;
  }

  pVnodeFileInfo->dataFd = open(pVnodeFileInfo->dataFilePath, O_RDONLY);
  if (!FD_VALID(pVnodeFileInfo->dataFd)) {
    dError("QInfo:%p failed open data file:%s reason:%s", pQInfo, pVnodeFileInfo->dataFilePath, strerror(errno));
    return -1;
  }

  pVnodeFileInfo->lastFd = open(pVnodeFileInfo->lastFilePath, O_RDONLY);
  if (!FD_VALID(pVnodeFileInfo->lastFd)) {
    dError("QInfo:%p failed open last file:%s reason:%s", pQInfo, pVnodeFileInfo->lastFilePath, strerror(errno));
    return -1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doOpenQueryFile_(SQueryFilesInfo_rv *pVnodeFileInfo) {
  //  SHeaderFileInfo *pHeaderFileInfo = &pVnodeFileInfo->pFileInfo[pVnodeFileInfo->current];

  /*
   * current header file is empty or broken, return directly.
   *
   * if the header is smaller than or equals to the minimum file size value, this file is empty. No need to open this
   * file and the corresponding files.
   */
  if (checkIsHeaderFileEmpty_(pVnodeFileInfo)) {
    //    qTrace("QInfo:%p vid:%d, fileId:%d, index:%d, size:%d, ignore file, empty or broken", pQInfo,
    //           pVnodeFileInfo->vnodeId, pHeaderFileInfo->fileID, pVnodeFileInfo->current,
    //           pVnodeFileInfo->headerFileSize);

    return -1;
  }

  pVnodeFileInfo->headerFd = open(pVnodeFileInfo->headerFilePath, O_RDONLY);
  if (!FD_VALID(pVnodeFileInfo->headerFd)) {
    //    dError("QInfo:%p failed open head file:%s reason:%s", pQInfo, pVnodeFileInfo->headerFilePath,
    //    strerror(errno));
    return -1;
  }

  pVnodeFileInfo->dataFd = open(pVnodeFileInfo->dataFilePath, O_RDONLY);
  if (!FD_VALID(pVnodeFileInfo->dataFd)) {
    //    dError("QInfo:%p failed open data file:%s reason:%s", pQInfo, pVnodeFileInfo->dataFilePath, strerror(errno));
    return -1;
  }

  pVnodeFileInfo->lastFd = open(pVnodeFileInfo->lastFilePath, O_RDONLY);
  if (!FD_VALID(pVnodeFileInfo->lastFd)) {
    //    dError("QInfo:%p failed open last file:%s reason:%s", pQInfo, pVnodeFileInfo->lastFilePath, strerror(errno));
    return -1;
  }

  return TSDB_CODE_SUCCESS;
}

void doCloseQueryFiles(SQueryFilesInfo *pVnodeFileInfo) {
  if (pVnodeFileInfo->current >= 0) {
    assert(pVnodeFileInfo->current < pVnodeFileInfo->numOfFiles && pVnodeFileInfo->current >= 0);

    pVnodeFileInfo->headerFileSize = -1;
    doCloseQueryFileInfoFD(pVnodeFileInfo);
  }

  assert(pVnodeFileInfo->current == -1);
}

void doCloseQueryFiles_(SQueryFilesInfo_rv *pVnodeFileInfo) {
  if (pVnodeFileInfo->current >= 0) {
    size_t size = taosArrayGetSize(pVnodeFileInfo->pFileInfo);

    assert(pVnodeFileInfo->current < size && pVnodeFileInfo->current >= 0);

    pVnodeFileInfo->headerFileSize = -1;
    doCloseQueryFileInfoFD(pVnodeFileInfo);
  }

  assert(pVnodeFileInfo->current == -1);
}

static int file_order_comparator(const void *p1, const void *p2) {
  SHeaderFileInfo *pInfo1 = (SHeaderFileInfo *)p1;
  SHeaderFileInfo *pInfo2 = (SHeaderFileInfo *)p2;

  if (pInfo1->fileID == pInfo2->fileID) {
    return 0;
  }

  return (pInfo1->fileID > pInfo2->fileID) ? 1 : -1;
}

void vnodeRecordAllFiles(SQInfo *pQInfo, int32_t vnodeId) {
  char suffix[] = ".head";

  struct dirent *pEntry = NULL;
  size_t         alloc = 4;  // default allocated size

  SQueryFilesInfo *pVnodeFilesInfo = &(pQInfo->pTableQuerySupporter->runtimeEnv.vnodeFileInfo);
  pVnodeFilesInfo->vnodeId = vnodeId;

  sprintf(pVnodeFilesInfo->dbFilePathPrefix, "%s/vnode%d/db/", tsDirectory, vnodeId);
  DIR *pDir = opendir(pVnodeFilesInfo->dbFilePathPrefix);
  if (pDir == NULL) {
    dError("QInfo:%p failed to open directory:%s, %s", pQInfo, pVnodeFilesInfo->dbFilePathPrefix, strerror(errno));
    return;
  }

  pVnodeFilesInfo->pFileInfo = calloc(1, sizeof(SHeaderFileInfo) * alloc);
  SVnodeObj *pVnode = &vnodeList[vnodeId];

  while ((pEntry = readdir(pDir)) != NULL) {
    if ((pEntry->d_name[0] == '.' && pEntry->d_name[1] == '\0') || (strcmp(pEntry->d_name, "..") == 0)) {
      continue;
    }

    if (pEntry->d_type & DT_DIR) {
      continue;
    }

    size_t len = strlen(pEntry->d_name);
    if (strcasecmp(&pEntry->d_name[len - 5], suffix) != 0) {
      continue;
    }

    int32_t vid = 0;
    int32_t fid = 0;
    sscanf(pEntry->d_name, "v%df%d", &vid, &fid);
    if (vid != vnodeId) { /* ignore error files */
      dError("QInfo:%p error data file:%s in vid:%d, ignore", pQInfo, pEntry->d_name, vnodeId);
      continue;
    }

    int32_t firstFid = pVnode->fileId - pVnode->numOfFiles + 1;
    if (fid > pVnode->fileId || fid < firstFid) {
      dError("QInfo:%p error data file:%s in vid:%d, fid:%d, fid range:%d-%d", pQInfo, pEntry->d_name, vnodeId, fid,
             firstFid, pVnode->fileId);
      continue;
    }

    assert(fid >= 0 && vid >= 0);

    if (++pVnodeFilesInfo->numOfFiles > alloc) {
      alloc = alloc << 1U;
      pVnodeFilesInfo->pFileInfo = realloc(pVnodeFilesInfo->pFileInfo, alloc * sizeof(SHeaderFileInfo));
      memset(&pVnodeFilesInfo->pFileInfo[alloc >> 1U], 0, (alloc >> 1U) * sizeof(SHeaderFileInfo));
    }

    int32_t index = pVnodeFilesInfo->numOfFiles - 1;
    vnodeStoreFileId(pVnodeFilesInfo, fid, index);
  }

  closedir(pDir);

  dTrace("QInfo:%p find %d data files in %s to be checked", pQInfo, pVnodeFilesInfo->numOfFiles,
         pVnodeFilesInfo->dbFilePathPrefix);

  /* order the files information according their names */
  qsort(pVnodeFilesInfo->pFileInfo, (size_t)pVnodeFilesInfo->numOfFiles, sizeof(SHeaderFileInfo),
        file_order_comparator);
}

void vnodeRecordAllFiles_rv(int32_t vnodeId, SQueryFilesInfo_rv *pVnodeFilesInfo) {
  char suffix[] = ".head";
  pVnodeFilesInfo->pFileInfo = taosArrayInit(4, sizeof(int32_t));

  struct dirent *pEntry = NULL;
  pVnodeFilesInfo->vnodeId = vnodeId;

  sprintf(pVnodeFilesInfo->dbFilePathPrefix, "%s/vnode%d/db/", tsDirectory, vnodeId);
  DIR *pDir = opendir(pVnodeFilesInfo->dbFilePathPrefix);
  if (pDir == NULL) {
    //    dError("QInfo:%p failed to open directory:%s, %s", pQInfo, pVnodeFilesInfo->dbFilePathPrefix,
    //    strerror(errno));
    return;
  }

  SVnodeObj *pVnode = &vnodeList[vnodeId];

  while ((pEntry = readdir(pDir)) != NULL) {
    if ((pEntry->d_name[0] == '.' && pEntry->d_name[1] == '\0') || (strcmp(pEntry->d_name, "..") == 0)) {
      continue;
    }

    if (pEntry->d_type & DT_DIR) {
      continue;
    }

    size_t len = strlen(pEntry->d_name);
    if (strcasecmp(&pEntry->d_name[len - 5], suffix) != 0) {
      continue;
    }

    int32_t vid = 0;
    int32_t fid = 0;
    sscanf(pEntry->d_name, "v%df%d", &vid, &fid);
    if (vid != vnodeId) { /* ignore error files */
      //      dError("QInfo:%p error data file:%s in vid:%d, ignore", pQInfo, pEntry->d_name, vnodeId);
      continue;
    }

    int32_t firstFid = pVnode->fileId - pVnode->numOfFiles + 1;
    if (fid > pVnode->fileId || fid < firstFid) {
      //      dError("QInfo:%p error data file:%s in vid:%d, fid:%d, fid range:%d-%d", pQInfo, pEntry->d_name, vnodeId,
      //      fid, firstFid, pVnode->fileId);
      continue;
    }

    assert(fid >= 0 && vid >= 0);
    taosArrayPush(pVnodeFilesInfo->pFileInfo, &fid);
  }

  closedir(pDir);

  //  dTrace("QInfo:%p find %d data files in %s to be checked", pQInfo, pVnodeFilesInfo->numOfFiles,
  //         pVnodeFilesInfo->dbFilePathPrefix);

  /* order the files information according their names */
  size_t numOfFiles = taosArrayGetSize(pVnodeFilesInfo->pFileInfo);
  qsort(pVnodeFilesInfo->pFileInfo, numOfFiles, sizeof(SHeaderFileInfo), file_order_comparator);
}

int32_t vnodeGetVnodeHeaderFileIndex(int32_t *fid, SQueryRuntimeEnv *pRuntimeEnv, int32_t order) {
  if (pRuntimeEnv->vnodeFileInfo.numOfFiles == 0) {
    return -1;
  }

  SQueryFilesInfo *pVnodeFiles = &pRuntimeEnv->vnodeFileInfo;

  /* set the initial file for current query */
  if (order == TSQL_SO_ASC && *fid < pVnodeFiles->pFileInfo[0].fileID) {
    *fid = pVnodeFiles->pFileInfo[0].fileID;
    return 0;
  } else if (order == TSQL_SO_DESC && *fid > pVnodeFiles->pFileInfo[pVnodeFiles->numOfFiles - 1].fileID) {
    *fid = pVnodeFiles->pFileInfo[pVnodeFiles->numOfFiles - 1].fileID;
    return pVnodeFiles->numOfFiles - 1;
  }

  int32_t numOfFiles = pVnodeFiles->numOfFiles;

  if (order == TSQL_SO_DESC && *fid > pVnodeFiles->pFileInfo[numOfFiles - 1].fileID) {
    *fid = pVnodeFiles->pFileInfo[numOfFiles - 1].fileID;
    return numOfFiles - 1;
  }

  if (order == TSQL_SO_ASC) {
    int32_t i = 0;
    int32_t step = QUERY_ASC_FORWARD_STEP;

    while (i<numOfFiles && * fid> pVnodeFiles->pFileInfo[i].fileID) {
      i += step;
    }

    if (i < numOfFiles && *fid <= pVnodeFiles->pFileInfo[i].fileID) {
      *fid = pVnodeFiles->pFileInfo[i].fileID;
      return i;
    } else {
      return -1;
    }
  } else {
    int32_t i = numOfFiles - 1;
    int32_t step = QUERY_DESC_FORWARD_STEP;

    while (i >= 0 && *fid < pVnodeFiles->pFileInfo[i].fileID) {
      i += step;
    }

    if (i >= 0 && *fid >= pVnodeFiles->pFileInfo[i].fileID) {
      *fid = pVnodeFiles->pFileInfo[i].fileID;
      return i;
    } else {
      return -1;
    }
  }
}

int32_t vnodeGetVnodeHeaderFileIndex_(int32_t *fid, int32_t order, SQueryFilesInfo_rv *pVnodeFiles) {
  if (taosArrayGetSize(pVnodeFiles->pFileInfo) == 0) {
    return -1;
  }

  SArray *pfileIdList = pVnodeFiles->pFileInfo;
  size_t  num = taosArrayGetSize(pfileIdList);

  int32_t first = *(int32_t *)taosArrayGet(pfileIdList, 0);
  int32_t last = *(int32_t *)taosArrayGet(pfileIdList, num - 1);

  /* set the initial file for current query */
  if (order == TSQL_SO_ASC && *fid < first) {
    *fid = (*(int32_t *)taosArrayGet(pfileIdList, 0));
    return 0;
  } else if (order == TSQL_SO_DESC && *fid > last) {
    *fid = (*(int32_t *)taosArrayGet(pfileIdList, num - 1));
    return num - 1;
  }

  if (order == TSQL_SO_DESC && *fid > last) {
    *fid = last;
    return num - 1;
  }

  // todo : binary search
  if (order == TSQL_SO_ASC) {
    int32_t i = 0;
    int32_t step = QUERY_ASC_FORWARD_STEP;

    while (i<num && * fid> * (int32_t *)taosArrayGet(pfileIdList, i)) {
      i += step;
    }

    // locate the first file id that is larger than the specified value
    if (i < num && *fid <= *(int32_t *)taosArrayGet(pfileIdList, i)) {
      *fid = *(int32_t *)taosArrayGet(pfileIdList, i);
      return i;
    } else {
      return -1;
    }
  } else {
    int32_t i = num - 1;
    int32_t step = QUERY_DESC_FORWARD_STEP;

    while (i >= 0 && *fid < *(int32_t *)taosArrayGet(pfileIdList, i)) {
      i += step;
    }

    if (i >= 0 && *fid >= *(int32_t *)taosArrayGet(pfileIdList, i)) {
      *fid = *(int32_t *)taosArrayGet(pfileIdList, i);
      return i;
    } else {
      return -1;
    }
  }
}

SBlockInfo getBlockBasicInfo(SQueryRuntimeEnv *pRuntimeEnv, void *pBlock, int32_t blockType) {
  SBlockInfo blockInfo = {0};
  if (IS_FILE_BLOCK(blockType)) {
    SCompBlock *pDiskBlock = (SCompBlock *)pBlock;

    blockInfo.keyFirst = pDiskBlock->keyFirst;
    blockInfo.keyLast = pDiskBlock->keyLast;
    blockInfo.size = pDiskBlock->numOfPoints;
    blockInfo.numOfCols = pDiskBlock->numOfCols;
  } else {
    SCacheBlock *pCacheBlock = (SCacheBlock *)pBlock;

    blockInfo.keyFirst = getTimestampInCacheBlock(pRuntimeEnv, pCacheBlock, 0);
    blockInfo.keyLast = getTimestampInCacheBlock(pRuntimeEnv, pCacheBlock, pCacheBlock->numOfPoints - 1);
    blockInfo.size = pCacheBlock->numOfPoints;
    blockInfo.numOfCols = pCacheBlock->pMeterObj->numOfColumns;
  }

  return blockInfo;
}

SCompBlock *getDiskDataBlock(SQuery *pQuery, int32_t slot) {
  assert(pQuery->fileId >= 0 && slot >= 0 && slot < pQuery->numOfBlocks && pQuery->pBlock != NULL);
  return &pQuery->pBlock[slot];
}

SCompBlock *getDiskDataBlock_(STsdbQueryHandle *pQueryHandle, int32_t slot) {
  SQueryHandlePos *cur = &pQueryHandle->cur;

  assert(cur->fileId >= 0 && slot >= 0 && slot < pQueryHandle->numOfBlocks && pQueryHandle->pBlock != NULL);
  return &pQueryHandle->pBlock[slot];
}

void *getGenericDataBlock(SMeterObj *pMeterObj, SQueryRuntimeEnv *pRuntimeEnv, int32_t slot) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (IS_DISK_DATA_BLOCK(pQuery)) {
    return getDiskDataBlock(pQuery, slot);
  } else {
    return getCacheDataBlock(pMeterObj, pRuntimeEnv, slot);
  }
}

SBlockInfo getBlockInfo(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  void *pBlock = getGenericDataBlock(pRuntimeEnv->pMeterObj, pRuntimeEnv, pQuery->slot);
  assert(pBlock != NULL);

  int32_t blockType = IS_DISK_DATA_BLOCK(pQuery) ? BLK_FILE_BLOCK : BLK_CACHE_BLOCK;
  return getBlockBasicInfo(pRuntimeEnv, pBlock, blockType);
}

SDataBlockInfo getTrueBlockInfo(STsdbQueryHandle *pQueryHandle) {
  // todo check the timestamp value
  void *         pBlock = NULL;
  SDataBlockInfo info = {{0}, 0};

  SQueryHandlePos *cur = &pQueryHandle->cur;

  if (cur->fileId > 0) {
    pBlock = getDiskDataBlock_(pQueryHandle, cur->slot);

    SCompBlock *pDiskBlock = (SCompBlock *)pBlock;
    info.window.skey = pDiskBlock->keyFirst;
    info.window.ekey = pDiskBlock->keyLast;
    info.size = pDiskBlock->numOfPoints;
    info.numOfCols = pDiskBlock->numOfCols;
  } else {
    pBlock = getCacheDataBlock_(pQueryHandle, taosArrayGet(pQueryHandle->pTableList, 0), pQueryHandle->cur.slot);

    SCacheBlock *pCacheBlock = (SCacheBlock *)pBlock;

    info.window.skey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pCacheBlock, 0);
    info.window.ekey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pCacheBlock, pCacheBlock->numOfPoints - 1);
    info.size = pCacheBlock->numOfPoints;
    info.numOfCols = pCacheBlock->pMeterObj->numOfColumns;
  }

  assert(pBlock != NULL);
  return info;
}

SDataBlockInfo getBlockInfo_(STsdbQueryHandle *pQueryHandle) {
  // todo check the timestamp value
  void *         pBlock = NULL;
  SDataBlockInfo info = {{0}, 0};

  SQueryHandlePos *cur = &pQueryHandle->cur;

  if (cur->fileId > 0) {
    pBlock = getDiskDataBlock_(pQueryHandle, cur->slot);

    SCompBlock *pDiskBlock = (SCompBlock *)pBlock;
    if (pQueryHandle->realNumOfRows == pDiskBlock->numOfPoints) {//.pos == 0 && QUERY_IS_ASC_QUERY_RV(pQueryHandle->order)) {
      info.window.skey = pDiskBlock->keyFirst;
      info.window.ekey = pDiskBlock->keyLast;
      info.size = pDiskBlock->numOfPoints;
      info.numOfCols = pDiskBlock->numOfCols;
    } else {  // this block must have been loaded into buffer
      info.size = pQueryHandle->realNumOfRows;
      TSKEY *tsArray = (TSKEY *)pQueryHandle->tsBuf->data;
      
      info.window.skey = tsArray[0];
      info.window.ekey = tsArray[info.size - 1];
      info.numOfCols = pDiskBlock->numOfCols;
    }
  } else {
    pBlock = getCacheDataBlock_(pQueryHandle, taosArrayGet(pQueryHandle->pTableList, 0), pQueryHandle->cur.slot);

    SCacheBlock *pCacheBlock = (SCacheBlock *)pBlock;

    info.window.skey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pCacheBlock, 0);
    info.window.ekey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pCacheBlock, pCacheBlock->numOfPoints - 1);
    info.size = pCacheBlock->numOfPoints;
    info.numOfCols = pCacheBlock->pMeterObj->numOfColumns;
  }
  assert(pBlock != NULL);

  return info;
}

char *doGetDataBlocks(SQuery *pQuery, SData **data, int32_t colIdx) {
  assert(colIdx >= 0 && colIdx < pQuery->numOfCols);
  char *pData = data[colIdx]->data;
  return pData;
}

char *getDataBlocks(SQueryRuntimeEnv *pRuntimeEnv, SArithmeticSupport *sas, int32_t col, int32_t size) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  char *dataBlock = NULL;

  int32_t functionId = pQuery->pSelectExpr[col].pBase.functionId;

  if (functionId == TSDB_FUNC_ARITHM) {
    sas->pExpr = &pQuery->pSelectExpr[col];

    // set the start offset to be the lowest start position, no matter asc/desc query order
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      pCtx->startOffset = pQuery->pos;
    } else {
      pCtx->startOffset = pQuery->pos - (size - 1);
    }

    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      SColumnInfo *pColMsg = &pQuery->colList[i].data;
      char *       pData = doGetDataBlocks(pQuery, pRuntimeEnv->colDataBuffer, pQuery->colList[i].colIdxInBuf);

      sas->elemSize[i] = pColMsg->bytes;
      sas->data[i] = pData + pCtx->startOffset * sas->elemSize[i];  // start from the offset
    }

    sas->numOfCols = pQuery->numOfCols;
    sas->offset = 0;
  } else {  // other type of query function
    SColIndexEx *pCol = &pQuery->pSelectExpr[col].pBase.colInfo;
    if (TSDB_COL_IS_TAG(pCol->flag)) {
      dataBlock = NULL;
    } else {
      /*
       *  the colIdx is acquired from the first meter of all qualified meters in this vnode during query prepare stage,
       *  the remain meter may not have the required column in cache actually.
       *  So, the validation of required column in cache with the corresponding meter schema is reinforced.
       */
      dataBlock = doGetDataBlocks(pQuery, pRuntimeEnv->colDataBuffer, pCol->colIdxInBuf);
    }
  }

  return dataBlock;
}

char *getDataBlocks_(SQueryRuntimeEnv *pRuntimeEnv, SArithmeticSupport *sas, int32_t col, int32_t size,
                     SArray *pDataBlock) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  char *dataBlock = NULL;

  int32_t functionId = pQuery->pSelectExpr[col].pBase.functionId;

  if (functionId == TSDB_FUNC_ARITHM) {
    sas->pExpr = &pQuery->pSelectExpr[col];

    // set the start offset to be the lowest start position, no matter asc/desc query order
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      pCtx->startOffset = pQuery->pos;
    } else {
      pCtx->startOffset = pQuery->pos - (size - 1);
    }

    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      SColumnInfo *pColMsg = &pQuery->colList[i].data;
      char *       pData = doGetDataBlocks(pQuery, pRuntimeEnv->colDataBuffer, pQuery->colList[i].colIdxInBuf);

      sas->elemSize[i] = pColMsg->bytes;
      sas->data[i] = pData + pCtx->startOffset * sas->elemSize[i];  // start from the offset
    }

    sas->numOfCols = pQuery->numOfCols;
    sas->offset = 0;
  } else {  // other type of query function
    SColIndexEx *pCol = &pQuery->pSelectExpr[col].pBase.colInfo;
    if (TSDB_COL_IS_TAG(pCol->flag)) {
      dataBlock = NULL;
    } else {
      /*
       *  the colIdx is acquired from the first meter of all qualified meters in this vnode during query prepare stage,
       *  the remain meter may not have the required column in cache actually.
       *  So, the validation of required column in cache with the corresponding meter schema is reinforced.
       */

      if (pDataBlock == NULL) {
        return NULL;
      }

      int32_t numOfCols = taosArrayGetSize(pDataBlock);
      for (int32_t i = 0; i < numOfCols; ++i) {
        SColumnInfoEx_ *p = taosArrayGet(pDataBlock, i);
        if (pCol->colId == p->info.colId) {
          dataBlock = p->pData->data;
          break;
        }
      }
    }
  }

  return dataBlock;
}

void getBasicCacheInfoSnapshot(SQuery *pQuery, SCacheInfo *pCacheInfo, int32_t vid) {
  // commitSlot here denotes the first uncommitted block in cache
  int32_t numOfBlocks = 0;
  int32_t lastSlot = 0;
  int32_t commitSlot = 0;
  int32_t commitPoint = 0;

  SCachePool *pPool = (SCachePool *)vnodeList[vid].pCachePool;
  pthread_mutex_lock(&pPool->vmutex);
  numOfBlocks = pCacheInfo->numOfBlocks;
  lastSlot = pCacheInfo->currentSlot;
  commitSlot = pCacheInfo->commitSlot;
  commitPoint = pCacheInfo->commitPoint;
  pthread_mutex_unlock(&pPool->vmutex);

  // make sure it is there, otherwise, return right away
  pQuery->currentSlot = lastSlot;
  pQuery->numOfBlocks = numOfBlocks;
  pQuery->firstSlot = getFirstCacheSlot(numOfBlocks, lastSlot, pCacheInfo);
  pQuery->commitSlot = commitSlot;
  pQuery->commitPoint = commitPoint;

  /*
   * Note: the block id is continuous increasing, never becomes smaller.
   *
   * blockId is the maximum block id in cache of current meter during query.
   * If any blocks' id are greater than this value, those blocks may be reallocated to other meters,
   * or assigned new data of this meter, on which the query is performed should be ignored.
   */
  if (pQuery->numOfBlocks > 0) {
    pQuery->blockId = pCacheInfo->cacheBlocks[pQuery->currentSlot]->blockId;
  }
}

void getBasicCacheInfoSnapshot_(STsdbQueryHandle *pQueryHandle, SCacheInfo *pCacheInfo, int32_t vid) {
  // commitSlot here denotes the first uncommitted block in cache
  int32_t numOfBlocks = 0;
  int32_t lastSlot = 0;
  int32_t commitSlot = 0;
  int32_t commitPoint = 0;

  SCachePool *pPool = (SCachePool *)vnodeList[vid].pCachePool;
  pthread_mutex_lock(&pPool->vmutex);
  numOfBlocks = pCacheInfo->numOfBlocks;
  lastSlot = pCacheInfo->currentSlot;
  commitSlot = pCacheInfo->commitSlot;
  commitPoint = pCacheInfo->commitPoint;
  pthread_mutex_unlock(&pPool->vmutex);

  // make sure it is there, otherwise, return right away
  pQueryHandle->currentSlot = lastSlot;
  pQueryHandle->numOfCacheBlocks = numOfBlocks;
  pQueryHandle->firstSlot = getFirstCacheSlot(numOfBlocks, lastSlot, pCacheInfo);
  pQueryHandle->commitSlot = commitSlot;
  pQueryHandle->commitPoint = commitPoint;

  /*
   * Note: the block id is continuous increasing, never becomes smaller.
   *
   * blockId is the maximum block id in cache of current meter during query.
   * If any blocks' id are greater than this value, those blocks may be reallocated to other meters,
   * or assigned new data of this meter, on which the query is performed should be ignored.
   */
  if (pQueryHandle->numOfCacheBlocks > 0) {
    pQueryHandle->blockId = pCacheInfo->cacheBlocks[pQueryHandle->currentSlot]->blockId;
  }
}

TSKEY getQueryPositionForCacheInvalid(SQueryRuntimeEnv *pRuntimeEnv, __block_search_fn_t searchFn) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SQInfo *   pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;
  int32_t    step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  dTrace(
      "QInfo:%p vid:%d sid:%d id:%s cache block re-allocated to other meter, "
      "try get query start position in file/cache, qrange:%" PRId64 "-%" PRId64 ", lastKey:%" PRId64,
      pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery->lastKey);

  if (step == QUERY_DESC_FORWARD_STEP) {
    /*
     * In descending order query, if the cache is invalid, it must be flushed to disk.
     * Try to find the appropriate position in file, and no need to search cache any more.
     */
    bool ret = getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_LESS_EQUAL, searchFn);

    dTrace("QInfo:%p vid:%d sid:%d id:%s find the possible position in file, fileId:%d, slot:%d, pos:%d", pQInfo,
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, pQuery->slot, pQuery->pos);

    if (ret) {
      TSKEY key = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);

      // key in query range. If not, no qualified in disk file
      if (key < pQuery->ekey) {
        setQueryStatus(pQuery, QUERY_COMPLETED);
      }

      return key;
    } else {
      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
      return -1;  // no data to check
    }
  } else {  // asc query
    bool ret = getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_GREATER_EQUAL, searchFn);
    if (ret) {
      dTrace("QInfo:%p vid:%d sid:%d id:%s find the possible position, fileId:%d, slot:%d, pos:%d", pQInfo,
             pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, pQuery->slot, pQuery->pos);

      TSKEY key = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);

      // key in query range. If not, no qualified in disk file
      if (key > pQuery->ekey) {
        setQueryStatus(pQuery, QUERY_COMPLETED);
      }

      return key;
    } else {
      /*
       * all data in file is less than the pQuery->lastKey, try cache again.
       * cache block status will be set in getFirstDataBlockInCache function
       */
      TSKEY key = getFirstDataBlockInCache(pRuntimeEnv);

      dTrace("QInfo:%p vid:%d sid:%d id:%s find the new position in cache, fileId:%d, slot:%d, pos:%d", pQInfo,
             pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, pQuery->slot, pQuery->pos);
      return key;
    }
  }
}

int32_t moveToNextBlockInCache(SQueryRuntimeEnv *pRuntimeEnv, int32_t step, __block_search_fn_t searchFn) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  assert(pQuery->fileId < 0);

  /*
   * ascending order to last cache block all data block in cache have been iterated, no need to set
   * pRuntimeEnv->nextPos. done
   */
  if (step == QUERY_ASC_FORWARD_STEP && pQuery->slot == pQuery->currentSlot) {
    setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    return DISK_DATA_LOADED;
  }

  /*
   * descending order to first cache block, try file
   * NOTE: use the real time cache information, not the snapshot
   */
  int32_t numOfBlocks = pCacheInfo->numOfBlocks;
  int32_t currentSlot = pCacheInfo->currentSlot;

  int32_t firstSlot = getFirstCacheSlot(numOfBlocks, currentSlot, pCacheInfo);

  if (step == QUERY_DESC_FORWARD_STEP && pQuery->slot == firstSlot) {
    bool ret = getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_LESS_EQUAL, searchFn);
    if (ret) {
      TSKEY key = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);

      // key in query range. If not, no qualified in disk file
      if (key < pQuery->ekey) {
        setQueryStatus(pQuery, QUERY_COMPLETED);
      }

      // the skip operation does NOT set the startPos yet
      //      assert(pRuntimeEnv->startPos.fileId < 0);
    } else {
      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    }
    return DISK_DATA_LOADED;
  }

  /* now still iterate the cache data blocks */
  pQuery->slot = (pQuery->slot + step + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
  SCacheBlock *pBlock = getCacheDataBlock(pMeterObj, pRuntimeEnv, pQuery->slot);

  /*
   * data in this cache block has been flushed to disk, then we should locate the start position in file.
   * In both desc/asc query, this situation may occur. And we need to locate the start query position in file or cache.
   */
  if (pBlock == NULL) {
    getQueryPositionForCacheInvalid(pRuntimeEnv, searchFn);

    return DISK_DATA_LOADED;
  } else {
    pQuery->pos = (QUERY_IS_ASC_QUERY(pQuery)) ? 0 : pBlock->numOfPoints - 1;

    TSKEY startkey = getTimestampInCacheBlock(pRuntimeEnv, pBlock, pQuery->pos);
    if (startkey < 0) {
      setQueryStatus(pQuery, QUERY_COMPLETED);
    }

    SET_CACHE_BLOCK_FLAG(pRuntimeEnv->blockStatus);

    dTrace("QInfo:%p check cache block, blockId:%d slot:%d pos:%d, blockstatus:%d", GET_QINFO_ADDR(pQuery),
           pQuery->blockId, pQuery->slot, pQuery->pos, pRuntimeEnv->blockStatus);
  }

  return DISK_DATA_LOADED;
}

int32_t getNextDataFileCompInfo(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, int32_t step) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->fileId += step;

  int32_t fileIndex = 0;
  int32_t order = (step == QUERY_ASC_FORWARD_STEP) ? TSQL_SO_ASC : TSQL_SO_DESC;
  while (1) {
    fileIndex = vnodeGetVnodeHeaderFileIndex(&pQuery->fileId, pRuntimeEnv, order);

    // no files left, abort
    if (fileIndex < 0) {
      if (step == QUERY_ASC_FORWARD_STEP) {
        dTrace("QInfo:%p no more file to access, try data in cache", GET_QINFO_ADDR(pQuery));
      } else {
        dTrace("QInfo:%p no more file to access in desc order, query completed", GET_QINFO_ADDR(pQuery));
      }

      vnodeFreeFieldsEx(pRuntimeEnv);
      pQuery->fileId = -1;
      break;
    }

    // failed to mmap header file into memory will cause the retrieval of compblock info failed
    if (vnodeGetCompBlockInfo(pMeterObj, pRuntimeEnv, fileIndex) > 0) {
      break;
    }

    /*
     * 1. failed to read blk information from header file or open data file failed
     * 2. header file is empty
     *
     * try next one
     */
    pQuery->fileId += step;

    /* for backwards search, if the first file is not valid, abort */
    if (step < 0 && fileIndex == 0) {
      vnodeFreeFieldsEx(pRuntimeEnv);
      pQuery->fileId = -1;
      fileIndex = -1;
      break;
    }
  }

  return fileIndex;
}

int32_t getNextDataFileCompInfo_(STsdbQueryHandle *pQueryHandle, SQueryHandlePos *pCur,
                                 SQueryFilesInfo_rv *pVnodeFilesInfo, int32_t step) {
  pCur->fileId += step;

  int32_t fileIndex = 0;
  int32_t order = (step == QUERY_ASC_FORWARD_STEP) ? TSQL_SO_ASC : TSQL_SO_DESC;
  while (1) {
    fileIndex = vnodeGetVnodeHeaderFileIndex_(&pCur->fileId, order, pVnodeFilesInfo);

    // no files left, abort
    if (fileIndex < 0) {
      if (step == QUERY_ASC_FORWARD_STEP) {
        //        dTrace("QInfo:%p no more file to access, try data in cache", GET_QINFO_ADDR(pQuery));
      } else {
        //        dTrace("QInfo:%p no more file to access in desc order, query completed", GET_QINFO_ADDR(pQuery));
      }

      //      vnodeFreeFieldsEx(pRuntimeEnv);
      pCur->fileId = -1;
      break;
    }

    // failed to mmap header file into memory will cause the retrieval of compblock info failed
    SMeterObj *pMeterObj = *(SMeterObj **)taosArrayGet(pQueryHandle->pTableList, 0);
    if (vnodeGetCompBlockInfo_(pQueryHandle, pMeterObj, fileIndex) > 0) {
      break;
    }

    /*
     * 1. failed to read blk information from header file or open data file failed
     * 2. header file is empty
     *
     * try next one
     */
    pCur->fileId += step;

    /* for backwards search, if the first file is not valid, abort */
    if (step < 0 && fileIndex == 0) {
      //      vnodeFreeFieldsEx(pRuntimeEnv);
      pCur->fileId = -1;
      fileIndex = -1;
      break;
    }
  }

  return fileIndex;
}

void vnodeSetCurrentFileNames(SQueryFilesInfo *pVnodeFilesInfo) {
  assert(pVnodeFilesInfo->current >= 0 && pVnodeFilesInfo->current < pVnodeFilesInfo->numOfFiles);

  SHeaderFileInfo *pCurrentFileInfo = &pVnodeFilesInfo->pFileInfo[pVnodeFilesInfo->current];

  /*
   * set the full file path for current opened files
   * the maximum allowed path string length is PATH_MAX in Linux, 100 bytes is used to
   * suppress the compiler warnings
   */
  char    str[PATH_MAX + 100] = {0};
  int32_t PATH_WITH_EXTRA = PATH_MAX + 100;

  int32_t vnodeId = pVnodeFilesInfo->vnodeId;
  int32_t fileId = pCurrentFileInfo->fileID;

  int32_t len = snprintf(str, PATH_WITH_EXTRA, "%sv%df%d.head", pVnodeFilesInfo->dbFilePathPrefix, vnodeId, fileId);
  assert(len <= PATH_MAX);

  strncpy(pVnodeFilesInfo->headerFilePath, str, PATH_MAX);

  len = snprintf(str, PATH_WITH_EXTRA, "%sv%df%d.data", pVnodeFilesInfo->dbFilePathPrefix, vnodeId, fileId);
  assert(len <= PATH_MAX);

  strncpy(pVnodeFilesInfo->dataFilePath, str, PATH_MAX);

  len = snprintf(str, PATH_WITH_EXTRA, "%sv%df%d.last", pVnodeFilesInfo->dbFilePathPrefix, vnodeId, fileId);
  assert(len <= PATH_MAX);

  strncpy(pVnodeFilesInfo->lastFilePath, str, PATH_MAX);
}

void vnodeSetCurrentFileNames_(SQueryFilesInfo_rv *pVnodeFilesInfo) {
  assert(pVnodeFilesInfo->current >= 0 && pVnodeFilesInfo->current < taosArrayGetSize(pVnodeFilesInfo->pFileInfo));

  SHeaderFileInfo *pCurrentFileInfo = taosArrayGet(pVnodeFilesInfo->pFileInfo, pVnodeFilesInfo->current);

  /*
   * set the full file path for current opened files
   * the maximum allowed path string length is PATH_MAX in Linux, 100 bytes is used to
   * suppress the compiler warnings
   */
  char    str[PATH_MAX + 100] = {0};
  int32_t PATH_WITH_EXTRA = PATH_MAX + 100;

  int32_t vnodeId = pVnodeFilesInfo->vnodeId;
  int32_t fileId = pCurrentFileInfo->fileID;

  int32_t len = snprintf(str, PATH_WITH_EXTRA, "%sv%df%d.head", pVnodeFilesInfo->dbFilePathPrefix, vnodeId, fileId);
  assert(len <= PATH_MAX);

  strncpy(pVnodeFilesInfo->headerFilePath, str, PATH_MAX);

  len = snprintf(str, PATH_WITH_EXTRA, "%sv%df%d.data", pVnodeFilesInfo->dbFilePathPrefix, vnodeId, fileId);
  assert(len <= PATH_MAX);

  strncpy(pVnodeFilesInfo->dataFilePath, str, PATH_MAX);

  len = snprintf(str, PATH_WITH_EXTRA, "%sv%df%d.last", pVnodeFilesInfo->dbFilePathPrefix, vnodeId, fileId);
  assert(len <= PATH_MAX);

  strncpy(pVnodeFilesInfo->lastFilePath, str, PATH_MAX);
}

void doCloseQueryFileInfoFD(SQueryFilesInfo *pVnodeFilesInfo) {
  tclose(pVnodeFilesInfo->headerFd);
  tclose(pVnodeFilesInfo->dataFd);
  tclose(pVnodeFilesInfo->lastFd);

  pVnodeFilesInfo->current = -1;
  pVnodeFilesInfo->headerFileSize = -1;
}

void initQueryFileInfoFD(SQueryFilesInfo *pVnodeFilesInfo) {
  pVnodeFilesInfo->current = -1;
  pVnodeFilesInfo->headerFileSize = -1;

  pVnodeFilesInfo->headerFd = FD_INITIALIZER;  // set the initial value
  pVnodeFilesInfo->dataFd = FD_INITIALIZER;
  pVnodeFilesInfo->lastFd = FD_INITIALIZER;
}

void initQueryFileInfoFD_rv(SQueryFilesInfo_rv *pVnodeFilesInfo) {
  pVnodeFilesInfo->current = -1;
  pVnodeFilesInfo->headerFileSize = -1;

  pVnodeFilesInfo->headerFd = FD_INITIALIZER;  // set the initial value
  pVnodeFilesInfo->dataFd = FD_INITIALIZER;
  pVnodeFilesInfo->lastFd = FD_INITIALIZER;
}

/**
 * @param pSupporter
 * @param pQuery
 * @param numOfMeters
 * @param filePath
 * @param pMeterDataInfo
 * @return
 */
int32_t getDataBlocksForMeters(STableQuerySupportObj *pSupporter, SQuery *pQuery, int32_t numOfMeters,
                               const char *filePath, SMeterDataInfo **pMeterDataInfo, uint32_t *numOfBlocks) {
  SQInfo *           pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);
  SQueryCostSummary *pSummary = &pSupporter->runtimeEnv.summary;

  TSKEY minval, maxval;

  *numOfBlocks = 0;
  SQueryFilesInfo *pVnodeFileInfo = &pSupporter->runtimeEnv.vnodeFileInfo;

  // sequentially scan this header file to extract the compHeader info
  for (int32_t j = 0; j < numOfMeters; ++j) {
    SMeterObj *pMeterObj = pMeterDataInfo[j]->pMeterObj;

    lseek(pVnodeFileInfo->headerFd, pMeterDataInfo[j]->offsetInHeaderFile, SEEK_SET);

    SCompInfo compInfo = {0};
    read(pVnodeFileInfo->headerFd, &compInfo, sizeof(SCompInfo));

    int32_t ret = validateCompBlockInfoSegment(pQInfo, filePath, pMeterObj->vnode, &compInfo,
                                               pMeterDataInfo[j]->offsetInHeaderFile);
    if (ret != TSDB_CODE_SUCCESS) {  // file corrupted
      clearAllMeterDataBlockInfo(pMeterDataInfo, 0, numOfMeters);
      return TSDB_CODE_FILE_CORRUPTED;
    }

    if (compInfo.numOfBlocks <= 0 || compInfo.uid != pMeterDataInfo[j]->pMeterObj->uid) {
      clearAllMeterDataBlockInfo(pMeterDataInfo, 0, numOfMeters);
      continue;
    }

    int32_t size = compInfo.numOfBlocks * sizeof(SCompBlock);
    size_t  bufferSize = size + sizeof(TSCKSUM);

    pMeterDataInfo[j]->numOfBlocks = compInfo.numOfBlocks;
    char *p = realloc(pMeterDataInfo[j]->pBlock, bufferSize);
    if (p == NULL) {
      clearAllMeterDataBlockInfo(pMeterDataInfo, 0, numOfMeters);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    } else {
      memset(p, 0, bufferSize);
      pMeterDataInfo[j]->pBlock = (SCompBlock *)p;
    }

    read(pVnodeFileInfo->headerFd, pMeterDataInfo[j]->pBlock, bufferSize);
    TSCKSUM checksum = *(TSCKSUM *)((char *)pMeterDataInfo[j]->pBlock + size);

    int64_t st = taosGetTimestampUs();

    // check compblock integrity
    ret = validateCompBlockSegment(pQInfo, filePath, &compInfo, (char *)pMeterDataInfo[j]->pBlock, pMeterObj->vnode,
                                   checksum);
    if (ret != TSDB_CODE_SUCCESS) {
      clearAllMeterDataBlockInfo(pMeterDataInfo, 0, numOfMeters);
      return TSDB_CODE_FILE_CORRUPTED;
    }

    int64_t et = taosGetTimestampUs();

    pSummary->readCompInfo++;
    pSummary->totalCompInfoSize += (size + sizeof(SCompInfo) + sizeof(TSCKSUM));
    pSummary->loadCompInfoUs += (et - st);

    if (!setCurrentQueryRange(pMeterDataInfo[j], pQuery, pSupporter->rawEKey, &minval, &maxval)) {
      clearAllMeterDataBlockInfo(pMeterDataInfo, j, j + 1);
      continue;
    }

    int32_t end = 0;
    if (!getValidDataBlocksRangeIndex(pMeterDataInfo[j], pQuery, pMeterDataInfo[j]->pBlock, compInfo.numOfBlocks,
                                      minval, maxval, &end)) {
      // current table has no qualified data blocks, erase its information.
      clearAllMeterDataBlockInfo(pMeterDataInfo, j, j + 1);
      continue;
    }

    if (!setValidDataBlocks(pMeterDataInfo[j], end)) {
      clearAllMeterDataBlockInfo(pMeterDataInfo, 0, numOfMeters);

      pQInfo->killed = 1;  // set query kill, abort current query since no memory available
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }

    qTrace("QInfo:%p vid:%d sid:%d id:%s, startIndex:%d, %d blocks qualified", pQInfo, pMeterObj->vnode, pMeterObj->sid,
           pMeterObj->meterId, pMeterDataInfo[j]->start, pMeterDataInfo[j]->numOfBlocks);

    (*numOfBlocks) += pMeterDataInfo[j]->numOfBlocks;
  }

  return TSDB_CODE_SUCCESS;
}

static void freeDataBlockFieldInfo(SMeterDataBlockInfoEx *pDataBlockInfoEx, int32_t len) {
  for (int32_t i = 0; i < len; ++i) {
    tfree(pDataBlockInfoEx[i].pBlock.fields);
  }
}

void freeMeterBlockInfoEx(SMeterDataBlockInfoEx *pDataBlockInfoEx, int32_t len) {
  freeDataBlockFieldInfo(pDataBlockInfoEx, len);
  tfree(pDataBlockInfoEx);
}

typedef struct SBlockOrderSupporter {
  int32_t                 numOfMeters;
  SMeterDataBlockInfoEx **pDataBlockInfoEx;
  int32_t *               blockIndexArray;
  int32_t *               numOfBlocksPerMeter;
} SBlockOrderSupporter;

static int32_t blockAccessOrderComparator(const void *pLeft, const void *pRight, void *param) {
  int32_t leftTableIndex = *(int32_t *)pLeft;
  int32_t rightTableIndex = *(int32_t *)pRight;

  SBlockOrderSupporter *pSupporter = (SBlockOrderSupporter *)param;

  int32_t leftTableBlockIndex = pSupporter->blockIndexArray[leftTableIndex];
  int32_t rightTableBlockIndex = pSupporter->blockIndexArray[rightTableIndex];

  if (leftTableBlockIndex > pSupporter->numOfBlocksPerMeter[leftTableIndex]) {
    /* left block is empty */
    return 1;
  } else if (rightTableBlockIndex > pSupporter->numOfBlocksPerMeter[rightTableIndex]) {
    /* right block is empty */
    return -1;
  }

  SMeterDataBlockInfoEx *pLeftBlockInfoEx = &pSupporter->pDataBlockInfoEx[leftTableIndex][leftTableBlockIndex];
  SMeterDataBlockInfoEx *pRightBlockInfoEx = &pSupporter->pDataBlockInfoEx[rightTableIndex][rightTableBlockIndex];

  //    assert(pLeftBlockInfoEx->pBlock.compBlock->offset != pRightBlockInfoEx->pBlock.compBlock->offset);
  if (pLeftBlockInfoEx->pBlock.compBlock->offset == pRightBlockInfoEx->pBlock.compBlock->offset &&
      pLeftBlockInfoEx->pBlock.compBlock->last == pRightBlockInfoEx->pBlock.compBlock->last) {
    // todo add more information
    dError("error in header file, two block with same offset:%p", pLeftBlockInfoEx->pBlock.compBlock->offset);
  }

  return pLeftBlockInfoEx->pBlock.compBlock->offset > pRightBlockInfoEx->pBlock.compBlock->offset ? 1 : -1;
}

void cleanBlockOrderSupporter(SBlockOrderSupporter *pSupporter, int32_t numOfTables) {
  tfree(pSupporter->numOfBlocksPerMeter);
  tfree(pSupporter->blockIndexArray);

  for (int32_t i = 0; i < numOfTables; ++i) {
    tfree(pSupporter->pDataBlockInfoEx[i]);
  }

  tfree(pSupporter->pDataBlockInfoEx);
}

int32_t createDataBlocksInfoEx(SMeterDataInfo **pMeterDataInfo, int32_t numOfMeters,
                               SMeterDataBlockInfoEx **pDataBlockInfoEx, int32_t numOfCompBlocks,
                               int32_t *numOfAllocBlocks, int64_t addr) {
  // release allocated memory first
  freeDataBlockFieldInfo(*pDataBlockInfoEx, *numOfAllocBlocks);

  if (*numOfAllocBlocks == 0 || *numOfAllocBlocks < numOfCompBlocks) {
    char *tmp = realloc((*pDataBlockInfoEx), sizeof(SMeterDataBlockInfoEx) * numOfCompBlocks);
    if (tmp == NULL) {
      tfree(*pDataBlockInfoEx);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }

    *pDataBlockInfoEx = (SMeterDataBlockInfoEx *)tmp;
    memset((*pDataBlockInfoEx), 0, sizeof(SMeterDataBlockInfoEx) * numOfCompBlocks);
    *numOfAllocBlocks = numOfCompBlocks;
  }

  SBlockOrderSupporter supporter = {0};
  supporter.numOfMeters = numOfMeters;
  supporter.numOfBlocksPerMeter = calloc(1, sizeof(int32_t) * numOfMeters);
  supporter.blockIndexArray = calloc(1, sizeof(int32_t) * numOfMeters);
  supporter.pDataBlockInfoEx = calloc(1, POINTER_BYTES * numOfMeters);

  if (supporter.numOfBlocksPerMeter == NULL || supporter.blockIndexArray == NULL ||
      supporter.pDataBlockInfoEx == NULL) {
    cleanBlockOrderSupporter(&supporter, 0);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  int32_t cnt = 0;
  int32_t numOfQualMeters = 0;
  for (int32_t j = 0; j < numOfMeters; ++j) {
    if (pMeterDataInfo[j]->numOfBlocks == 0) {
      continue;
    }

    SCompBlock *pBlock = pMeterDataInfo[j]->pBlock;
    supporter.numOfBlocksPerMeter[numOfQualMeters] = pMeterDataInfo[j]->numOfBlocks;

    char *buf = calloc(1, sizeof(SMeterDataBlockInfoEx) * pMeterDataInfo[j]->numOfBlocks);
    if (buf == NULL) {
      cleanBlockOrderSupporter(&supporter, numOfQualMeters);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }

    supporter.pDataBlockInfoEx[numOfQualMeters] = (SMeterDataBlockInfoEx *)buf;

    for (int32_t k = 0; k < pMeterDataInfo[j]->numOfBlocks; ++k) {
      SMeterDataBlockInfoEx *pBlockInfoEx = &supporter.pDataBlockInfoEx[numOfQualMeters][k];

      pBlockInfoEx->pBlock.compBlock = &pBlock[k];
      pBlockInfoEx->pBlock.fields = NULL;

      pBlockInfoEx->pMeterDataInfo = pMeterDataInfo[j];
      pBlockInfoEx->groupIdx = pMeterDataInfo[j]->groupIdx;     // set the group index
      pBlockInfoEx->blockIndex = pMeterDataInfo[j]->start + k;  // set the block index in original meter
      cnt++;
    }

    numOfQualMeters++;
  }

  dTrace("QInfo %p create data blocks info struct completed", addr);

  assert(cnt == numOfCompBlocks && numOfQualMeters <= numOfMeters);  // the pMeterDataInfo[j]->numOfBlocks may be 0
  supporter.numOfMeters = numOfQualMeters;
  SLoserTreeInfo *pTree = NULL;

  uint8_t ret = tLoserTreeCreate(&pTree, supporter.numOfMeters, &supporter, blockAccessOrderComparator);
  if (ret != TSDB_CODE_SUCCESS) {
    cleanBlockOrderSupporter(&supporter, numOfMeters);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  int32_t numOfTotal = 0;

  while (numOfTotal < cnt) {
    int32_t                pos = pTree->pNode[0].index;
    SMeterDataBlockInfoEx *pBlocksInfoEx = supporter.pDataBlockInfoEx[pos];
    int32_t                index = supporter.blockIndexArray[pos]++;

    (*pDataBlockInfoEx)[numOfTotal++] = pBlocksInfoEx[index];

    // set data block index overflow, in order to disable the offset comparator
    if (supporter.blockIndexArray[pos] >= supporter.numOfBlocksPerMeter[pos]) {
      supporter.blockIndexArray[pos] = supporter.numOfBlocksPerMeter[pos] + 1;
    }

    tLoserTreeAdjust(pTree, pos + supporter.numOfMeters);
  }

  /*
   * available when no import exists
   * for(int32_t i = 0; i < cnt - 1; ++i) {
   *   assert((*pDataBlockInfoEx)[i].pBlock.compBlock->offset < (*pDataBlockInfoEx)[i+1].pBlock.compBlock->offset);
   * }
   */

  dTrace("QInfo %p %d data blocks sort completed", addr, cnt);
  cleanBlockOrderSupporter(&supporter, numOfMeters);
  free(pTree);

  return TSDB_CODE_SUCCESS;
}

static bool cacheBoundaryCheck(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj) {
  /*
   * here we get the first slot from the meter cache, not from the cache snapshot from pQuery, since the
   * snapshot value in pQuery may have been expired now.
   */
  SQuery *pQuery = pRuntimeEnv->pQuery;

  SCacheInfo * pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  SCacheBlock *pBlock = NULL;

  // earliest key in cache
  TSKEY keyFirst = 0;
  TSKEY keyLast = pMeterObj->lastKey;

  // keep the value in local variable, since it may be changed by other thread any time
  int32_t numOfBlocks = pCacheInfo->numOfBlocks;
  int32_t currentSlot = pCacheInfo->currentSlot;

  // no data in cache, return false directly
  if (numOfBlocks == 0) {
    return false;
  }

  int32_t first = getFirstCacheSlot(numOfBlocks, currentSlot, pCacheInfo);

  while (1) {
    /*
     * pBlock may be null value since this block is flushed to disk, and re-distributes to
     * other meter, so go on until we get the first not flushed cache block.
     */
    if ((pBlock = getCacheDataBlock(pMeterObj, pRuntimeEnv, first)) != NULL) {
      keyFirst = getTimestampInCacheBlock(pRuntimeEnv, pBlock, 0);
      break;
    } else {
      /*
       * there may be only one empty cache block existed caused by import.
       */
      if (first == currentSlot || numOfBlocks == 1) {
        return false;
      }

      // todo use defined macro
      first = (first + 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
    }
  }

  TSKEY min, max;
  getQueryRange(pQuery, &min, &max);

  /*
   * The query time range is earlier than the first element or later than the last elements in cache.
   * If the query window overlaps with the time range of disk files, the flag needs to be reset.
   * Otherwise, this flag will cause error in following processing.
   */
  if (max < keyFirst || min > keyLast) {
    setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    return false;
  }

  return true;
}

static bool cacheBoundaryCheck_(STsdbQueryHandle *pQueryHandle, SMeterObj *pMeterObj) {
  /*
   * here we get the first slot from the meter cache, not from the cache snapshot from pQuery, since the
   * snapshot value in pQuery may have been expired now.
   */
  SCacheInfo * pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  SCacheBlock *pBlock = NULL;

  // earliest key in cache
  TSKEY keyFirst = 0;
  TSKEY keyLast = pMeterObj->lastKey;

  // keep the value in local variable, since it may be changed by other thread any time
  int32_t numOfBlocks = pCacheInfo->numOfBlocks;
  int32_t currentSlot = pCacheInfo->currentSlot;

  // no data in cache, return false directly
  if (numOfBlocks == 0) {
    return false;
  }

  int32_t first = getFirstCacheSlot(numOfBlocks, currentSlot, pCacheInfo);

  while (1) {
    /*
     * pBlock may be null value since this block is flushed to disk, and re-distributes to
     * other meter, so go on until we get the first not flushed cache block.
     */
    if ((pBlock = getCacheDataBlock_(pQueryHandle, pMeterObj, first)) != NULL) {
      keyFirst = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pBlock, 0);
      break;
    } else {
      /*
       * there may be only one empty cache block existed caused by import.
       */
      if (first == currentSlot || numOfBlocks == 1) {
        return false;
      }

      // todo use defined macro
      first = (first + 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
    }
  }

  //  TSKEY min, max;
  //  getQueryRange(pQuery, &min, &max);

  /*
   * The query time range is earlier than the first element or later than the last elements in cache.
   * If the query window overlaps with the time range of disk files, the flag needs to be reset.
   * Otherwise, this flag will cause error in following processing.
   */
  if (pQueryHandle->window.ekey < keyFirst || pQueryHandle->window.skey > keyLast) {
    //    setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    return false;
  }

  return true;
}

int64_t getQueryStartPositionInCache(SQueryRuntimeEnv *pRuntimeEnv, int32_t *slot, int32_t *pos,
                                     bool ignoreQueryRange) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  pQuery->fileId = -1;
  vnodeFreeFieldsEx(pRuntimeEnv);

  // keep in-memory cache status in local variables in case that it may be changed by write operation
  getBasicCacheInfoSnapshot(pQuery, pMeterObj->pCache, pMeterObj->vnode);

  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  if (pCacheInfo == NULL || pCacheInfo->cacheBlocks == NULL || pQuery->numOfBlocks == 0) {
    setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    return -1;
  }

  assert((pQuery->lastKey >= pQuery->skey && QUERY_IS_ASC_QUERY(pQuery)) ||
         (pQuery->lastKey <= pQuery->skey && !QUERY_IS_ASC_QUERY(pQuery)));

  if (!ignoreQueryRange && !cacheBoundaryCheck(pRuntimeEnv, pMeterObj)) {
    return -1;
  }

  /* find the appropriated slot that contains the requested points */
  TSKEY rawskey = pQuery->skey;

  /* here we actual start to query from pQuery->lastKey */
  pQuery->skey = pQuery->lastKey;

  (*slot) = binarySearchInCacheBlk(pCacheInfo, pQuery, TSDB_KEYSIZE, pQuery->firstSlot, pQuery->currentSlot);

  /* locate the first point of which time stamp is no less than pQuery->skey */
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

  pQuery->slot = *slot;

  // cache block has been flushed to disk, no required data block in cache.
  SCacheBlock *pBlock = getCacheDataBlock(pMeterObj, pRuntimeEnv, pQuery->slot);
  if (pBlock == NULL) {
    pQuery->skey = rawskey;  // restore the skey
    return -1;
  }

  (*pos) = searchFn(pRuntimeEnv->primaryColBuffer->data, pBlock->numOfPoints, pQuery->skey, pQuery->order.order);

  // restore skey before return
  pQuery->skey = rawskey;

  // all data are less(greater) than the pQuery->lastKey in case of ascending(descending) query
  if (*pos == -1) {
    return -1;
  }

  int64_t nextKey = getTimestampInCacheBlock(pRuntimeEnv, pBlock, *pos);
  if ((nextKey < pQuery->lastKey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (nextKey > pQuery->lastKey && !QUERY_IS_ASC_QUERY(pQuery))) {
    // all data are less than the pQuery->lastKey(pQuery->sKey) for asc query
    return -1;
  }

  SET_CACHE_BLOCK_FLAG(pRuntimeEnv->blockStatus);
  return nextKey;
}

int64_t getQueryStartPositionInCache_rv(STsdbQueryHandle *pQueryHandle, int32_t *slot, int32_t *pos,
                                        bool ignoreQueryRange) {
  //  pQuery->fileId = -1;
  //  vnodeFreeFieldsEx(pRuntimeEnv);

  // keep in-memory cache status in local variables in case that it may be changed by write operation
  SMeterObj *pTable = *(SMeterObj **)taosArrayGet(pQueryHandle->pTableList, 0);
  getBasicCacheInfoSnapshot_(pQueryHandle, pTable->pCache, pTable->vnode);

  SCacheInfo *pCacheInfo = (SCacheInfo *)pTable->pCache;
  if (pCacheInfo == NULL || pCacheInfo->cacheBlocks == NULL || pQueryHandle->numOfCacheBlocks == 0) {
    //      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
    return -1;
  }

  //  assert((pQuery->lastKey >= pQuery->skey && QUERY_IS_ASC_QUERY(pQuery)) ||
  //      (pQuery->lastKey <= pQuery->skey && !QUERY_IS_ASC_QUERY(pQuery)));
  //
  if (!ignoreQueryRange && !cacheBoundaryCheck_(pQueryHandle, pTable)) {
    return -1;
  }

  /* find the appropriated slot that contains the requested points */

  /* here we actual start to query from pQuery->lastKey */
  //  pQuery->skey = pQueryHandle->lastKey;

  (*slot) = binarySearchInCacheBlk_(pCacheInfo, pQueryHandle->order, pQueryHandle->lastKey, TSDB_KEYSIZE,
                                    pQueryHandle->firstSlot, pQueryHandle->currentSlot);

  /* locate the first point of which time stamp is no less than pQuery->skey */
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[0];

  pQueryHandle->cur.slot = *slot;

  // cache block has been flushed to disk, no required data block in cache.
  SCacheBlock *pBlock = getCacheDataBlock_(pQueryHandle, pTable, pQueryHandle->cur.slot);
  if (pBlock == NULL) {
    return -1;
  }

  (*pos) = searchFn(pQueryHandle->tsBuf->data, pBlock->numOfPoints, pQueryHandle->window.skey, pQueryHandle->order);

  // restore skey before return
  //  pQuery->skey = rawskey;

  // all data are less(greater) than the pQuery->lastKey in case of ascending(descending) query
  if (*pos == -1) {
    return -1;
  }

  int64_t nextKey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pBlock, *pos);
  //  if ((nextKey < pQuery->lastKey && QUERY_IS_ASC_QUERY(pQuery)) ||
  //      (nextKey > pQuery->lastKey && !QUERY_IS_ASC_QUERY(pQuery))) {
  //     all data are less than the pQuery->lastKey(pQuery->sKey) for asc query
  //    return -1;
  //  }

  //  SET_CACHE_BLOCK_FLAG(pRuntimeEnv->blockStatus);
  return nextKey;
}

/**
 * if sfields is null
 * 1. count(*)/spread(ts) is invoked
 * 2. this column does not exists
 *
 * first filter the data block according to the value filter condition, then, if the top/bottom query applied,
 * invoke the filter function to decide if the data block need to be accessed or not.
 * TODO handle the whole data block is NULL situation
 * @param pQuery
 * @param pField
 * @return
 */
static bool needToLoadDataBlock(SQuery *pQuery, SField *pField, SQLFunctionCtx *pCtx, int32_t numOfTotalPoints) {
  if (pField == NULL) {
    return false;  // no need to load data
  }

  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    int32_t                  colIndex = pFilterInfo->info.colIdx;

    // this column not valid in current data block
    if (colIndex < 0 || pField[colIndex].colId != pFilterInfo->info.data.colId) {
      continue;
    }

    // not support pre-filter operation on binary/nchar data type
    if (!vnodeSupportPrefilter(pFilterInfo->info.data.type)) {
      continue;
    }

    // all points in current column are NULL, no need to check its boundary value
    if (pField[colIndex].numOfNullPoints == numOfTotalPoints) {
      continue;
    }

    if (pFilterInfo->info.data.type == TSDB_DATA_TYPE_FLOAT) {
      float minval = *(double *)(&pField[colIndex].min);
      float maxval = *(double *)(&pField[colIndex].max);

      for (int32_t i = 0; i < pFilterInfo->numOfFilters; ++i) {
        if (pFilterInfo->pFilters[i].fp(&pFilterInfo->pFilters[i], (char *)&minval, (char *)&maxval)) {
          return true;
        }
      }
    } else {
      for (int32_t i = 0; i < pFilterInfo->numOfFilters; ++i) {
        if (pFilterInfo->pFilters[i].fp(&pFilterInfo->pFilters[i], (char *)&pField[colIndex].min,
                                        (char *)&pField[colIndex].max)) {
          return true;
        }
      }
    }
  }

  // todo disable this opt code block temporarily
  //  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
  //    int32_t functId = pQuery->pSelectExpr[i].pBase.functionId;
  //    if (functId == TSDB_FUNC_TOP || functId == TSDB_FUNC_BOTTOM) {
  //      return top_bot_datablock_filter(&pCtx[i], functId, (char *)&pField[i].min, (char *)&pField[i].max);
  //    }
  //  }

  return true;
}

bool getNeighborPoints(STableQuerySupportObj *pSupporter, SMeterObj *pMeterObj,
                       SPointInterpoSupporter *pPointInterpSupporter) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (!isPointInterpoQuery(pQuery)) {
    return false;
  }

  /*
   * for interpolate point query, points that are directly before/after the specified point are required
   */
  if (isFirstLastRowQuery(pQuery)) {
    assert(!QUERY_IS_ASC_QUERY(pQuery));
  } else {
    assert(QUERY_IS_ASC_QUERY(pQuery));
  }
  assert(pPointInterpSupporter != NULL && pQuery->skey == pQuery->ekey);

  SCacheBlock *pBlock = NULL;

  qTrace("QInfo:%p get next data point, fileId:%d, slot:%d, pos:%d", GET_QINFO_ADDR(pQuery), pQuery->fileId,
         pQuery->slot, pQuery->pos);

  // save the point that is directly after or equals to the specified point
  getOneRowFromDataBlock(pRuntimeEnv, pPointInterpSupporter->pNextPoint, pQuery->pos);

  /*
   * 1. for last_row query, return immediately.
   * 2. the specified timestamp equals to the required key, interpolation according to neighbor points is not necessary
   *    for interp query.
   */
  TSKEY actualKey = *(TSKEY *)pPointInterpSupporter->pNextPoint[0];
  if (isFirstLastRowQuery(pQuery) || actualKey == pQuery->skey) {
    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

    /*
     * the retrieved ts may not equals to pMeterObj->lastKey due to cache re-allocation
     * set the pQuery->ekey/pQuery->skey/pQuery->lastKey to be the new value.
     */
    if (pQuery->ekey != actualKey) {
      pQuery->skey = actualKey;
      pQuery->ekey = actualKey;
      pQuery->lastKey = actualKey;
      pSupporter->rawSKey = actualKey;
      pSupporter->rawEKey = actualKey;
    }
    return true;
  }

  /* the qualified point is not the first point in data block */
  if (pQuery->pos > 0) {
    int32_t prevPos = pQuery->pos - 1;

    /* save the point that is directly after the specified point */
    getOneRowFromDataBlock(pRuntimeEnv, pPointInterpSupporter->pPrevPoint, prevPos);
  } else {
    __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

    savePointPosition(&pRuntimeEnv->startPos, pQuery->fileId, pQuery->slot, pQuery->pos);

    // backwards movement would not set the pQuery->pos correct. We need to set it manually later.
    moveToNextBlock(pRuntimeEnv, QUERY_DESC_FORWARD_STEP, searchFn, true);

    /*
     * no previous data exists.
     * reset the status and load the data block that contains the qualified point
     */
    if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
      dTrace("QInfo:%p no previous data block, start fileId:%d, slot:%d, pos:%d, qrange:%" PRId64 "-%" PRId64
             ", out of range",
             GET_QINFO_ADDR(pQuery), pRuntimeEnv->startPos.fileId, pRuntimeEnv->startPos.slot,
             pRuntimeEnv->startPos.pos, pQuery->skey, pQuery->ekey);

      // no result, return immediately
      setQueryStatus(pQuery, QUERY_COMPLETED);
      return false;
    } else {  // prev has been located
      if (pQuery->fileId >= 0) {
        pQuery->pos = pQuery->pBlock[pQuery->slot].numOfPoints - 1;
        getOneRowFromDataBlock(pRuntimeEnv, pPointInterpSupporter->pPrevPoint, pQuery->pos);

        qTrace("QInfo:%p get prev data point, fileId:%d, slot:%d, pos:%d, pQuery->pos:%d", GET_QINFO_ADDR(pQuery),
               pQuery->fileId, pQuery->slot, pQuery->pos, pQuery->pos);
      } else {
        // moveToNextBlock make sure there is a available cache block, if exists
        assert(vnodeIsDatablockLoaded(pRuntimeEnv, pMeterObj, -1, true) == DISK_BLOCK_NO_NEED_TO_LOAD);
        pBlock = &pRuntimeEnv->cacheBlock;

        pQuery->pos = pBlock->numOfPoints - 1;
        getOneRowFromDataBlock(pRuntimeEnv, pPointInterpSupporter->pPrevPoint, pQuery->pos);

        qTrace("QInfo:%p get prev data point, fileId:%d, slot:%d, pos:%d, pQuery->pos:%d", GET_QINFO_ADDR(pQuery),
               pQuery->fileId, pQuery->slot, pBlock->numOfPoints - 1, pQuery->pos);
      }
    }
  }

  pQuery->skey = *(TSKEY *)pPointInterpSupporter->pPrevPoint[0];
  pQuery->ekey = *(TSKEY *)pPointInterpSupporter->pNextPoint[0];
  pQuery->lastKey = pQuery->skey;

  return true;
}

/**
 * check if data in disk.
 */
bool hasDataInDisk(SQuery *pQuery, SMeterObj *pMeterObj) {
  SVnodeObj *pVnode = &vnodeList[pMeterObj->vnode];
  if (pVnode->numOfFiles <= 0) {
    pQuery->fileId = -1;
    return false;
  }

  int64_t latestKey = pMeterObj->lastKeyOnFile;
  int64_t oldestKey = getOldestKey(pVnode->numOfFiles, pVnode->fileId, &pVnode->cfg);

  TSKEY min, max;
  getQueryRange(pQuery, &min, &max);

  /* query range is out of current time interval of table */
  if ((min > latestKey) || (max < oldestKey)) {
    pQuery->fileId = -1;
    return false;
  }

  return true;
}
bool hasDataInDisk_rv(STimeWindow window, SMeterObj *pMeterObj) {
  SVnodeObj *pVnode = &vnodeList[pMeterObj->vnode];
  if (pVnode->numOfFiles <= 0) {
    return false;
  }

  int64_t latestKey = pMeterObj->lastKeyOnFile;
  int64_t oldestKey = getOldestKey(pVnode->numOfFiles, pVnode->fileId, &pVnode->cfg);

  /* query range is out of current time interval of table */
  if ((window.skey > latestKey) || (window.ekey < oldestKey)) {
    return false;
  }

  return true;
}

bool hasDataInCache(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj) {
  SQuery *    pQuery = pRuntimeEnv->pQuery;
  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;

  /* no data in cache, return */
  if ((pCacheInfo == NULL) || (pCacheInfo->cacheBlocks == NULL)) {
    return false;
  }

  /* numOfBlocks value has been overwrite, release pFields data if exists */
  vnodeFreeFieldsEx(pRuntimeEnv);
  getBasicCacheInfoSnapshot(pQuery, pCacheInfo, pMeterObj->vnode);
  if (pQuery->numOfBlocks <= 0) {
    return false;
  }

  return cacheBoundaryCheck(pRuntimeEnv, pMeterObj);
}

bool hasDataInCache_(STsdbQueryHandle *pQueryHandle, SMeterObj *pMeterObj) {
  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;

  /* no data in cache, return */
  if ((pCacheInfo == NULL) || (pCacheInfo->cacheBlocks == NULL) || (pCacheInfo->numOfBlocks == 0)) {
    return false;
  }

  /* numOfBlocks value has been overwrite, release pFields data if exists */
  //  vnodeFreeFieldsEx(pRuntimeEnv);
  getBasicCacheInfoSnapshot_(pQueryHandle, pCacheInfo, pMeterObj->vnode);
  if (pQueryHandle->numOfBlocks <= 0) {
    return false;
  }

  return cacheBoundaryCheck_(pQueryHandle, pMeterObj);
}

bool getQualifiedDataBlock(SMeterObj *pMeterObj, SQueryRuntimeEnv *pRuntimeEnv, int32_t type,
                           __block_search_fn_t searchFn) {
  int32_t blkIdx = -1;
  int32_t fid = -1;
  int32_t step = (type == QUERY_RANGE_GREATER_EQUAL) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP;

  SQuery *pQuery = pRuntimeEnv->pQuery;
  pQuery->slot = -1;

  TSKEY key = pQuery->lastKey;

  SData *primaryColBuffer = pRuntimeEnv->primaryColBuffer;
  pQuery->fileId = getFileIdFromKey(pMeterObj->vnode, key) - step;

  while (1) {
    if ((fid = getNextDataFileCompInfo(pRuntimeEnv, pMeterObj, step)) < 0) {
      break;
    }

    blkIdx = binarySearchForBlock(pQuery, key);

    if (type == QUERY_RANGE_GREATER_EQUAL) {
      if (key <= pQuery->pBlock[blkIdx].keyLast) {
        break;
      } else {
        blkIdx = -1;
      }
    } else {
      if (key >= pQuery->pBlock[blkIdx].keyFirst) {
        break;
      } else {
        blkIdx = -1;
      }
    }
  }

  /* failed to find qualified point in file, abort */
  if (blkIdx == -1) {
    return false;
  }

  assert(blkIdx >= 0 && blkIdx < pQuery->numOfBlocks);

  // load first data block into memory failed, caused by disk block error
  bool blockLoaded = false;
  while (blkIdx < pQuery->numOfBlocks && blkIdx >= 0) {
    pQuery->slot = blkIdx;
    if (loadDataBlockIntoMem(&pQuery->pBlock[pQuery->slot], &pQuery->pFields[pQuery->slot], pRuntimeEnv, fid, true,
                             true) == 0) {
      SET_DATA_BLOCK_LOADED(pRuntimeEnv->blockStatus);
      blockLoaded = true;
      break;
    }

    dError("QInfo:%p fileId:%d total numOfBlks:%d blockId:%d load into memory failed due to error in disk files",
           GET_QINFO_ADDR(pQuery), pQuery->fileId, pQuery->numOfBlocks, blkIdx);
    blkIdx += step;
  }

  // failed to load data from disk, abort current query
  if (blockLoaded == false) {
    return false;
  }

  SCompBlock *pBlocks = getDiskDataBlock(pQuery, blkIdx);

  // search qualified points in blk, according to primary key (timestamp) column
  pQuery->pos = searchFn(primaryColBuffer->data, pBlocks->numOfPoints, key, pQuery->order.order);
  assert(pQuery->pos >= 0 && pQuery->fileId >= 0 && pQuery->slot >= 0);

  return true;
}

static bool getValidDataBlocksRangeIndex(SMeterDataInfo *pMeterDataInfo, SQuery *pQuery, SCompBlock *pCompBlock,
                                         int64_t numOfBlocks, TSKEY minval, TSKEY maxval, int32_t *end) {
  SMeterObj *pMeterObj = pMeterDataInfo->pMeterObj;
  SQInfo *   pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);

  /*
   * search the possible blk that may satisfy the query condition always start from the min value, therefore,
   * the order is always ascending order
   */
  pMeterDataInfo->start = binarySearchForBlockImpl(pCompBlock, (int32_t)numOfBlocks, minval, TSQL_SO_ASC);
  if (minval > pCompBlock[pMeterDataInfo->start].keyLast || maxval < pCompBlock[pMeterDataInfo->start].keyFirst) {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, no result in files", pQInfo, pMeterObj->vnode, pMeterObj->sid,
           pMeterObj->meterId);
    return false;
  }

  // incremental checks following blocks until whose time range does not overlap with the query range
  *end = pMeterDataInfo->start;
  while (*end <= (numOfBlocks - 1)) {
    if (pCompBlock[*end].keyFirst <= maxval && pCompBlock[*end].keyLast >= maxval) {
      break;
    }

    if (pCompBlock[*end].keyFirst > maxval) {
      *end -= 1;
      break;
    }

    if (*end == numOfBlocks - 1) {
      break;
    } else {
      ++(*end);
    }
  }

  return true;
}

bool setCurrentQueryRange(SMeterDataInfo *pMeterDataInfo, SQuery *pQuery, TSKEY endKey, TSKEY *minval, TSKEY *maxval) {
  SQInfo *         pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);
  SMeterObj *      pMeterObj = pMeterDataInfo->pMeterObj;
  SMeterQueryInfo *pMeterQInfo = pMeterDataInfo->pMeterQInfo;

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    *minval = pMeterQInfo->lastKey;
    *maxval = endKey;
  } else {
    *minval = endKey;
    *maxval = pMeterQInfo->lastKey;
  }

  if (*minval > *maxval) {
    qTrace("QInfo:%p vid:%d sid:%d id:%s, no result in files, qrange:%" PRId64 "-%" PRId64 ", lastKey:%" PRId64, pQInfo,
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pMeterQInfo->skey, pMeterQInfo->ekey,
           pMeterQInfo->lastKey);
    return false;
  } else {
    qTrace("QInfo:%p vid:%d sid:%d id:%s, query in files, qrange:%" PRId64 "-%" PRId64 ", lastKey:%" PRId64, pQInfo,
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pMeterQInfo->skey, pMeterQInfo->ekey,
           pMeterQInfo->lastKey);
    return true;
  }
}

// todo all functions that call this function should check the returned data blocks status
SCacheBlock *getCacheDataBlock(SMeterObj *pMeterObj, SQueryRuntimeEnv *pRuntimeEnv, int32_t slot) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  if (pCacheInfo == NULL || pCacheInfo->cacheBlocks == NULL || slot < 0 || slot >= pCacheInfo->maxBlocks) {
    return NULL;
  }

  vnodeFreeFields(pQuery);
  getBasicCacheInfoSnapshot(pQuery, pCacheInfo, pMeterObj->vnode);

  SCacheBlock *pBlock = pCacheInfo->cacheBlocks[slot];
  if (pBlock == NULL) {  // the cache info snapshot must be existed.
    int32_t curNumOfBlocks = pCacheInfo->numOfBlocks;
    int32_t curSlot = pCacheInfo->currentSlot;

    dError(
        "QInfo:%p NULL Block In Cache, snapshot (available blocks:%d, last block:%d), current (available blocks:%d, "
        "last block:%d), accessed null block:%d, pBlockId:%d",
        GET_QINFO_ADDR(pQuery), pQuery->numOfBlocks, pQuery->currentSlot, curNumOfBlocks, curSlot, slot,
        pQuery->blockId);

    return NULL;
  }

  // block is empty or block does not belongs to current table, return NULL value
  if (!isCacheBlockValid(pQuery, pBlock, pMeterObj, slot)) {
    return NULL;
  }

  // the accessed cache block has been loaded already, return directly
  if (vnodeIsDatablockLoaded(pRuntimeEnv, pMeterObj, -1, true) == DISK_BLOCK_NO_NEED_TO_LOAD) {
    TSKEY skey = getTimestampInCacheBlock(pRuntimeEnv, pBlock, 0);
    TSKEY ekey = getTimestampInCacheBlock(pRuntimeEnv, pBlock, pBlock->numOfPoints - 1);

    dTrace(
        "QInfo:%p vid:%d sid:%d id:%s, fileId:%d, cache block has been loaded, no need to load again, ts:%d, "
        "slot:%d, brange:%lld-%lld, rows:%d",
        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, 1, pQuery->slot,
        skey, ekey, pBlock->numOfPoints);

    return &pRuntimeEnv->cacheBlock;
  }

  // keep the structure as well as the block data into local buffer
  memcpy(&pRuntimeEnv->cacheBlock, pBlock, sizeof(SCacheBlock));

  SCacheBlock *pNewBlock = &pRuntimeEnv->cacheBlock;

  int32_t offset = 0;
  int32_t numOfPoints = pNewBlock->numOfPoints;

  // the commit data points will be ignored
  if (slot == pQuery->commitSlot) {
    assert(pQuery->commitPoint >= 0 && pQuery->commitPoint <= pNewBlock->numOfPoints);

    offset = pQuery->commitPoint;
    numOfPoints = pNewBlock->numOfPoints - offset;
    pNewBlock->numOfPoints = numOfPoints;

    if (offset != 0) {
      if (pNewBlock->numOfPoints > 0) {
        dTrace(
            "QInfo:%p ignore the data in cache block that are commit already, numOfBlock:%d slot:%d ignore points:%d "
            "remain:%d first:%d last:%d",
            GET_QINFO_ADDR(pQuery), pQuery->numOfBlocks, pQuery->slot, pQuery->commitPoint, pNewBlock->numOfPoints,
            pQuery->firstSlot, pQuery->currentSlot);
      } else {
        // current block are all commit already, ignore it
        assert(pNewBlock->numOfPoints == 0);
        dTrace(
            "QInfo:%p ignore points in cache block that are all commit already, numOfBlock:%d slot:%d ignore points:%d "
            "first:%d last:%d",
            GET_QINFO_ADDR(pQuery), pQuery->numOfBlocks, slot, offset, pQuery->firstSlot, pQuery->currentSlot);
        return NULL;
      }
    }
  }

  // keep the data from in cache into the temporarily allocated buffer
  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    SColumnInfoEx *pColumnInfoEx = &pQuery->colList[i];

    int16_t columnIndex = pColumnInfoEx->colIdx;
    int16_t columnIndexInBuf = pColumnInfoEx->colIdxInBuf;

    SColumn *pCol = &pMeterObj->schema[columnIndex];

    int16_t bytes = pCol->bytes;
    int16_t type = pCol->type;

    char *dst = pRuntimeEnv->colDataBuffer[columnIndexInBuf]->data;

    if (pQuery->colList[i].colIdx != -1) {
      assert(pCol->colId == pQuery->colList[i].data.colId && bytes == pColumnInfoEx->data.bytes &&
             type == pColumnInfoEx->data.type);

      memcpy(dst, pBlock->offset[columnIndex] + offset * bytes, numOfPoints * bytes);
    } else {
      setNullN(dst, type, bytes, numOfPoints);
    }
  }

  assert(numOfPoints == pNewBlock->numOfPoints);

  // if the primary timestamp are not loaded by default, always load it here into buffer
  if (!PRIMARY_TSCOL_LOADED(pQuery)) {
    memcpy(pRuntimeEnv->primaryColBuffer->data, pBlock->offset[0] + offset * TSDB_KEYSIZE, TSDB_KEYSIZE * numOfPoints);
  }

  pQuery->fileId = -1;
  pQuery->slot = slot;

  if (!isCacheBlockValid(pQuery, pNewBlock, pMeterObj, slot)) {
    return NULL;
  }

  /*
   * the accessed cache block still belongs to current meterObj, go on
   * update the load data block info
   */
  vnodeSetDataBlockInfoLoaded(pRuntimeEnv, pMeterObj, -1, true);

  TSKEY skey = getTimestampInCacheBlock(pRuntimeEnv, pNewBlock, 0);
  TSKEY ekey = getTimestampInCacheBlock(pRuntimeEnv, pNewBlock, numOfPoints - 1);

  dTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d, load cache block, ts:%d, slot:%d, brange:%lld-%lld, rows:%d",
         GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, 1, pQuery->slot,
         skey, ekey, numOfPoints);

  return pNewBlock;
}

bool isCacheBlockValid_(STsdbQueryHandle *pQueryHandle, SCacheBlock *pBlock, SMeterObj *pMeterObj, int32_t slot);

// todo all functions that call this function should check the returned data blocks status
SCacheBlock *getCacheDataBlock_(STsdbQueryHandle *pQueryHandle, SMeterObj *pMeterObj, int32_t slot) {
  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  if (pCacheInfo == NULL || pCacheInfo->cacheBlocks == NULL || slot < 0 || slot >= pCacheInfo->maxBlocks) {
    return NULL;
  }

  getBasicCacheInfoSnapshot_(pQueryHandle, pCacheInfo, pMeterObj->vnode);

  SCacheBlock *pBlock = pCacheInfo->cacheBlocks[slot];
  if (pBlock == NULL) {  // the cache info snapshot must be existed.
    int32_t curNumOfBlocks = pCacheInfo->numOfBlocks;
    int32_t curSlot = pCacheInfo->currentSlot;

    //    dError(
    //        "QInfo:%p NULL Block In Cache, snapshot (available blocks:%d, last block:%d), current (available
    //        blocks:%d, " "last block:%d), accessed null block:%d, pBlockId:%d", GET_QINFO_ADDR(pQuery),
    //        pQuery->numOfBlocks, pQuery->currentSlot, curNumOfBlocks, curSlot, slot, pQuery->blockId);

    return NULL;
  }

  // block is empty or block does not belongs to current table, return NULL value
  if (!isCacheBlockValid_(pQueryHandle, pBlock, pMeterObj, slot)) {
    return NULL;
  }

  // the accessed cache block has been loaded already, return directly
  if (vnodeIsDatablockLoaded_(&pQueryHandle->cur, &pQueryHandle->dataBlockLoadInfo, pMeterObj, NULL) ==
      DISK_BLOCK_NO_NEED_TO_LOAD) {
    TSKEY skey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pBlock, 0);
    TSKEY ekey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pBlock, pBlock->numOfPoints - 1);

    //    dTrace(
    //        "QInfo:%p vid:%d sid:%d id:%s, fileId:%d, cache block has been loaded, no need to load again, ts:%d, "
    //        "slot:%d, brange:%lld-%lld, rows:%d",
    //        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, 1,
    //        pQuery->slot, skey, ekey, pBlock->numOfPoints);

    return &pQueryHandle->cacheBlock;
  }

  // keep the structure as well as the block data into local buffer
  memcpy(&pQueryHandle->cacheBlock, pBlock, sizeof(SCacheBlock));

  SCacheBlock *pNewBlock = &pQueryHandle->cacheBlock;

  int32_t offset = 0;
  int32_t numOfPoints = pNewBlock->numOfPoints;

  // the commit data points will be ignored
  if (slot == pQueryHandle->commitSlot) {
    assert(pQueryHandle->commitPoint >= 0 && pQueryHandle->commitPoint <= pNewBlock->numOfPoints);

    offset = pQueryHandle->commitPoint;
    numOfPoints = pNewBlock->numOfPoints - offset;
    pNewBlock->numOfPoints = numOfPoints;

    if (offset != 0) {
      if (pNewBlock->numOfPoints > 0) {
        //        dTrace(
        //            "QInfo:%p ignore the data in cache block that are commit already, numOfBlock:%d slot:%d ignore
        //            points:%d " "remain:%d first:%d last:%d", GET_QINFO_ADDR(pQuery), pQuery->numOfBlocks,
        //            pQuery->slot, pQuery->commitPoint, pNewBlock->numOfPoints, pQuery->firstSlot,
        //            pQuery->currentSlot);
      } else {
        // current block are all commit already, ignore it
        assert(pNewBlock->numOfPoints == 0);
        //        dTrace(
        //            "QInfo:%p ignore points in cache block that are all commit already, numOfBlock:%d slot:%d ignore
        //            points:%d " "first:%d last:%d", GET_QINFO_ADDR(pQuery), pQuery->numOfBlocks, slot, offset,
        //            pQuery->firstSlot, pQuery->currentSlot);
        return NULL;
      }
    }
  }

  // keep the data from in cache into the temporarily allocated buffer
  for (int32_t i = 0; i < QH_GET_NUM_OF_COLS(pQueryHandle); ++i) {
    SColumnInfoEx_ *pColumnInfoEx = taosArrayGet(pQueryHandle->pColumns, i);

    //    int16_t columnIndex = pColumnInfoEx->colIdx;
    //    int16_t columnIndexInBuf = pColumnInfoEx->colIdxInBuf;

    SColumn *pCol = NULL;
    int16_t  bytes = pColumnInfoEx->info.bytes;
    int16_t  type = pColumnInfoEx->info.type;

    char *  dst = pColumnInfoEx->pData->data;
    int32_t index = -1;

    for (int32_t j = 0; j < pMeterObj->numOfColumns; ++j) {
      SColumn *pColumn = &pMeterObj->schema[j];
      if (pColumn->colId == pColumnInfoEx->info.colId) {
        pCol = pColumn;
        break;
      }
    }

    if (index >= 0) {
      assert(bytes == pCol->bytes && type == pCol->type);
      memcpy(dst, pBlock->offset[index] + offset * bytes, numOfPoints * bytes);
    } else {
      setNullN(dst, type, bytes, numOfPoints);
    }
  }

  assert(numOfPoints == pNewBlock->numOfPoints);

  // if the primary timestamp are not loaded by default, always load it here into buffer
  if (!PRIMARY_TSCOL_REQUIRED(pQueryHandle->pColumns)) {
    memcpy(pQueryHandle->tsBuf->data, pBlock->offset[0] + offset * TSDB_KEYSIZE, TSDB_KEYSIZE * numOfPoints);
  }

  pQueryHandle->cur.fileId = -1;
  pQueryHandle->cur.slot = slot;

  if (!isCacheBlockValid_(pQueryHandle, pNewBlock, pMeterObj, slot)) {
    return NULL;
  }

  /*
   * the accessed cache block still belongs to current meterObj, go on
   * update the load data block info
   */
  vnodeSetDataBlockInfoLoaded_(pQueryHandle, pMeterObj, -1, true, &pQueryHandle->cur);

  TSKEY skey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pNewBlock, 0);
  TSKEY ekey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pNewBlock, numOfPoints - 1);

  //  dTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d, load cache block, ts:%d, slot:%d, brange:%lld-%lld, rows:%d",
  //         GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, 1,
  //         pQuery->slot, skey, ekey, numOfPoints);

  return pNewBlock;
}

int32_t binarySearchInCacheBlk(SCacheInfo *pCacheInfo, SQuery *pQuery, int32_t keyLen, int32_t firstSlot,
                               int32_t lastSlot) {
  int32_t midSlot = 0;

  while (1) {
    int32_t numOfBlocks = (lastSlot - firstSlot + 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
    if (numOfBlocks == 0) {
      numOfBlocks = pCacheInfo->maxBlocks;
    }

    midSlot = (firstSlot + (numOfBlocks >> 1)) % pCacheInfo->maxBlocks;
    SCacheBlock *pBlock = pCacheInfo->cacheBlocks[midSlot];

    TSKEY keyFirst = *((TSKEY *)pBlock->offset[0]);
    TSKEY keyLast = *((TSKEY *)(pBlock->offset[0] + (pBlock->numOfPoints - 1) * keyLen));

    if (numOfBlocks == 1) {
      break;
    }

    if (pQuery->skey > keyLast) {
      if (numOfBlocks == 2) break;
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        int          nextSlot = (midSlot + 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
        SCacheBlock *pNextBlock = pCacheInfo->cacheBlocks[nextSlot];
        TSKEY        nextKeyFirst = *((TSKEY *)(pNextBlock->offset[0]));
        if (pQuery->skey < nextKeyFirst) break;
      }
      firstSlot = (midSlot + 1) % pCacheInfo->maxBlocks;
    } else if (pQuery->skey < keyFirst) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        int          prevSlot = (midSlot - 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
        SCacheBlock *pPrevBlock = pCacheInfo->cacheBlocks[prevSlot];
        TSKEY        prevKeyLast = *((TSKEY *)(pPrevBlock->offset[0] + (pPrevBlock->numOfPoints - 1) * keyLen));
        if (pQuery->skey > prevKeyLast) {
          break;
        }
      }
      lastSlot = (midSlot - 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
    } else {
      break;  // got the slot
    }
  }

  return midSlot;
}

int32_t binarySearchInCacheBlk_(SCacheInfo *pCacheInfo, int32_t order, TSKEY skey, int32_t keyLen, int32_t firstSlot,
                                int32_t lastSlot) {
  int32_t midSlot = 0;

  while (1) {
    int32_t numOfBlocks = (lastSlot - firstSlot + 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
    if (numOfBlocks == 0) {
      numOfBlocks = pCacheInfo->maxBlocks;
    }

    midSlot = (firstSlot + (numOfBlocks >> 1)) % pCacheInfo->maxBlocks;
    SCacheBlock *pBlock = pCacheInfo->cacheBlocks[midSlot];

    TSKEY keyFirst = *((TSKEY *)pBlock->offset[0]);
    TSKEY keyLast = *((TSKEY *)(pBlock->offset[0] + (pBlock->numOfPoints - 1) * keyLen));

    if (numOfBlocks == 1) {
      break;
    }

    if (skey > keyLast) {
      if (numOfBlocks == 2) break;
      if (!QUERY_IS_ASC_QUERY_RV(order)) {
        int          nextSlot = (midSlot + 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
        SCacheBlock *pNextBlock = pCacheInfo->cacheBlocks[nextSlot];
        TSKEY        nextKeyFirst = *((TSKEY *)(pNextBlock->offset[0]));
        if (skey < nextKeyFirst) break;
      }
      firstSlot = (midSlot + 1) % pCacheInfo->maxBlocks;
    } else if (skey < keyFirst) {
      if (QUERY_IS_ASC_QUERY_RV(order)) {
        int          prevSlot = (midSlot - 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
        SCacheBlock *pPrevBlock = pCacheInfo->cacheBlocks[prevSlot];
        TSKEY        prevKeyLast = *((TSKEY *)(pPrevBlock->offset[0] + (pPrevBlock->numOfPoints - 1) * keyLen));
        if (skey > prevKeyLast) {
          break;
        }
      }
      lastSlot = (midSlot - 1 + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
    } else {
      break;  // got the slot
    }
  }

  return midSlot;
}

bool isCacheBlockValid(SQuery *pQuery, SCacheBlock *pBlock, SMeterObj *pMeterObj, int32_t slot) {
  if (pMeterObj != pBlock->pMeterObj || pBlock->blockId > pQuery->blockId) {
    SMeterObj *pNewMeterObj = pBlock->pMeterObj;
    char *     id = (pNewMeterObj != NULL) ? pNewMeterObj->meterId : NULL;

    dWarn(
        "QInfo:%p vid:%d sid:%d id:%s, cache block is overwritten, slot:%d blockId:%d qBlockId:%d, meterObj:%p, "
        "blockMeterObj:%p, blockMeter id:%s, first:%d, last:%d, numOfBlocks:%d",
        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot, pBlock->blockId,
        pQuery->blockId, pMeterObj, pNewMeterObj, id, pQuery->firstSlot, pQuery->currentSlot, pQuery->numOfBlocks);

    return false;
  }

  /*
   * The check for empty block:
   *    pBlock->numOfPoints == 0. There is a empty block, which is caused by allocate-and-write data into cache
   *    procedure. The block has been allocated but data has not been put into yet. If the block is the last
   *    block(newly allocated block), abort query. Otherwise, skip it and go on.
   */
  if (pBlock->numOfPoints == 0) {
    dWarn(
        "QInfo:%p vid:%d sid:%d id:%s, cache block is empty. slot:%d first:%d, last:%d, numOfBlocks:%d,"
        "allocated but not write data yet.",
        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, slot, pQuery->firstSlot,
        pQuery->currentSlot, pQuery->numOfBlocks);

    return false;
  }

  return true;
}

bool isCacheBlockValid_(STsdbQueryHandle *pQueryHandle, SCacheBlock *pBlock, SMeterObj *pMeterObj, int32_t slot) {
  if (pMeterObj != pBlock->pMeterObj || pBlock->blockId > pQueryHandle->blockId) {
    SMeterObj *pNewMeterObj = pBlock->pMeterObj;
    char *     id = (pNewMeterObj != NULL) ? pNewMeterObj->meterId : NULL;

    //    dWarn(
    //        "QInfo:%p vid:%d sid:%d id:%s, cache block is overwritten, slot:%d blockId:%d qBlockId:%d, meterObj:%p, "
    //        "blockMeterObj:%p, blockMeter id:%s, first:%d, last:%d, numOfBlocks:%d",
    //        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot,
    //        pBlock->blockId, pQuery->blockId, pMeterObj, pNewMeterObj, id, pQuery->firstSlot, pQuery->currentSlot,
    //        pQuery->numOfBlocks);
    //
    return false;
  }

  /*
   * The check for empty block:
   *    pBlock->numOfPoints == 0. There is a empty block, which is caused by allocate-and-write data into cache
   *    procedure. The block has been allocated but data has not been put into yet. If the block is the last
   *    block(newly allocated block), abort query. Otherwise, skip it and go on.
   */
  if (pBlock->numOfPoints == 0) {
    //    dWarn(
    //        "QInfo:%p vid:%d sid:%d id:%s, cache block is empty. slot:%d first:%d, last:%d, numOfBlocks:%d,"
    //        "allocated but not write data yet.",
    //        GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, slot, pQuery->firstSlot,
    //        pQuery->currentSlot, pQuery->numOfBlocks);

    return false;
  }

  return true;
}

/**
 *  Get cache snapshot will destroy the comp block info in SQuery, in order to speedup the query
 *  process, we always check cache first.
 */
void vnodeCheckIfDataExists(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj, bool *dataInDisk, bool *dataInCache) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  *dataInCache = hasDataInCache(pRuntimeEnv, pMeterObj);
  *dataInDisk = hasDataInDisk(pQuery, pMeterObj);

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
}

bool isIntervalQuery(SQuery *pQuery) { return pQuery->intervalTime > 0; }

int64_t loadRequiredBlockIntoMem(SQueryRuntimeEnv *pRuntimeEnv, SPositionInfo *position) {
  TSKEY nextTimestamp = -1;

  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  pQuery->fileId = position->fileId;
  pQuery->slot = position->slot;
  pQuery->pos = position->pos;

  if (position->fileId == -1) {
    SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
    if (pCacheInfo == NULL || pCacheInfo->numOfBlocks == 0 || pCacheInfo->cacheBlocks == NULL) {
      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
      return -1;
    }

    SCacheBlock *pBlock = getCacheDataBlock(pMeterObj, pRuntimeEnv, pQuery->slot);
    if (pBlock != NULL) {
      nextTimestamp = getTimestampInCacheBlock(pRuntimeEnv, pBlock, position->pos);
    } else {
      // todo fix it
    }

    SET_CACHE_BLOCK_FLAG(pRuntimeEnv->blockStatus);
  } else {
    // todo handle the file broken situation
    /*
     * load the file metadata into buffer first, then the specific data block.
     * currently opened file is not the start file, reset to the start file
     */
    int32_t fileIdx = vnodeGetVnodeHeaderFileIndex(&pQuery->fileId, pRuntimeEnv, pQuery->order.order);
    if (fileIdx < 0) {  // ignore the files on disk
      dError("QInfo:%p failed to get data file:%d", GET_QINFO_ADDR(pQuery), pQuery->fileId);
      position->fileId = -1;
      return -1;
    }

    /*
     * NOTE:
     * The compblock information may not be loaded yet, here loaded it firstly.
     * If the compBlock info is loaded, it wont be loaded again.
     *
     * If failed to load comp block into memory due some how reasons, e.g., empty header file/not enough memory
     */
    if (vnodeGetCompBlockInfo(pMeterObj, pRuntimeEnv, fileIdx) <= 0) {
      position->fileId = -1;
      return -1;
    }

    nextTimestamp = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
  }

  return nextTimestamp;
}

int32_t LoadDatablockOnDemand(SCompBlock *pBlock, SField **pFields, uint8_t *blkStatus, SQueryRuntimeEnv *pRuntimeEnv,
                              int32_t fileIdx, int32_t slotIdx, __block_search_fn_t searchFn, bool onDemand) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  TSKEY *primaryKeys = (TSKEY *)pRuntimeEnv->primaryColBuffer->data;

  pQuery->slot = slotIdx;
  pQuery->pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : pBlock->numOfPoints - 1;

  SET_FILE_BLOCK_FLAG(*blkStatus);
  SET_DATA_BLOCK_NOT_LOADED(*blkStatus);

  if (((pQuery->lastKey <= pBlock->keyFirst && pQuery->ekey >= pBlock->keyLast && QUERY_IS_ASC_QUERY(pQuery)) ||
       (pQuery->ekey <= pBlock->keyFirst && pQuery->lastKey >= pBlock->keyLast && !QUERY_IS_ASC_QUERY(pQuery))) &&
      onDemand) {
    uint32_t req = 0;
    if (pQuery->numOfFilterCols > 0) {
      req = BLK_DATA_ALL_NEEDED;
    } else {
      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        int32_t functID = pQuery->pSelectExpr[i].pBase.functionId;
        req |= aAggs[functID].dataReqFunc(&pRuntimeEnv->pCtx[i], pBlock->keyFirst, pBlock->keyLast,
                                          pQuery->pSelectExpr[i].pBase.colInfo.colId, *blkStatus);
      }

      if (pRuntimeEnv->pTSBuf > 0 || isIntervalQuery(pQuery)) {
        req |= BLK_DATA_ALL_NEEDED;
      }
    }

    if (req == BLK_DATA_NO_NEEDED) {
      qTrace("QInfo:%p vid:%d sid:%d id:%s, slot:%d, data block ignored, brange:%" PRId64 "-%" PRId64 ", rows:%d",
             GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot,
             pBlock->keyFirst, pBlock->keyLast, pBlock->numOfPoints);

      setTimestampRange(pRuntimeEnv, pBlock->keyFirst, pBlock->keyLast);
    } else if (req == BLK_DATA_FILEDS_NEEDED) {
      if (loadDataBlockFieldsInfo(pRuntimeEnv, pBlock, pFields) < 0) {
        return DISK_DATA_LOAD_FAILED;
      }
    } else {
      assert(req == BLK_DATA_ALL_NEEDED);
      goto _load_all;
    }
  } else {
  _load_all:
    if (loadDataBlockFieldsInfo(pRuntimeEnv, pBlock, pFields) < 0) {
      return DISK_DATA_LOAD_FAILED;
    }

    if ((pQuery->lastKey <= pBlock->keyFirst && pQuery->ekey >= pBlock->keyLast && QUERY_IS_ASC_QUERY(pQuery)) ||
        (pQuery->lastKey >= pBlock->keyLast && pQuery->ekey <= pBlock->keyFirst && !QUERY_IS_ASC_QUERY(pQuery))) {
      /*
       * if this block is completed included in the query range, do more filter operation
       * filter the data block according to the value filter condition.
       * no need to load the data block, continue for next block
       */
      if (!needToLoadDataBlock(pQuery, *pFields, pRuntimeEnv->pCtx, pBlock->numOfPoints)) {
#if defined(_DEBUG_VIEW)
        dTrace("QInfo:%p fileId:%d, slot:%d, block discarded by per-filter, ", GET_QINFO_ADDR(pQuery), pQuery->fileId,
               pQuery->slot);
#endif
        qTrace("QInfo:%p id:%s slot:%d, data block ignored by pre-filter, fields loaded, brange:%" PRId64 "-%" PRId64
               ", rows:%d",
               GET_QINFO_ADDR(pQuery), pMeterObj->meterId, pQuery->slot, pBlock->keyFirst, pBlock->keyLast,
               pBlock->numOfPoints);
        return DISK_DATA_DISCARDED;
      }
    }

    SBlockInfo binfo = getBlockBasicInfo(pRuntimeEnv, pBlock, BLK_FILE_BLOCK);
    bool       loadTS = needPrimaryTimestampCol(pQuery, &binfo);

    /*
     * the pRuntimeEnv->pMeterObj is not updated during loop, since which meter this block is belonged to is not matter
     * in order to enforce to load the data block, we HACK the load check procedure,
     * by changing pQuery->slot each time to IGNORE the pLoadInfo data check. It is NOT a normal way.
     */
    int32_t ret = loadDataBlockIntoMem(pBlock, pFields, pRuntimeEnv, fileIdx, loadTS, false);
    SET_DATA_BLOCK_LOADED(*blkStatus);

    if (ret < 0) {
      return DISK_DATA_LOAD_FAILED;
    }

    /* find first qualified record position in this block */
    if (loadTS) {
      pQuery->pos = searchFn((char *)primaryKeys, pBlock->numOfPoints, pQuery->lastKey, pQuery->order.order);

      /* boundary timestamp check */
      assert(pBlock->keyFirst == primaryKeys[0] && pBlock->keyLast == primaryKeys[pBlock->numOfPoints - 1]);
    }

    /*
     * NOTE:
     * if the query of current timestamp window is COMPLETED, the query range condition may not be satisfied
     * such as:
     * pQuery->lastKey + 1 == pQuery->ekey for descending order interval query
     * pQuery->lastKey - 1 == pQuery->ekey for ascending query
     */
    assert(((pQuery->ekey >= pQuery->lastKey || pQuery->ekey == pQuery->lastKey - 1) && QUERY_IS_ASC_QUERY(pQuery)) ||
           ((pQuery->ekey <= pQuery->lastKey || pQuery->ekey == pQuery->lastKey + 1) && !QUERY_IS_ASC_QUERY(pQuery)));
  }

  return DISK_DATA_LOADED;
}

void setTimestampRange(SQueryRuntimeEnv *pRuntimeEnv, int64_t stime, int64_t etime) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;

    if (functionId == TSDB_FUNC_SPREAD) {
      pRuntimeEnv->pCtx[i].param[1].dKey = stime;
      pRuntimeEnv->pCtx[i].param[2].dKey = etime;

      pRuntimeEnv->pCtx[i].param[1].nType = TSDB_DATA_TYPE_DOUBLE;
      pRuntimeEnv->pCtx[i].param[2].nType = TSDB_DATA_TYPE_DOUBLE;
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////
// new api
static void filterDataInDataBlock(STsdbQueryHandle *pQueryHandle, SArray *sa, int32_t numOfPoints);

bool getQualifiedDataBlock_rv(STsdbQueryHandle *pQueryHandle, int32_t type) {
  int32_t blkIdx = -1;
  int32_t fid = -1;

  int32_t step = (type == QUERY_RANGE_GREATER_EQUAL) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP;

  TSKEY key = pQueryHandle->lastKey;

  SQueryHandlePos *cur = &pQueryHandle->cur;
  SMeterObj *      pTable = taosArrayGetP(pQueryHandle->pTableList, 0);

  cur->fileId = getFileIdFromKey(pTable->vnode, key) - step;
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[0];  // vnodeBinarySearchKey;

  SData *ptsBuf = pQueryHandle->tsBuf;

  while (1) {
    if ((fid = getNextDataFileCompInfo_(pQueryHandle, cur, &pQueryHandle->vnodeFileInfo, step)) < 0) {
      break;
    }

    blkIdx = binarySearchForBlock_(pQueryHandle->pBlock, pQueryHandle->numOfBlocks, pQueryHandle->order, key);

    if (type == QUERY_RANGE_GREATER_EQUAL) {
      if (key <= pQueryHandle->pBlock[blkIdx].keyLast) {
        break;
      } else {
        blkIdx = -1;
      }
    } else {
      if (key >= pQueryHandle->pBlock[blkIdx].keyFirst) {
        break;
      } else {
        blkIdx = -1;
      }
    }
  }

  /* failed to find qualified point in file, abort */
  if (blkIdx == -1) {
    return false;
  }

  assert(blkIdx >= 0 && blkIdx < pQueryHandle->numOfBlocks);

  // load first data block into memory failed, caused by disk block error
  bool    blockLoaded = false;
  SArray *sa = NULL;

  while (blkIdx < pQueryHandle->numOfBlocks && blkIdx >= 0) {
    cur->slot = blkIdx;
    sa = getDefaultLoadColumns(pQueryHandle, true);

    if (loadDataBlockIntoMem_(pQueryHandle, &pQueryHandle->pBlock[cur->slot], &pQueryHandle->pFields[cur->slot], fid,
                              sa) == 0) {
      blockLoaded = true;
      break;
    }

    //    dError("QInfo:%p fileId:%d total numOfBlks:%d blockId:%d load into memory failed due to error in disk files",
    //           GET_QINFO_ADDR(pQuery), pQuery->fileId, pQuery->numOfBlocks, blkIdx);
    blkIdx += step;
  }

  // failed to load data from disk, abort current query
  if (blockLoaded == false) {
    return false;
  }

  SCompBlock *pBlocks = getDiskDataBlock_(pQueryHandle, blkIdx);

  // search qualified points in blk, according to primary key (timestamp) column
  cur->pos = searchFn(ptsBuf->data, pBlocks->numOfPoints, key, pQueryHandle->order);
  assert(cur->pos >= 0 && cur->fileId >= 0 && cur->slot >= 0);

  filterDataInDataBlock(pQueryHandle, sa, pBlocks->numOfPoints);
  return true;
}

void filterDataInDataBlock(STsdbQueryHandle *pQueryHandle, SArray *sa, int32_t numOfPoints) {
  // only return the qualified data to client in terms of query time window, data rows in the same block but do not
  // be included in the query time window will be discarded
  SQueryHandlePos *cur = &pQueryHandle->cur;
  if (cur->pos == 0) {  // no need to filter the unqualified rows
    return;
  }

  SDataBlockInfo blockInfo = getTrueBlockInfo(pQueryHandle);

  int32_t numOfCols = QH_GET_NUM_OF_COLS(pQueryHandle);

  for (int32_t i = 0; i < taosArrayGetSize(sa); ++i) {
    int16_t         colId = *(int16_t *)taosArrayGet(sa, i);
    SColumnInfoEx_ *pCol = NULL;

    for (int32_t j = 0; j < numOfCols; ++j) {
      pCol = taosArrayGet(pQueryHandle->pColumns, j);
      if (pCol->info.colId == colId) {
        // for desc query, no need to move data blocks
        if (QUERY_IS_ASC_QUERY_RV(pQueryHandle->order)) {
          memmove(pCol->pData->data, ((char *)pCol->pData->data) + pCol->info.bytes * cur->pos,
                  (numOfPoints - cur->pos) * pCol->info.bytes);
        }

        break;
      }
    }
  }

  //
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[0];  // vnodeBinarySearchKey;
  
  int32_t remain = numOfPoints - cur->pos;

  if (pQueryHandle->window.ekey < blockInfo.window.ekey) {
    int32_t endPos = searchFn(pQueryHandle->tsBuf->data, remain, pQueryHandle->window.ekey, 1);
    pQueryHandle->realNumOfRows = (endPos - cur->pos) + 1;
  } else {
    pQueryHandle->realNumOfRows = (blockInfo.size - cur->pos) + 1;
  }

  assert(pQueryHandle->realNumOfRows <= blockInfo.size);
}

static bool findStartPosition_(STsdbQueryHandle *pQueryHandle) {
  pQueryHandle->locateStart = true;

  if (QUERY_IS_ASC_QUERY_RV(pQueryHandle->order)) {
    SMeterObj *pMeterObj = *(SMeterObj **)taosArrayGet(pQueryHandle->pTableList, 0);

    bool hasData = hasDataInDisk_rv(pQueryHandle->window, pMeterObj);
    if (hasData) {
      hasData = getQualifiedDataBlock_rv(pQueryHandle, QUERY_RANGE_GREATER_EQUAL);
      if (hasData) {
        pQueryHandle->start = pQueryHandle->cur;
        return true;
      }
    }

    hasData = hasDataInCache_(pQueryHandle, taosArrayGet(pQueryHandle->pTableList, 0));
    if (hasData) {
      TSKEY key = getQueryStartPositionInCache_rv(pQueryHandle, &pQueryHandle->cur.slot, &pQueryHandle->cur.pos, false);
    }

    pQueryHandle->start = pQueryHandle->cur;
    return hasData;
  } else {  // descending order
            //    if (dataInCache) {  // todo handle error
            //      TSKEY nextKey = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, false);
            //      assert(nextKey == -1 || nextKey <= pQuery->skey);
            //
            //      if (key != NULL) {
            //        *key = nextKey;
            //      }
            //
            //      if (nextKey != -1) {  // find qualified data in cache
            //        if (nextKey >= pQuery->ekey) {
            //          return doSetDataInfo(pSupporter, pPointInterpSupporter, pMeterObj, nextKey);
            //        } else {
            //          /*
            //           * nextKey < pQuery->ekey && nextKey < pQuery->lastKey, query range is
            //           * larger than all data, abort
            //           *
            //           * NOTE: Interp query does not reach here, since for all interp query,
            //           * the query order is ascending order.
            //           */
            //          return false;
            //        }
            //      } else {  // all data in cache are greater than pQuery->skey, try file
            //      }
            //    }
            //
            //    if (dataInDisk && getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_LESS_EQUAL, searchFn)) {
            //      TSKEY nextKey = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
            //      assert(nextKey <= pQuery->skey);
            //
            //      if (key != NULL) {
            //        *key = nextKey;
            //      }
            //
            //      // key in query range. If not, no qualified in disk file
            //      if (nextKey >= pQuery->ekey) {
            //        return doSetDataInfo(pSupporter, pPointInterpSupporter, pMeterObj, nextKey);
            //      } else {  // In case of all queries, the value of false will be returned if key < pQuery->ekey
            //        return false;
            //      }
            //    }
  }

  return false;
}

static int32_t doAllocateBuf(STsdbQueryHandle *pQueryHandle, int32_t rowsPerFileBlock);

STsdbQueryHandle *tsdbQueryByTableId(STsdbQueryCond *pCond, SArray *pTableList, SArray *pColumnInfo) {
  STsdbQueryHandle *pQueryHandle = calloc(1, sizeof(STsdbQueryHandle));
  pQueryHandle->order = pCond->order;
  pQueryHandle->window = pCond->window;

  pQueryHandle->traverseOrder = pCond->order;
  pQueryHandle->pTableList = pTableList;
  pQueryHandle->pColumns = pColumnInfo;
  pQueryHandle->needLoadData = false;

  pQueryHandle->lastKey = pQueryHandle->window.skey;  // ascending query

  // malloc buffer in order to load data from file
  SMeterObj *pTable = taosArrayGetP(pQueryHandle->pTableList, 0);
  int32_t    numOfCols = taosArrayGetSize(pColumnInfo);

  pQueryHandle->pColumns = taosArrayInit(numOfCols, sizeof(SColumnInfoEx_));
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoEx *pCol = taosArrayGet(pColumnInfo, i);
    SColumnInfoEx_ pDest = {{0}, 0};

    pDest.pData = calloc(1, sizeof(SData) + EXTRA_BYTES + pTable->pointsPerFileBlock * pCol->data.bytes);
    pDest.info = pCol->data;
    taosArrayPush(pQueryHandle->pColumns, &pDest);
  }

  if (doAllocateBuf(pQueryHandle, pTable->pointsPerFileBlock) != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  initQueryFileInfoFD_rv(&pQueryHandle->vnodeFileInfo);
  vnodeInitDataBlockLoadInfo(&pQueryHandle->dataBlockLoadInfo);
  vnodeInitCompBlockLoadInfo(&pQueryHandle->compBlockLoadInfo);

  vnodeRecordAllFiles_rv(pTable->vnode, &pQueryHandle->vnodeFileInfo);

  return pQueryHandle;
}

int32_t doAllocateBuf(STsdbQueryHandle *pQueryHandle, int32_t rowsPerFileBlock) {
  // record the maximum column width among columns of this meter/metric
  SColumnInfoEx *pColumn = taosArrayGet(pQueryHandle->pColumns, 0);

  int32_t maxColWidth = pColumn->data.bytes;
  for (int32_t i = 1; i < QH_GET_NUM_OF_COLS(pQueryHandle); ++i) {
    int32_t bytes = pColumn[i].data.bytes;
    if (bytes > maxColWidth) {
      maxColWidth = bytes;
    }
  }

  pQueryHandle->tsBuf = NULL;
  if (PRIMARY_TSCOL_REQUIRED(pQueryHandle->pColumns)) {
    pQueryHandle->tsBuf = ((SColumnInfoEx_ *)taosArrayGet(pQueryHandle->pColumns, 0))->pData;
  } else {
    pQueryHandle->tsBuf = (SData *)calloc(1, rowsPerFileBlock * TSDB_KEYSIZE + sizeof(SData) + EXTRA_BYTES);
  }

  // only one unzip buffer required, since we can unzip each column one by one
  pQueryHandle->unzipBufSize = (size_t)(maxColWidth * rowsPerFileBlock + EXTRA_BYTES);  // plus extra_bytes
  pQueryHandle->unzipBuffer = (char *)calloc(1, pQueryHandle->unzipBufSize);

  pQueryHandle->secondaryUnzipBuffer = (char *)calloc(1, pQueryHandle->unzipBufSize);

  if (pQueryHandle->unzipBuffer == NULL || pQueryHandle->secondaryUnzipBuffer == NULL || pQueryHandle->tsBuf == NULL) {
    goto _error_clean;
  }

  return TSDB_CODE_SUCCESS;

_error_clean:
  tfree(pQueryHandle->unzipBuffer);
  tfree(pQueryHandle->secondaryUnzipBuffer);

  if (!PRIMARY_TSCOL_REQUIRED(pQueryHandle->pColumns)) {
    tfree(pQueryHandle->tsBuf);
  }

  return TSDB_CODE_SERV_OUT_OF_MEMORY;
}

bool tsdbNextDataBlock(STsdbQueryHandle *pQueryHandle) {
  if (pQueryHandle == NULL) {
    return false;
  }

  if (pQueryHandle->needLoadData) {
    if (pQueryHandle->cur.fileId == 0) {
      pQueryHandle->cur = pQueryHandle->start;
    }
    
    SQueryHandlePos *cur = &pQueryHandle->cur;

    if (cur->fileId > 0) {  // file
      SMeterObj *pTable = (SMeterObj *)taosArrayGetP(pQueryHandle->pTableList, 0);

      SArray *sa = getDefaultLoadColumns(pQueryHandle, true);
      vnodeGetCompBlockInfo_(pQueryHandle, pTable, cur->fileIndex);
      loadDataBlockIntoMem_(pQueryHandle, &pQueryHandle->pBlock[cur->slot], &pQueryHandle->pFields[cur->slot],
                            cur->fileIndex, sa);
    } else {
      // todo cache
    }

    pQueryHandle->needLoadData = false;
    return true;
  }

  // the start position does not locate yet
  if (!pQueryHandle->locateStart) {
    return findStartPosition_(pQueryHandle);
  } else {
    int32_t ret = moveToNextBlock_(pQueryHandle, 1, false);
    return ret > 0;
  }
}

SDataBlockInfo tsdbRetrieveDataBlockInfo(STsdbQueryHandle *pQueryHandle) { return getBlockInfo_(pQueryHandle); }

static bool completedIncluded(STimeWindow *win, SDataBlockInfo *pBlockInfo) {
  return (win->skey > pBlockInfo->window.skey || win->ekey < pBlockInfo->window.ekey);
}

int32_t tsdbRetrieveDataBlockStatisInfo(STsdbQueryHandle *pQueryHandle, SDataStatis **pBlockStatis) {
  int32_t slot = pQueryHandle->cur.slot;

  // cache data does not has the statistics info
  if (pQueryHandle->cur.fileId < 0) {
    *pBlockStatis = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // get current file block info
  SDataBlockInfo blockInfo = getTrueBlockInfo(pQueryHandle);

  // if the query time window does not completed cover this block, the statistics data will not return.
  if (pQueryHandle->window.skey > blockInfo.window.skey || pQueryHandle->window.ekey < blockInfo.window.ekey) {
    *pBlockStatis = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // load the file block info
  loadDataBlockFieldsInfo_(pQueryHandle, &pQueryHandle->pBlock[slot], &pQueryHandle->pFields[slot]);

  *pBlockStatis = pQueryHandle->pFields[slot];
  return TSDB_CODE_SUCCESS;
}

static SArray *getColumnIdList(STsdbQueryHandle *pQueryHandle) {
  int32_t numOfCols = QH_GET_NUM_OF_COLS(pQueryHandle);
  SArray *pIdList = taosArrayInit(numOfCols, sizeof(int16_t));
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoEx_ *pCol = taosArrayGet(pQueryHandle->pColumns, i);

    taosArrayPush(pIdList, &pCol->info.colId);
  }

  return pIdList;
}

SArray *getDefaultLoadColumns(STsdbQueryHandle *pQueryHandle, bool loadTS) {
  SArray *pLocalIdList = getColumnIdList(pQueryHandle);

  // check if the primary time stamp column needs to load
  int16_t colId = *(int16_t *)taosArrayGet(pLocalIdList, 0);

  // the primary timestamp column does not be included in the the specified load column list, add it
  if (loadTS && colId != 0) {
    int16_t columnId = 0;
    taosArrayInsert(pLocalIdList, 0, &columnId);
  }

  return pLocalIdList;
}

SArray *tsdbRetrieveDataBlock(STsdbQueryHandle *pQueryHandle, SArray *pIdList) {
  int32_t slot = pQueryHandle->cur.slot;

  // if the pIdList is NULL, all required columns are loaded into buffer
  SArray *pLocalIdList = pIdList;
  if (pIdList == NULL) {
    pLocalIdList = getColumnIdList(pQueryHandle);
  } else {
    assert(taosArrayGetSize(pIdList) > 0);
  }

  // check if the primary time stamp column needs to load
  SDataBlockInfo blockInfo = getTrueBlockInfo(pQueryHandle);

  if (!completedIncluded(&pQueryHandle->window, &blockInfo)) {
    int32_t colId = *(int32_t *)taosArrayGet(pLocalIdList, 0);

    // the primary timestamp column does not be included in the the specified load column list, add it
    if (colId != 0) {
      int32_t columnId = 0;
      taosArrayInsert(pLocalIdList, 0, &columnId);
    }
  }

  loadDataBlockIntoMem_(pQueryHandle, &pQueryHandle->pBlock[slot], &pQueryHandle->pFields[slot],
                        pQueryHandle->cur.fileIndex, pLocalIdList);

  return pQueryHandle->pColumns;
}

int32_t tsdbDataBlockSeek(STsdbQueryHandle *pQueryHandle, tsdbPos_t pos) {
  if (pos == NULL || pQueryHandle == NULL) {
    return -1;
  }

  pQueryHandle->cur = *(SQueryHandlePos *)pos;
  pQueryHandle->needLoadData = true;
}

tsdbPos_t tsdbDataBlockTell(STsdbQueryHandle *pQueryHandle) {
  if (pQueryHandle == NULL) {
    return NULL;
  }

  SQueryHandlePos *pPos = calloc(1, sizeof(SQueryHandlePos));
  memcpy(pPos, &pQueryHandle->cur, sizeof(SQueryHandlePos));

  return pPos;
}
