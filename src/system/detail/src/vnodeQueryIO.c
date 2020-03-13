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

#define ALL_CACHE_BLOCKS_CHECKED(q, p)                                  \
  (((q).slot == (p)->currentSlot && QUERY_IS_ASC_QUERY_RV(p->order)) || \
   ((q).slot == (p)->firstSlot && (!QUERY_IS_ASC_QUERY_RV(p->order))))

#define FORWARD_CACHE_BLOCK_CHECK_SLOT(slot, step, maxblocks) (slot) = ((slot) + (step) + (maxblocks)) % (maxblocks);

int32_t validateHeaderOffsetSegment_(STsdbQueryHandle *pQueryHandle, char *filePath, int32_t vid, char *data,
                                     int32_t size) {
  if (!taosCheckChecksumWhole((uint8_t *)data + TSDB_FILE_HEADER_LEN, size)) {
    dLError("QInfo:%p handle:%p vid:%d, failed to read header file:%s, file offset area is broken", pQueryHandle->qinfo,
            pQueryHandle, vid, filePath);
    return -1;
  }

  return 0;
}

int32_t getCompHeaderSegSize(SVnodeCfg *pCfg) { return pCfg->maxSessions * sizeof(SCompHeader) + sizeof(TSCKSUM); }

int32_t getCompHeaderStartPosition(SVnodeCfg *pCfg) { return TSDB_FILE_HEADER_LEN + getCompHeaderSegSize(pCfg); }

int32_t validateCompBlockOffset_(STsdbQueryHandle *pQueryHandle, SMeterObj *pMeterObj, SCompHeader *pCompHeader,
                                 SQueryFilesInfo_rv *pQueryFileInfo, int32_t headerSize) {
  if (pCompHeader->compInfoOffset < headerSize || pCompHeader->compInfoOffset > pQueryFileInfo->headerFileSize) {
    dError("QInfo:%p handle:%p vid:%d sid:%d id:%s, compInfoOffset:%" PRId64 " is not valid, size:%" PRId64,
           pQueryHandle->qinfo, pQueryFileInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId,
           pCompHeader->compInfoOffset, pQueryFileInfo->headerFileSize);

    return -1;
  }

  return 0;
}

// check compinfo integrity
static FORCE_INLINE int32_t validateCompBlockInfoSegment_(STsdbQueryHandle *pQueryHandle, const char *filePath,
                                                          int32_t vid, SCompInfo *compInfo, int64_t offset) {
  if (!taosCheckChecksumWhole((uint8_t *)compInfo, sizeof(SCompInfo))) {
    dLError("QInfo:%p handle:%p vid:%d, failed to read header file:%s, file compInfo broken, offset:%" PRId64,
            pQueryHandle->qinfo, pQueryHandle, vid, filePath, offset);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t validateCompBlockSegment_(STsdbQueryHandle *pQueryHandle, const char *filePath,
                                                      SCompInfo *compInfo, char *pBlock, int32_t vid, TSCKSUM ck) {
  uint32_t size = compInfo->numOfBlocks * sizeof(SCompBlock);

  if (ck != taosCalcChecksum(0, (uint8_t *)pBlock, size)) {
    dLError("QInfo:%p handle:%p vid:%d, failed to read header file:%s, file compblock is broken:%zu",
            pQueryHandle->qinfo, pQueryHandle, vid, filePath, (char *)compInfo + sizeof(SCompInfo));
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

static bool checkIsHeaderFileEmpty_(SQueryFilesInfo_rv *pVnodeFilesInfo) {
  struct stat fstat = {0};
  if (stat(pVnodeFilesInfo->headerFilePath, &fstat) < 0) {
    return true;
  }

  pVnodeFilesInfo->headerFileSize = fstat.st_size;
  return isHeaderFileEmpty(pVnodeFilesInfo->vnodeId, pVnodeFilesInfo->headerFileSize);
}

static int32_t vnodeGetVnodeHeaderFileIndex_(int32_t *fid, int32_t order, SQueryFilesInfo_rv *pVnodeFiles);

static void doCloseQueryFileInfoFD(SQueryFilesInfo *pVnodeFilesInfo);

static bool isCacheBlockValid(SQuery *pQuery, SCacheBlock *pBlock, SMeterObj *pMeterObj, int32_t slot);

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

static SArray *getDefaultLoadColumns(STsdbQueryHandle *pQueryHandle, bool loadTS);

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

static void vnodeSetCompBlockInfoLoaded_(SLoadCompBlockInfo *pCompBlockLoadInfo, int32_t fileIndex, int32_t sid,
                                         int32_t fileId) {
  pCompBlockLoadInfo->sid = sid;
  pCompBlockLoadInfo->fileListIndex = fileIndex;
  pCompBlockLoadInfo->fileId = fileId;
}

int64_t getQueryStartPositionInCache_rv(STsdbQueryHandle *pQueryHandle, int32_t *slot, int32_t *pos,
                                        bool ignoreQueryRange);

void vnodeInitCompBlockLoadInfo(SLoadCompBlockInfo *pCompBlockLoadInfo) {
  pCompBlockLoadInfo->sid = -1;
  pCompBlockLoadInfo->fileId = -1;
  pCompBlockLoadInfo->fileListIndex = -1;
}

static void clearAllMeterDataBlockInfo(SMeterDataInfo **pMeterDataInfo, int32_t start, int32_t end) {
  for (int32_t i = start; i < end; ++i) {
    tfree(pMeterDataInfo[i]->pBlock);
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

#if 0
TSKEY getTimestampInCacheBlock(SQueryRuntimeEnv *pRuntimeEnv, SCacheBlock *pBlock, int32_t index) {
  if (pBlock == NULL || index >= pBlock->numOfPoints || index < 0) {
    return -1;
  }

  return ((TSKEY *)(pRuntimeEnv->primaryColBuffer->data))[index];
}

#endif

TSKEY getTimestampInCacheBlock_(SData *ptsBuf, SCacheBlock *pBlock, int32_t index) {
  if (pBlock == NULL || index >= pBlock->numOfPoints || index < 0) {
    return -1;
  }

  return ((TSKEY *)(ptsBuf->data))[index];
}

#if 0
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

#endif

static void vnodeSetDataBlockInfoLoaded_(STsdbQueryHandle *pQueryHandle, SMeterObj *pMeterObj, int32_t fileIndex,
                                         SArray *pLoadedCols, SQueryHandlePos *cur) {
  SDataBlockLoadInfo_ *pLoadInfo = &pQueryHandle->dataBlockLoadInfo;

  pLoadInfo->fileId = cur->fileId;
  pLoadInfo->slotIdx = cur->slot;
  pLoadInfo->fileListIndex = fileIndex;
  pLoadInfo->sid = pMeterObj->sid;
  pLoadInfo->pLoadedCols = pLoadedCols;
}

void vnodeInitDataBlockLoadInfo(SDataBlockLoadInfo_ *pBlockLoadInfo) {
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

int32_t vnodeGetHeaderFile_(SQueryFilesInfo_rv *pVnodeFileInfo, int32_t fileIndex) {
  //  assert(fileIndex >= 0 && fileIndex < pRuntimeEnv->vnodeFileInfo.numOfFiles);
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
  if (validateHeaderOffsetSegment_(pQueryHandle, pVnodeFileInfo->headerFilePath, pMeterObj->vnode,
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
  size_t startPos = getCompHeaderStartPosition(pCfg);
  if (validateCompBlockOffset_(pQueryHandle, pMeterObj, compHeader, pVnodeFileInfo, startPos) < 0) {
    free(buf);
    return -1;
  }

  lseek(pVnodeFileInfo->headerFd, compHeader->compInfoOffset, SEEK_SET);

  SCompInfo compInfo = {0};
  read(pVnodeFileInfo->headerFd, &compInfo, sizeof(SCompInfo));

  // check compblock info integrity
  if (validateCompBlockInfoSegment_(pQueryHandle, pVnodeFileInfo->headerFilePath, pMeterObj->vnode, &compInfo,
                                    compHeader->compInfoOffset) < 0) {
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
  if (validateCompBlockSegment_(pQueryHandle, pVnodeFileInfo->headerFilePath, &compInfo, (char *)pQueryHandle->pBlock,
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

bool     moveToNextBlockInCache_(STsdbQueryHandle *pQueryHandle, int32_t step);
static void filterDataInDataBlock(STsdbQueryHandle *pQueryHandle, SArray *sa, int32_t numOfPoints);

static bool loadQaulifiedData(STsdbQueryHandle *pQueryHandle) {
  SQueryHandlePos *cur = &pQueryHandle->cur;

  SCompBlock *pBlock = &pQueryHandle->pBlock[cur->slot];

  SArray *sa = getDefaultLoadColumns(pQueryHandle, true);
  if (QUERY_IS_ASC_QUERY_RV(pQueryHandle->order)) {
    // load data in to buffer to decide the end position
    if (pQueryHandle->window.ekey < pBlock->keyLast) {
      loadDataBlockIntoMem_(pQueryHandle, pBlock, &pQueryHandle->pFields[cur->slot], cur->fileId, sa);
    }
  } else {
    if (pQueryHandle->window.ekey > pBlock->keyFirst) {
      loadDataBlockIntoMem_(pQueryHandle, pBlock, &pQueryHandle->pFields[cur->slot], cur->fileId, sa);
    }
  }

  filterDataInDataBlock(pQueryHandle, sa, pBlock->numOfPoints);
  return pQueryHandle->realNumOfRows > 0;
}

bool moveToNextBlock_(STsdbQueryHandle *pQueryHandle, int32_t step) {
  SQueryHandlePos *cur = &pQueryHandle->cur;

  if (pQueryHandle->cur.fileId >= 0) {
    int32_t fileIndex = -1;

    /*
     * 1. ascending  order. The last data block of data file
     * 2. descending order. The first block of file
     */
    if ((step == QUERY_ASC_FORWARD_STEP && (pQueryHandle->cur.slot == pQueryHandle->numOfBlocks - 1)) ||
        (step == QUERY_DESC_FORWARD_STEP && (pQueryHandle->cur.slot == 0))) {
      // temporarily keep the position value, in case of no data qualified when move forwards(backwards)
      SQueryHandlePos save = pQueryHandle->cur;

      fileIndex = getNextDataFileCompInfo_(pQueryHandle, &pQueryHandle->cur, &pQueryHandle->vnodeFileInfo, step);

      // first data block in the next file
      if (fileIndex >= 0) {
        cur->slot = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQueryHandle->numOfBlocks - 1;
        cur->pos = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pQueryHandle->pBlock[cur->slot].numOfPoints - 1;
        return loadQaulifiedData(pQueryHandle);
      } else {// try data in cache
        assert(cur->fileId == -1);

        if (step == QUERY_ASC_FORWARD_STEP) {
          TSKEY nextTimestamp =
              getQueryStartPositionInCache_rv(pQueryHandle, &pQueryHandle->cur.slot, &pQueryHandle->cur.pos, true);
          if (nextTimestamp < 0) {
            pQueryHandle->cur = save;
          }
          
          return (nextTimestamp > 0);
        }
        
        // no data to check for desc order query, restore the saved position value
        pQueryHandle->cur = save;
        return false;
      }
    }

    // next block in the same file
    int32_t fid = cur->fileId;
    fileIndex = vnodeGetVnodeHeaderFileIndex_(&fid, pQueryHandle->order, &pQueryHandle->vnodeFileInfo);
    assert(fileIndex == cur->fileIndex);

    cur->slot += step;

    SCompBlock *pBlock = &pQueryHandle->pBlock[cur->slot];
    cur->pos = (step == QUERY_ASC_FORWARD_STEP) ? 0 : pBlock->numOfPoints - 1;
    return loadQaulifiedData(pQueryHandle);
  } else {  // data in cache
    return moveToNextBlockInCache_(pQueryHandle, step);
  }
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

static void fillWithNull_(SColumnInfo *pColumnInfo, char *dst, int32_t numOfPoints) {
  setNullN(dst, pColumnInfo->type, pColumnInfo->bytes, numOfPoints);
}

static int32_t loadPrimaryTSColumn_(STsdbQueryHandle *pQueryHandle, SCompBlock *pBlock, SField **pField,
                                    int32_t *columnBytes) {
  if (columnBytes != NULL) {
    (*columnBytes) += (*pField)[PRIMARYKEY_TIMESTAMP_COL_INDEX].len + sizeof(TSCKSUM);
  }

  int32_t ret = loadColumnIntoMem_(&pQueryHandle->vnodeFileInfo, pBlock, *pField, PRIMARYKEY_TIMESTAMP_COL_INDEX,
                                   pQueryHandle->tsBuf, pQueryHandle->unzipBuffer, pQueryHandle->secondaryUnzipBuffer,
                                   pQueryHandle->unzipBufSize);
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
        pColumn = taosArrayGet(pQueryHandle->pColumns, k);
        if (pColumn->info.colId == colId) {
          sdata = pColumn->pData;
          break;
        }
      }

      assert(sdata != NULL);

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
int32_t doOpenQueryFile_(SQueryFilesInfo_rv *pVnodeFileInfo) {
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

void doCloseQueryFileInfoFD_(SQueryFilesInfo_rv *pVnodeFileInfo) {
  tclose(pVnodeFileInfo->headerFd);
  tclose(pVnodeFileInfo->dataFd);
  tclose(pVnodeFileInfo->lastFd);

  pVnodeFileInfo->current = -1;
  pVnodeFileInfo->headerFileSize = -1;
}

void doCloseQueryFiles_(SQueryFilesInfo_rv *pVnodeFileInfo) {
  if (pVnodeFileInfo->current >= 0) {
    size_t size = taosArrayGetSize(pVnodeFileInfo->pFileInfo);

    assert(pVnodeFileInfo->current < size && pVnodeFileInfo->current >= 0);

    pVnodeFileInfo->headerFileSize = -1;
    doCloseQueryFileInfoFD_(pVnodeFileInfo);
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
  qsort(pVnodeFilesInfo->pFileInfo->pData, numOfFiles, sizeof(SHeaderFileInfo), file_order_comparator);
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

static bool isMultiTableQuery(STsdbQueryHandle *pQueryHandle) { return taosArrayGetSize(pQueryHandle->pTableList) > 1; }

SCompBlock *getDiskDataBlock_(STsdbQueryHandle *pQueryHandle, int32_t slot) {
  SQueryHandlePos *cur = &pQueryHandle->cur;

  if (isMultiTableQuery(pQueryHandle)) {
    return pQueryHandle->pDataBlockInfoEx[cur->slot].pBlock.compBlock;
  } else {
    assert(cur->fileId >= 0 && slot >= 0 && slot < pQueryHandle->numOfBlocks && pQueryHandle->pBlock != NULL);
    return &pQueryHandle->pBlock[slot];
  }
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
    pBlock = getCacheDataBlock_(pQueryHandle, taosArrayGetP(pQueryHandle->pTableList, 0), pQueryHandle->cur.slot);
    assert(pBlock != NULL);

    SCacheBlock *   pCacheBlock = (SCacheBlock *)pBlock;
    SColumnInfoEx_ *pColInfo = taosArrayGet(pQueryHandle->pColumns, 0);

    info.window.skey = getTimestampInCacheBlock_(pColInfo->pData, pCacheBlock, 0);
    info.window.ekey = getTimestampInCacheBlock_(pColInfo->pData, pCacheBlock, pCacheBlock->numOfPoints - 1);
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
    if (pQueryHandle->realNumOfRows == pDiskBlock->numOfPoints) {
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
    pBlock = getCacheDataBlock_(pQueryHandle, taosArrayGetP(pQueryHandle->pTableList, 0), pQueryHandle->cur.slot);
    SCacheBlock *pCacheBlock = (SCacheBlock *)pBlock;

    info.window.skey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pCacheBlock, 0);
    info.window.ekey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pCacheBlock, pCacheBlock->numOfPoints - 1);
    info.size = pCacheBlock->numOfPoints;
    info.numOfCols = pCacheBlock->pMeterObj->numOfColumns;
  }

  assert(pBlock != NULL);
  if (isMultiTableQuery(pQueryHandle)) {
    SMeterDataBlockInfoEx_ *pBlockInfoEx = &pQueryHandle->pDataBlockInfoEx[cur->slot];
    info.sid = pBlockInfoEx->pMeterDataInfo->pMeterObj->sid;
    info.uid = pBlockInfoEx->pMeterDataInfo->pMeterObj->uid;
  }

  return info;
}

char *doGetDataBlocks(SQuery *pQuery, SData **data, int32_t colIdx) {
  assert(colIdx >= 0 && colIdx < pQuery->numOfCols);
  char *pData = data[colIdx]->data;
  return pData;
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

TSKEY getQueryPositionForCacheInvalid(STsdbQueryHandle *pQueryHandle) {
  assert(0);
  return 0;

#if 0
//  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;
  int32_t    step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

//  dTrace(
//      "QInfo:%p vid:%d sid:%d id:%s cache block re-allocated to other meter, "
//      "try get query start position in file/cache, qrange:%" PRId64 "-%" PRId64 ", lastKey:%" PRId64,
//      pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery->lastKey);

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
#endif
}

bool getQualifiedDataBlock_rv(STsdbQueryHandle *pQueryHandle, int32_t type);

bool moveToNextBlockInCache_(STsdbQueryHandle *pQueryHandle, int32_t step) {
  assert(taosArrayGetSize(pQueryHandle->pTableList) == 1);

  SMeterObj *      pMeterObj = taosArrayGetP(pQueryHandle->pTableList, 0);
  SQueryHandlePos *cur = &pQueryHandle->cur;

  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;

  /*
   * ascending order to last cache block all data block in cache have been iterated, no need to set
   * pRuntimeEnv->nextPos. done
   */
  if (step == QUERY_ASC_FORWARD_STEP && cur->slot == pQueryHandle->currentSlot) {
    return DISK_DATA_LOADED;
  }

  /*
   * descending order to first cache block, try file
   * NOTE: use the real time cache information, not the snapshot
   */
  int32_t numOfBlocks = pCacheInfo->numOfBlocks;
  int32_t currentSlot = pCacheInfo->currentSlot;

  int32_t firstSlot = getFirstCacheSlot(numOfBlocks, currentSlot, pCacheInfo);

  // find the start position in the file
  if (step == QUERY_DESC_FORWARD_STEP && cur->slot == firstSlot) {
    return getQualifiedDataBlock_rv(pQueryHandle, QUERY_RANGE_LESS_EQUAL);
  }

  // now still iterate the cache data blocks
  cur->slot = (cur->slot + step + pCacheInfo->maxBlocks) % pCacheInfo->maxBlocks;
  SCacheBlock *pBlock = getCacheDataBlock_(pQueryHandle, pMeterObj, cur->slot);

  /*
   * data in this cache block has been flushed to disk, then we should locate the start position in file.
   * In both desc/asc query, this situation may occur. And we need to locate the start query position in file or cache.
   */
  if (pBlock == NULL) {
    getQueryPositionForCacheInvalid(pQueryHandle);
    return DISK_DATA_LOADED;
  }
  
  cur->pos = (QUERY_IS_ASC_QUERY_RV(pQueryHandle->order)) ? 0 : pBlock->numOfPoints - 1;
  return true;
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

void initQueryFileInfoFD_rv(SQueryFilesInfo_rv *pVnodeFilesInfo) {
  pVnodeFilesInfo->current = -1;
  pVnodeFilesInfo->headerFileSize = -1;

  pVnodeFilesInfo->headerFd = FD_INITIALIZER;  // set the initial value
  pVnodeFilesInfo->dataFd = FD_INITIALIZER;
  pVnodeFilesInfo->lastFd = FD_INITIALIZER;
}

bool setCurrentQueryRange_(int32_t order, STableQueryRec *pTableInfo, TSKEY endKey, TSKEY *minval, TSKEY *maxval) {
  //  SMeterObj *      pMeterObj = pTableInfo->pMeterObj;

  if (QUERY_IS_ASC_QUERY_RV(order)) {
    *minval = pTableInfo->lastKey;
    *maxval = endKey;
  } else {
    *minval = endKey;
    *maxval = pTableInfo->lastKey;
  }

  if (*minval > *maxval) {
    //    qTrace("QInfo:%p vid:%d sid:%d id:%s, no result in files, qrange:%" PRId64 "-%" PRId64 ", lastKey:%" PRId64,
    //    pQInfo,
    //           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pMeterQInfo->skey, pMeterQInfo->ekey,
    //           pTableInfo->lastKey);
    return false;
  } else {
    //    qTrace("QInfo:%p vid:%d sid:%d id:%s, query in files, qrange:%" PRId64 "-%" PRId64 ", lastKey:%" PRId64,
    //    pQInfo,
    //           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pMeterQInfo->skey, pMeterQInfo->ekey,
    //           pTableInfo->lastKey);
    return true;
  }
}

static bool getValidDataBlocksRangeIndex_(STableQueryRec *pTableInfo, SCompBlock *pCompBlock, int64_t numOfBlocks,
                                          TSKEY minval, TSKEY maxval, int32_t *end) {
  /*
   * search the possible blk that may satisfy the query condition always start from the min value, therefore,
   * the order is always ascending order
   */
  pTableInfo->start = binarySearchForBlockImpl(pCompBlock, (int32_t)numOfBlocks, minval, TSQL_SO_ASC);
  if (minval > pCompBlock[pTableInfo->start].keyLast || maxval < pCompBlock[pTableInfo->start].keyFirst) {
    //    dTrace("QInfo:%p vid:%d sid:%d id:%s, no result in files", pQInfo, pMeterObj->vnode, pMeterObj->sid,
    //           pMeterObj->meterId);
    return false;
  }

  // incremental checks following blocks until whose time range does not overlap with the query range
  *end = pTableInfo->start;
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

static bool setValidDataBlocks_(STableQueryRec *pTableInfo, int32_t end) {
  int32_t size = (end - pTableInfo->start) + 1;
  assert(size > 0);

  if (size != pTableInfo->numOfBlocks) {
    memmove(pTableInfo->pBlock, &pTableInfo->pBlock[pTableInfo->start], size * sizeof(SCompBlock));

    char *tmp = realloc(pTableInfo->pBlock, size * sizeof(SCompBlock));
    if (tmp == NULL) {
      return false;
    }

    pTableInfo->pBlock = (SCompBlock *)tmp;
    pTableInfo->numOfBlocks = size;
  }

  return true;
}

typedef struct SBlockOrderSupporter_ {
  int32_t                  numOfMeters;
  SMeterDataBlockInfoEx_ **pDataBlockInfoEx;
  int32_t *                blockIndexArray;
  int32_t *                numOfBlocksPerMeter;
} SBlockOrderSupporter_;

static int32_t blockAccessOrderComparator(const void *pLeft, const void *pRight, void *param);

int32_t createDataBlocksInfoEx_(SArray **pMeterDataInfo, int32_t numOfMeters, SMeterDataBlockInfoEx_ **pDataBlockInfoEx,
                                int32_t numOfCompBlocks, int32_t *numOfAllocBlocks) {
  // release allocated memory first
  //  freeDataBlockFieldInfo(*pDataBlockInfoEx, *numOfAllocBlocks);

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

  SBlockOrderSupporter_ supporter = {0};
  supporter.numOfMeters = numOfMeters;
  supporter.numOfBlocksPerMeter = calloc(1, sizeof(int32_t) * numOfMeters);
  supporter.blockIndexArray = calloc(1, sizeof(int32_t) * numOfMeters);
  supporter.pDataBlockInfoEx = calloc(1, POINTER_BYTES * numOfMeters);

  if (supporter.numOfBlocksPerMeter == NULL || supporter.blockIndexArray == NULL ||
      supporter.pDataBlockInfoEx == NULL) {
    //    cleanBlockOrderSupporter(&supporter, 0);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  int32_t cnt = 0;
  int32_t numOfQualMeters = 0;
  for (int32_t j = 0; j < numOfMeters; ++j) {
    STableQueryRec *pTableInfo = taosArrayGet(*pMeterDataInfo, j);

    if (pTableInfo->numOfBlocks == 0) {
      continue;
    }

    SCompBlock *pBlock = pTableInfo->pBlock;
    supporter.numOfBlocksPerMeter[numOfQualMeters] = pTableInfo->numOfBlocks;

    char *buf = calloc(1, sizeof(SMeterDataBlockInfoEx_) * pTableInfo->numOfBlocks);
    if (buf == NULL) {
      //      cleanBlockOrderSupporter(&supporter, numOfQualMeters);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }

    supporter.pDataBlockInfoEx[numOfQualMeters] = (SMeterDataBlockInfoEx_ *)buf;

    for (int32_t k = 0; k < pTableInfo->numOfBlocks; ++k) {
      SMeterDataBlockInfoEx_ *pBlockInfoEx = &supporter.pDataBlockInfoEx[numOfQualMeters][k];

      pBlockInfoEx->pBlock.compBlock = &pBlock[k];
      pBlockInfoEx->pBlock.fields = NULL;

      pBlockInfoEx->pMeterDataInfo = pTableInfo;
      //      pBlockInfoEx->groupIdx = pTableInfo->groupIdx;     // set the group index
      pBlockInfoEx->blockIndex = pTableInfo->start + k;  // set the block index in original meter
      cnt++;
    }

    numOfQualMeters++;
  }

  //  dTrace("QInfo %p create data blocks info struct completed", addr);

  assert(cnt == numOfCompBlocks && numOfQualMeters <= numOfMeters);  // the pTableInfo->numOfBlocks may be 0
  supporter.numOfMeters = numOfQualMeters;
  SLoserTreeInfo *pTree = NULL;

  uint8_t ret = tLoserTreeCreate(&pTree, supporter.numOfMeters, &supporter, blockAccessOrderComparator);
  if (ret != TSDB_CODE_SUCCESS) {
    //    cleanBlockOrderSupporter(&supporter, numOfMeters);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  int32_t numOfTotal = 0;

  while (numOfTotal < cnt) {
    int32_t                 pos = pTree->pNode[0].index;
    SMeterDataBlockInfoEx_ *pBlocksInfoEx = supporter.pDataBlockInfoEx[pos];
    int32_t                 index = supporter.blockIndexArray[pos]++;

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

  //  dTrace("QInfo %p %d data blocks sort completed", addr, cnt);
  //  cleanBlockOrderSupporter(&supporter, numOfMeters);
  free(pTree);

  return TSDB_CODE_SUCCESS;
}

int32_t getDataBlocksForMeters_(STsdbQueryHandle *pQueryHandle, const char *filePath, SArray *pQualTables) {
  TSKEY minval, maxval;

  pQueryHandle->numOfBlocks = 0;
  SQueryFilesInfo_rv *pVnodeFileInfo = &pQueryHandle->vnodeFileInfo;

  int32_t numOfTables = taosArrayGetSize(pQualTables);

  // sequentially scan this header file to extract the compHeader info
  for (int32_t j = 0; j < numOfTables; ++j) {
    STableQueryRec *pTableInfo = taosArrayGet(pQualTables, j);
    SMeterObj *     pMeterObj = pTableInfo->pMeterObj;

    lseek(pVnodeFileInfo->headerFd, pTableInfo->offsetInHeaderFile, SEEK_SET);

    SCompInfo compInfo = {0};
    read(pVnodeFileInfo->headerFd, &compInfo, sizeof(SCompInfo));

    int32_t ret = validateCompBlockInfoSegment_(pQueryHandle, filePath, pMeterObj->vnode, &compInfo,
                                                pTableInfo->offsetInHeaderFile);

    // file corrupted
    if (ret != TSDB_CODE_SUCCESS) {
      //      clearAllMeterDataBlockInfo(pMeterDataInfo, 0, numOfMeters);
      return TSDB_CODE_FILE_CORRUPTED;
    }

    if (compInfo.numOfBlocks <= 0 || compInfo.uid != pMeterObj->uid) {
      //      clearAllMeterDataBlockInfo(pMeterDataInfo, 0, numOfMeters);
      continue;
    }

    int32_t size = compInfo.numOfBlocks * sizeof(SCompBlock);
    size_t  bufferSize = size + sizeof(TSCKSUM);

    pTableInfo->numOfBlocks = compInfo.numOfBlocks;
    char *p = realloc(pTableInfo->pBlock, bufferSize);
    if (p == NULL) {
      //      clearAllMeterDataBlockInfo(pMeterDataInfo, 0, numOfMeters);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    } else {
      memset(p, 0, bufferSize);
      pTableInfo->pBlock = (SCompBlock *)p;
    }

    read(pVnodeFileInfo->headerFd, pTableInfo->pBlock, bufferSize);
    TSCKSUM checksum = *(TSCKSUM *)((char *)pTableInfo->pBlock + size);

    int64_t st = taosGetTimestampUs();

    // check compblock integrity
    ret = validateCompBlockSegment_(pQueryHandle, filePath, &compInfo, (char *)pTableInfo->pBlock, pMeterObj->vnode,
                                    checksum);
    if (ret != TSDB_CODE_SUCCESS) {
      //      clearAllMeterDataBlockInfo(pMeterDataInfo, 0, numOfMeters);
      return TSDB_CODE_FILE_CORRUPTED;
    }

    int64_t et = taosGetTimestampUs();

    //    pSummary->readCompInfo++;
    //    pSummary->totalCompInfoSize += (size + sizeof(SCompInfo) + sizeof(TSCKSUM));
    //    pSummary->loadCompInfoUs += (et - st);

    if (!setCurrentQueryRange_(pQueryHandle->order, pTableInfo, pQueryHandle->window.ekey, &minval, &maxval)) {
      //      clearAllMeterDataBlockInfo(pMeterDataInfo, j, j + 1);
      continue;
    }

    int32_t end = 0;
    if (!getValidDataBlocksRangeIndex_(pTableInfo, pTableInfo->pBlock, compInfo.numOfBlocks, minval, maxval, &end)) {
      // current table has no qualified data blocks, erase its information.
      //      clearAllMeterDataBlockInfo(pMeterDataInfo, j, j + 1);
      continue;
    }

    if (!setValidDataBlocks_(pTableInfo, end)) {
      //      clearAllMeterDataBlockInfo(pMeterDataInfo, 0, numOfMeters);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }

    //    qTrace("QInfo:%p vid:%d sid:%d id:%s, startIndex:%d, %d blocks qualified", pQInfo, pMeterObj->vnode,
    //    pMeterObj->sid,
    //           pMeterObj->meterId, pTableInfo->start, pTableInfo->numOfBlocks);
    pQueryHandle->numOfBlocks += pTableInfo->numOfBlocks;
  }

  return TSDB_CODE_SUCCESS;
}

static void freeDataBlockFieldInfo(SMeterDataBlockInfoEx *pDataBlockInfoEx, int32_t len) {
  for (int32_t i = 0; i < len; ++i) {
    tfree(pDataBlockInfoEx[i].pBlock.fields);
  }
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
    return false;
  }

  return true;
}

int64_t getQueryStartPositionInCache_rv(STsdbQueryHandle *pQueryHandle, int32_t *slot, int32_t *pos,
                                        bool ignoreQueryRange) {
  // keep in-memory cache status in local variables in case that it may be changed by write operation
  SMeterObj *pTable = (SMeterObj *)taosArrayGetP(pQueryHandle->pTableList, 0);
  getBasicCacheInfoSnapshot_(pQueryHandle, pTable->pCache, pTable->vnode);

  SCacheInfo *pCacheInfo = (SCacheInfo *)pTable->pCache;
  if (pCacheInfo == NULL || pCacheInfo->cacheBlocks == NULL || pQueryHandle->numOfCacheBlocks == 0) {
    return -1;
  }

  if (!ignoreQueryRange && !cacheBoundaryCheck_(pQueryHandle, pTable)) {
    return -1;
  }

  /* here we actual start to query from pQuery->lastKey */
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

  // all data are less(greater) than the pQuery->lastKey in case of ascending(descending) query
  if (*pos == -1) {
    return -1;
  }

  SColumnInfoEx_ *pColInfo = taosArrayGet(pQueryHandle->pColumns, 0);

  int64_t nextKey = getTimestampInCacheBlock_(pColInfo->pData, pBlock, *pos);
  return nextKey;
}

bool getNeighborPoints(STableQuerySupportObj *pSupporter, SMeterObj *pMeterObj,
                       SPointInterpoSupporter *pPointInterpSupporter) {
#if 0
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

//    savePointPosition(&pRuntimeEnv->startPos, pQuery->fileId, pQuery->slot, pQuery->pos);

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
#endif
  return true;
}

/**
 * check if data in disk.
 */
bool hasDataInDisk_rv(STimeWindow window, SMeterObj *pMeterObj, int32_t order) {
  SVnodeObj *pVnode = &vnodeList[pMeterObj->vnode];
  if (pVnode->numOfFiles <= 0) {
    return false;
  }

  int64_t latestKey = pMeterObj->lastKeyOnFile;
  int64_t oldestKey = getOldestKey(pVnode->numOfFiles, pVnode->fileId, &pVnode->cfg);

  /* query range is out of current time interval of table */
  if (((window.skey > latestKey || window.ekey < oldestKey) && QUERY_IS_ASC_QUERY_RV(order)) ||
      ((window.skey < oldestKey || window.ekey > latestKey) && !QUERY_IS_ASC_QUERY_RV(order))) {
    return false;
  }

  return true;
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
  if (pQueryHandle->numOfCacheBlocks <= 0) {
    return false;
  }

  return cacheBoundaryCheck_(pQueryHandle, pMeterObj);
}

#if 0
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

#endif

bool isCacheBlockValid_(STsdbQueryHandle *pQueryHandle, SCacheBlock *pBlock, SMeterObj *pMeterObj, int32_t slot);

// todo all functions that call this function should check the returned data blocks status
SCacheBlock *getCacheDataBlock_(STsdbQueryHandle *pQueryHandle, SMeterObj *pMeterObj, int32_t slot) {
  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  if (pCacheInfo == NULL || pCacheInfo->cacheBlocks == NULL || slot < 0 || slot >= pCacheInfo->maxBlocks) {
    return NULL;
  }

  getBasicCacheInfoSnapshot_(pQueryHandle, pCacheInfo, pMeterObj->vnode);

  SCacheBlock *pBlock = pCacheInfo->cacheBlocks[slot];
  if (pBlock == NULL) {  // the cache info snapshot must exist.
    dError(
        "QInfo:%p handle:%p NULL Block In Cache, snapshot (available blocks:%d, last block:%d), current (available "
        "blocks:%d, "
        "last block:%d), accessed null block:%d, pBlockId:%d",
        pQueryHandle->qinfo, pQueryHandle, pQueryHandle->numOfBlocks, pQueryHandle->currentSlot,
        pCacheInfo->numOfBlocks, pCacheInfo->currentSlot, slot, pQueryHandle->blockId);

    return NULL;
  }

  // block is empty or block does not belongs to current table, return NULL value
  if (!isCacheBlockValid_(pQueryHandle, pBlock, pMeterObj, slot)) {
    return NULL;
  }

  // the accessed cache block has been loaded already, return directly
  if (vnodeIsDatablockLoaded_(&pQueryHandle->cur, &pQueryHandle->dataBlockLoadInfo, pMeterObj, -1) ==
      DISK_BLOCK_NO_NEED_TO_LOAD) {
    TSKEY skey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pBlock, 0);
    TSKEY ekey = getTimestampInCacheBlock_(pQueryHandle->tsBuf, pBlock, pBlock->numOfPoints - 1);

    dTrace(
        "Handle:%p QInfo:%p vid:%d sid:%d id:%s, fileId:%d, cache block has been loaded, no need to load again, ts:%d, "
        "slot:%d, brange:%lld-%lld, rows:%d",
        pQueryHandle, pQueryHandle->qinfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId,
        pQueryHandle->cur.fileId, 1, pQueryHandle->cur.slot, skey, ekey, pBlock->numOfPoints);

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

    SColumn *pCol = NULL;
    int16_t  bytes = pColumnInfoEx->info.bytes;
    int16_t  type = pColumnInfoEx->info.type;

    char *  dst = pColumnInfoEx->pData->data;
    int32_t index = -1;

    for (int32_t j = 0; j < pMeterObj->numOfColumns; ++j) {
      SColumn *pColumn = &pMeterObj->schema[j];
      if (pColumn->colId == pColumnInfoEx->info.colId) {
        pCol = pColumn;
        index = j;
        break;
      }
    }

    if (pCol != NULL) {
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

  // double check to make sure the copyed cache data is still valid
  if (!isCacheBlockValid_(pQueryHandle, pNewBlock, pMeterObj, slot)) {
    return NULL;
  }

  /*
   * the accessed cache block still belongs to current meterObj, go on
   * update the load data block info
   */
  SArray *sa = getDefaultLoadColumns(pQueryHandle, true);
  vnodeSetDataBlockInfoLoaded_(pQueryHandle, pMeterObj, -1, sa, &pQueryHandle->cur);

  filterDataInDataBlock(pQueryHandle, sa, pNewBlock->numOfPoints);
  
  //  dTrace("QInfo:%p vid:%d sid:%d id:%s, fileId:%d, load cache block, ts:%d, slot:%d, brange:%lld-%lld, rows:%d",
  //         GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->fileId, 1,
  //         pQuery->slot, skey, ekey, numOfPoints);

  return pNewBlock;
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

bool isCacheBlockValid_(STsdbQueryHandle *pQueryHandle, SCacheBlock *pBlock, SMeterObj *pMeterObj, int32_t slot) {
  if (pMeterObj != pBlock->pMeterObj || pBlock->blockId > pQueryHandle->blockId) {
    SMeterObj *pNewMeterObj = pBlock->pMeterObj;

    char *id = (pNewMeterObj != NULL) ? pNewMeterObj->meterId : NULL;

    dWarn(
        "QInfo:%p handle:%p vid:%d sid:%d id:%s, cache block is overwritten, slot:%d blockId:%d qBlockId:%d, "
        "meterObj:%p, "
        "blockMeterObj:%p, blockMeter id:%s, first:%d, last:%d, numOfBlocks:%d",
        pQueryHandle->qinfo, pQueryHandle, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQueryHandle->cur.slot,
        pBlock->blockId, pQueryHandle->blockId, pMeterObj, pNewMeterObj, id, pQueryHandle->firstSlot,
        pQueryHandle->currentSlot, pQueryHandle->numOfBlocks);
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

bool isIntervalQuery(SQuery *pQuery) { return pQuery->intervalTime > 0; }

/////////////////////////////////////////////////////////////////////////////////////////////
// new api
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

  // todo no need to loaded!!!
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
  return pQueryHandle->realNumOfRows > 0;
}

/**
 * NOTE: this function will change the cur->pos, so this function should not invoked repeated.
 *
 * @param pQueryHandle
 * @param sa
 * @param numOfPoints
 */
void filterDataInDataBlock(STsdbQueryHandle *pQueryHandle, SArray *sa, int32_t numOfPoints) {
  // only return the qualified data to client in terms of query time window, data rows in the same block but do not
  // be included in the query time window will be discarded
  SQueryHandlePos *cur = &pQueryHandle->cur;

  __block_search_fn_t searchFn = vnodeSearchKeyFunc[0];
  SDataBlockInfo      blockInfo = getTrueBlockInfo(pQueryHandle);

  int32_t endPos = cur->pos;
  if (QUERY_IS_ASC_QUERY_RV(pQueryHandle->order) && pQueryHandle->window.ekey > blockInfo.window.ekey) {
    endPos = blockInfo.size - 1;
    pQueryHandle->realNumOfRows = endPos - cur->pos + 1;
  } else if (!QUERY_IS_ASC_QUERY_RV(pQueryHandle->order) && pQueryHandle->window.ekey < blockInfo.window.skey) {
    endPos = 0;
    pQueryHandle->realNumOfRows = cur->pos + 1;
  } else {
    endPos = searchFn(pQueryHandle->tsBuf->data, blockInfo.size, pQueryHandle->window.ekey, pQueryHandle->order);
    
    if (QUERY_IS_ASC_QUERY_RV(pQueryHandle->order)) {
      if (endPos < cur->pos) {
        pQueryHandle->realNumOfRows = 0;
        return;
      } else {
        pQueryHandle->realNumOfRows = endPos - cur->pos;
      }
    } else {
      if (endPos > cur->pos) {
        pQueryHandle->realNumOfRows = 0;
        return;
      } else {
        pQueryHandle->realNumOfRows = cur->pos - endPos;
      }
    }
  }

  int32_t start = MIN(cur->pos, endPos);
  
  // move the data block in the front to data block if needed
  if (start != 0) {
    int32_t numOfCols = QH_GET_NUM_OF_COLS(pQueryHandle);
    
    for (int32_t i = 0; i < taosArrayGetSize(sa); ++i) {
      int16_t colId = *(int16_t *)taosArrayGet(sa, i);
    
      for (int32_t j = 0; j < numOfCols; ++j) {
        SColumnInfoEx_ *pCol = taosArrayGet(pQueryHandle->pColumns, j);
      
        if (pCol->info.colId == colId) {
          memmove(pCol->pData->data, ((char *)pCol->pData->data) + pCol->info.bytes * start,
                  pQueryHandle->realNumOfRows * pCol->info.bytes);
          break;
        }
      }
    }
  }

  

  assert(pQueryHandle->realNumOfRows <= blockInfo.size);
  
  // forward(backward) the position for cursor
  cur->pos = endPos;
}

static bool findStartPosition_(STsdbQueryHandle *pQueryHandle) {
  pQueryHandle->locateStart = true;
  SMeterObj *pMeterObj = taosArrayGetP(pQueryHandle->pTableList, 0);

  if (QUERY_IS_ASC_QUERY_RV(pQueryHandle->order)) {
    bool hasData = hasDataInDisk_rv(pQueryHandle->window, pMeterObj, pQueryHandle->order);
    if (hasData) {
      hasData = getQualifiedDataBlock_rv(pQueryHandle, QUERY_RANGE_GREATER_EQUAL);
      if (hasData) {
        pQueryHandle->start = pQueryHandle->cur;
        return true;
      }
    }

    hasData = hasDataInCache_(pQueryHandle, taosArrayGetP(pQueryHandle->pTableList, 0));
    if (hasData) {
      TSKEY key = getQueryStartPositionInCache_rv(pQueryHandle, &pQueryHandle->cur.slot, &pQueryHandle->cur.pos, false);
    }

    pQueryHandle->start = pQueryHandle->cur;
    return hasData;
  } else {  // descending order
    bool hasData = hasDataInCache_(pQueryHandle, taosArrayGetP(pQueryHandle->pTableList, 0));
    if (hasData) {
      int64_t ret =
          getQueryStartPositionInCache_rv(pQueryHandle, &pQueryHandle->cur.slot, &pQueryHandle->cur.pos, false);
      if (ret > 0) {
        pQueryHandle->start = pQueryHandle->cur;
        return true;
      }
    }

    hasData = hasDataInDisk_rv(pQueryHandle->window, pMeterObj, pQueryHandle->order);
    if (hasData) {
      hasData = getQualifiedDataBlock_rv(pQueryHandle, QUERY_RANGE_LESS_EQUAL);
      if (hasData) {
        pQueryHandle->start = pQueryHandle->cur;
      }
    }

    return hasData;
  }
}

static int32_t doAllocateBuf(STsdbQueryHandle *pQueryHandle, int32_t rowsPerFileBlock);

SMeterQueryInfo *createMeterQueryInfo_(SQueryRuntimeEnv *pRuntimeEnv, int32_t sid, TSKEY skey, TSKEY ekey) {
  SMeterQueryInfo *pMeterQueryInfo = calloc(1, sizeof(SMeterQueryInfo));

  pMeterQueryInfo->skey = skey;
  pMeterQueryInfo->ekey = ekey;
  pMeterQueryInfo->lastKey = skey;

  pMeterQueryInfo->sid = sid;
  pMeterQueryInfo->cur.vnodeIndex = -1;

  initWindowResInfo(&pMeterQueryInfo->windowResInfo, pRuntimeEnv, 100, 100, TSDB_DATA_TYPE_INT);
  return pMeterQueryInfo;
}

static int32_t offsetComparator(const void *pLeft, const void *pRight) {
  STableQueryRec *pLeft1 = (STableQueryRec *)pLeft;
  STableQueryRec *pRight1 = (STableQueryRec *)pRight;

  if (pLeft1->offsetInHeaderFile == pRight1->offsetInHeaderFile) {
    return 0;
  }

  return (pLeft1->offsetInHeaderFile > pRight1->offsetInHeaderFile) ? 1 : -1;
}

/**
 *
 * @param pQInfo
 * @param fid
 * @param pQueryFileInfo
 * @param start
 * @param end
 * @param pMeterHeadDataInfo
 * @return
 */
int32_t vnodeFilterQualifiedMeters_(STsdbQueryHandle *pQueryHandle, int32_t vid, SArray *pReqMeterDataInfo) {
  SVnodeObj *pVnode = &vnodeList[vid];
  int32_t    numOfTables = taosArrayGetSize(pQueryHandle->pTableList);

  int32_t headerSize = getCompHeaderSegSize(&pVnode->cfg);
  char *  buf = calloc(1, headerSize);
  if (buf == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }
  SQueryFilesInfo_rv *pVnodeFileInfo = &pQueryHandle->vnodeFileInfo;

  lseek(pVnodeFileInfo->headerFd, TSDB_FILE_HEADER_LEN, SEEK_SET);
  read(pVnodeFileInfo->headerFd, buf, headerSize);

  // check the offset value integrity
  if (validateHeaderOffsetSegment_(pQueryHandle, pVnodeFileInfo->headerFilePath, vid, buf - TSDB_FILE_HEADER_LEN,
                                   headerSize) < 0) {
    free(buf);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  int64_t oldestKey = getOldestKey(pVnode->numOfFiles, pVnode->fileId, &pVnode->cfg);
  TSKEY   skey, ekey;

  for (int32_t i = 0; i < numOfTables; ++i) {  // load all meter meta info
    SMeterObj *pMeterObj = taosArrayGetP(pQueryHandle->pTableList, i);
    assert(pMeterObj != NULL);
    skey = pQueryHandle->pTableQueryInfo[i].lastKey;

    // query on disk data files, which actually starts from the lastkey
    ekey = pQueryHandle->window.ekey;

    if (QUERY_IS_ASC_QUERY_RV(pQueryHandle->order)) {
      assert(skey >= pQueryHandle->window.skey);
      if (ekey < oldestKey || skey > pMeterObj->lastKeyOnFile) {
        continue;
      }
    } else {
      assert(skey <= pQueryHandle->window.skey);
      if (skey < oldestKey || ekey > pMeterObj->lastKeyOnFile) {
        continue;
      }
    }

    int64_t      headerOffset = sizeof(SCompHeader) * pMeterObj->sid;
    SCompHeader *compHeader = (SCompHeader *)(buf + headerOffset);
    if (compHeader->compInfoOffset == 0) {  // current table is empty
      continue;
    }

    // corrupted file may cause the invalid compInfoOffset, check needs
    int32_t compHeaderOffset = getCompHeaderStartPosition(&pVnode->cfg);
    if (validateCompBlockOffset_(pQueryHandle, pMeterObj, compHeader, &pQueryHandle->vnodeFileInfo, compHeaderOffset) !=
        TSDB_CODE_SUCCESS) {
      free(buf);
      return TSDB_CODE_FILE_CORRUPTED;
    }

    STableQueryRec rec = {0};
    rec.offsetInHeaderFile = (uint64_t)compHeader->compInfoOffset;
    rec.pMeterObj = pMeterObj;
    rec.lastKey = pQueryHandle->window.skey;

    taosArrayPush(pReqMeterDataInfo, &rec);
  }

  /* enable sequentially access*/
  size_t size = taosArrayGetSize(pReqMeterDataInfo);
  if (size > 1) {
    qsort(pReqMeterDataInfo->pData, size, sizeof(STableQueryRec), offsetComparator);
  }

  free(buf);

  return TSDB_CODE_SUCCESS;
}

static int32_t getDataBlocksFromFile(STsdbQueryHandle *pQueryHandle, SArray *pTableList) {
  assert(taosArrayGetSize(pTableList) > 1 && pQueryHandle != NULL);

  SMeterObj *pTable = taosArrayGetP(pTableList, 0);
  int32_t    step = 1;
  bool       asc = QUERY_IS_ASC_QUERY_RV(pQueryHandle->order);

  if (asc) {
    SQueryHandlePos *cur = &pQueryHandle->cur;

    while (1) {
      if (cur->fileId == 0) {
        pQueryHandle->cur.fileId = getFileIdFromKey(pTable->vnode, pQueryHandle->window.skey);
      } else {
        pQueryHandle->cur.fileId += 1;
      }

      cur->fileIndex = vnodeGetVnodeHeaderFileIndex_(&cur->fileId, pQueryHandle->order, &pQueryHandle->vnodeFileInfo);
      if (cur->fileIndex < 0) {  // no valid file, abort current search
        break;
      }

      if (vnodeGetHeaderFile_(&pQueryHandle->vnodeFileInfo, cur->fileIndex) == TSDB_CODE_SUCCESS) {
        break;
      }

      cur->fileId += step;
    }

    if (cur->fileIndex < 0) {
      return -1;
    }

    int32_t numOfTable = taosArrayGetSize(pQueryHandle->pTableList);
    if (pQueryHandle->pTableQueryInfo == NULL) {
      pQueryHandle->pTableQueryInfo = calloc(numOfTable, sizeof(STableQueryRec));
      for (int32_t i = 0; i < numOfTable; ++i) {
        pQueryHandle->pTableQueryInfo[i].lastKey = pQueryHandle->window.skey;
      }
    }

    SArray *pQualTables = taosArrayInit(10, sizeof(STableQueryRec));
    int32_t ret = vnodeFilterQualifiedMeters_(pQueryHandle, pTable->vnode, pQualTables);
    if (ret != TSDB_CODE_SUCCESS) {
      //      dError("QInfo:%p failed to create meterdata struct to perform query processing, abort", pQInfo);
    }

    ret = getDataBlocksForMeters_(pQueryHandle, pQueryHandle->vnodeFileInfo.headerFilePath, pQualTables);
    if (ret != TSDB_CODE_SUCCESS) {
      //      dError("QInfo:%p failed to get data block before scan data blocks, abort", pQInfo);
      //      pQInfo->code = -ret;
      //      pQInfo->killed = 1;

      //      return;
    }

    int32_t numOfTables = taosArrayGetSize(pQualTables);
    int32_t nAllocBlocksInfoSize = 0;

    ret = createDataBlocksInfoEx_(&pQualTables, numOfTables, &pQueryHandle->pDataBlockInfoEx, pQueryHandle->numOfBlocks,
                                  &nAllocBlocksInfoSize);
    if (ret != TSDB_CODE_SUCCESS) {  // failed to create data blocks
                                     //      dError("QInfo:%p build blockInfoEx failed, abort", pQInfo);
                                     //      pQInfo->code = -ret;
                                     //      pQInfo->killed = 1;
                                     //      return;
    }
  }
}

static int32_t getDataBlockFromCache(STsdbQueryHandle *pQueryHandle, SArray *pTableList) {
  int32_t numOfTable = taosArrayGetSize(pQueryHandle->pTableList);
  if (pQueryHandle->pTableQueryInfo == NULL) {
    pQueryHandle->pTableQueryInfo = calloc(numOfTable, sizeof(STableQueryRec));
    for (int32_t i = 0; i < numOfTable; ++i) {
      pQueryHandle->pTableQueryInfo[i].lastKey = pQueryHandle->window.skey;
    }
  }

  // todo handle asc or desc order traverse
  while (pQueryHandle->tableIndex < numOfTable) {
    SMeterObj *pMeterObj = taosArrayGetP(pQueryHandle->pTableList, pQueryHandle->tableIndex);
    /*
     * find the appropriated start position in cache
     * NOTE: (taking ascending order query for example)
     * for the specific query range [pQuery->lastKey, pQuery->ekey], there may be no qualified result in cache.
     * Therefore, we need the first point that is greater(less) than the pQuery->lastKey, so the boundary check
     * should be ignored (the fourth parameter).
     */
    //  TSKEY nextKey = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, true);
    //  if (nextKey < 0 || !doCheckWithPrevQueryRange(pQuery, nextKey)) {
    //    qTrace("QInfo:%p vid:%d sid:%d id:%s, no data qualified in cache, cache blocks:%d, lastKey:%" PRId64, pQInfo,
    //           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->numOfBlocks, pQuery->lastKey);
    //    continue;
    //  }

    // data in this block may be flushed to disk and this block is allocated to other meter
    // todo try with remain cache blocks
    //  SCacheBlock *pBlock = getCacheDataBlock_(pMeterObj, pRuntimeEnv, pQuery->slot);
    //  if (pBlock == NULL) {
    //    continue;
    //  }

    SQueryHandlePos *cur = &pQueryHandle->cur;

    SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
    if (pQueryHandle->isFirstSlot) {
      TSKEY key = getQueryStartPositionInCache_rv(pQueryHandle, &cur->slot, &cur->pos, true);
      if (key < 0) {
        pQueryHandle->tableIndex++;
        pQueryHandle->isFirstSlot = true;
        continue;
      }

      pQueryHandle->isFirstSlot = false;
    } else {
      FORWARD_CACHE_BLOCK_CHECK_SLOT(pQueryHandle->cur.slot, 1, pCacheInfo->maxBlocks);

      // try next table
      if (ALL_CACHE_BLOCKS_CHECKED(pQueryHandle->cur, pQueryHandle)) {
        pQueryHandle->tableIndex++;
        pQueryHandle->isFirstSlot = true;
        continue;
      }

      SCacheBlock *pBlock = getCacheDataBlock_(pQueryHandle, pMeterObj, pQueryHandle->cur.slot);
      return 0;
    }
  }

  return -1;
}

STsdbQueryHandle *tsdbQueryByTableId(STsdbQueryCond *pCond, SArray *pTableList, SArray *pColumnInfo) {
  STsdbQueryHandle *pQueryHandle = calloc(1, sizeof(STsdbQueryHandle));
  pQueryHandle->order = pCond->order;
  pQueryHandle->window = pCond->window;

  pQueryHandle->pTableList = pTableList;
  pQueryHandle->pColumns = pColumnInfo;
  pQueryHandle->loadDataAfterSeek = false;
  pQueryHandle->isFirstSlot = true;

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

static bool getNextDataBlockForMultiTables(STsdbQueryHandle *pQueryHandle) {
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[0];

  SQueryHandlePos *cur = &pQueryHandle->cur;

  // data block in current file are all checked already, try next file
  if (++cur->slot >= pQueryHandle->numOfBlocks) {
    int32_t ret = getDataBlocksFromFile(pQueryHandle, pQueryHandle->pTableList);
    if (ret != 0) {  // no data in file, try cache
      ret = getDataBlockFromCache(pQueryHandle, pQueryHandle->pTableList);
      return (ret == 0);
    } else {
      cur->slot = 0;  // the first block in the next file

      SMeterDataBlockInfoEx_ *pBlockInfoEx = &pQueryHandle->pDataBlockInfoEx[cur->slot];
      SCompBlock *            pBlock = pBlockInfoEx->pBlock.compBlock;

      STimeWindow *pw = &pQueryHandle->window;
      int32_t      order = pQueryHandle->order;
      SArray *     sa = getDefaultLoadColumns(pQueryHandle, true);

      if (((pw->skey > pBlock->keyFirst || pw->ekey < pBlock->keyLast) && QUERY_IS_ASC_QUERY_RV(order)) ||
          ((pw->ekey > pBlock->keyFirst || pw->skey < pBlock->keyLast) && !QUERY_IS_ASC_QUERY_RV(order))) {
        // data needs to load
        if (loadDataBlockIntoMem_(pQueryHandle, &pQueryHandle->pBlock[cur->slot], &pQueryHandle->pFields[cur->slot],
                                  cur->fileId, sa) == 0) {
          // search qualified points in blk, according to primary key (timestamp) column
          // todo the last key for each table, is not inconsistant
          cur->pos =
              searchFn(pQueryHandle->tsBuf->data, pBlock->numOfPoints, pQueryHandle->window.skey, pQueryHandle->order);
          assert(cur->pos >= 0 && cur->fileId >= 0 && cur->slot >= 0);
        }
      } else {
      }

      filterDataInDataBlock(pQueryHandle, sa, pBlock->numOfPoints);
      return true;
    }
  } else {
    SMeterDataBlockInfoEx_ *pBlockInfoEx = &pQueryHandle->pDataBlockInfoEx[cur->slot];
    SCompBlock *            pBlock = pBlockInfoEx->pBlock.compBlock;

    STimeWindow *pw = &pQueryHandle->window;
    int32_t      order = pQueryHandle->order;

    if (((pw->skey > pBlock->keyFirst || pw->ekey < pBlock->keyLast) && QUERY_IS_ASC_QUERY_RV(order)) ||
        ((pw->ekey > pBlock->keyFirst || pw->skey < pBlock->keyLast) && !QUERY_IS_ASC_QUERY_RV(order))) {
      // data needs load
      SArray *sa = getDefaultLoadColumns(pQueryHandle, true);

      if (loadDataBlockIntoMem_(pQueryHandle, &pQueryHandle->pBlock[cur->slot], &pQueryHandle->pFields[cur->slot],
                                cur->fileId, sa) == 0) {
        // search qualified points in blk, according to primary key (timestamp) column
        // todo the last key for eache table, is not consistant
        cur->pos =
            searchFn(pQueryHandle->tsBuf->data, pBlock->numOfPoints, pQueryHandle->window.skey, pQueryHandle->order);
        assert(cur->pos >= 0 && cur->fileId >= 0 && cur->slot >= 0);

        filterDataInDataBlock(pQueryHandle, sa, pBlock->numOfPoints);
      }
    }

    return true;
  }
}

bool tsdbNextDataBlock(STsdbQueryHandle *pQueryHandle) {
  if (pQueryHandle == NULL) {
    return false;
  }

  if (isMultiTableQuery(pQueryHandle)) {
    return getNextDataBlockForMultiTables(pQueryHandle);
  }

  if (pQueryHandle->loadDataAfterSeek) {
    if (pQueryHandle->cur.fileId == 0) {
      pQueryHandle->cur = pQueryHandle->start;
    }

    SQueryHandlePos *cur = &pQueryHandle->cur;
    SMeterObj *      pTable = (SMeterObj *)taosArrayGetP(pQueryHandle->pTableList, 0);

    if (cur->fileId > 0) {  // file
      SArray *sa = getDefaultLoadColumns(pQueryHandle, true);
      vnodeGetCompBlockInfo_(pQueryHandle, pTable, cur->fileIndex);
      loadDataBlockIntoMem_(pQueryHandle, &pQueryHandle->pBlock[cur->slot], &pQueryHandle->pFields[cur->slot],
                            cur->fileIndex, sa);

      filterDataInDataBlock(pQueryHandle, sa, pQueryHandle->pBlock[cur->slot].numOfPoints);
    } else {  // todo handle the endpoint
      getCacheDataBlock_(pQueryHandle, pTable, cur->slot);
    }

    pQueryHandle->loadDataAfterSeek = false;
    return true;
  }

  // the start position does not locate yet
  if (!pQueryHandle->locateStart) {
    return findStartPosition_(pQueryHandle);
  } else {
    int32_t step = QUERY_IS_ASC_QUERY_RV(pQueryHandle->order) ? 1 : -1;
    return moveToNextBlock_(pQueryHandle, step);
  }
}

SDataBlockInfo tsdbRetrieveDataBlockInfo(STsdbQueryHandle *pQueryHandle) { return getBlockInfo_(pQueryHandle); }

static bool completedIncluded(STimeWindow *win, SDataBlockInfo *pBlockInfo) {
  return !(win->skey > pBlockInfo->window.skey || win->ekey < pBlockInfo->window.ekey);
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
  STimeWindow *pw = &pQueryHandle->window;
  int32_t      order = pQueryHandle->order;
  if (((pw->skey > blockInfo.window.skey || pw->ekey < blockInfo.window.ekey) && QUERY_IS_ASC_QUERY_RV(order)) ||
      ((pw->ekey > blockInfo.window.skey || pw->skey < blockInfo.window.ekey) && !QUERY_IS_ASC_QUERY_RV(order))) {
    *pBlockStatis = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // load the file block info
  loadDataBlockFieldsInfo_(pQueryHandle, &pQueryHandle->pBlock[slot], &pQueryHandle->pFields[slot]);
  (*pBlockStatis) = calloc(blockInfo.numOfCols, sizeof(SDataStatis));

  // todo move the statistics data to SColumnInfoEx_ struct
  for (int32_t i = 0; i < blockInfo.numOfCols; ++i) {
    SField *pField = &pQueryHandle->pFields[slot][i];

    (*pBlockStatis)[i].colId = pField->colId;
    (*pBlockStatis)[i].numOfNull = pField->numOfNullPoints;
    (*pBlockStatis)[i].sum = pField->sum;

    (*pBlockStatis)[i].min = pField->min;
    (*pBlockStatis)[i].max = pField->max;

    (*pBlockStatis)[i].minIndex = pField->minIndex;
    (*pBlockStatis)[i].maxIndex = pField->maxIndex;
  }

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
  // in case of cache data, return directly
  if (pQueryHandle->cur.fileId < 0) {
    return pQueryHandle->pColumns;
  }

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
    int16_t colId = *(int16_t *)taosArrayGet(pLocalIdList, 0);

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

int32_t tsdbDataBlockSeek(STsdbQueryHandle *pQueryHandle, tsdbpos_t pos) {
  if (pos == NULL || pQueryHandle == NULL) {
    return -1;
  }

  // seek to current position, do nothing
  pQueryHandle->cur = *(SQueryHandlePos *)pos;
  pQueryHandle->loadDataAfterSeek = true;
}

tsdbpos_t tsdbDataBlockTell(STsdbQueryHandle *pQueryHandle) {
  if (pQueryHandle == NULL) {
    return NULL;
  }

  SQueryHandlePos *pPos = calloc(1, sizeof(SQueryHandlePos));
  memcpy(pPos, &pQueryHandle->cur, sizeof(SQueryHandlePos));

  return pPos;
}

#if 0
static bool doFindGreaterEqualData(STsdbQueryHandle *pQueryHandle) {
  SMeterObj *pTable = taosArrayGetP(pQueryHandle->pTableList, 0);

  bool hasData = hasDataInDisk_rv(pQueryHandle->window, pTable, pQueryHandle->order);
  if (hasData) {
    hasData = getQualifiedDataBlock_rv(pQueryHandle, QUERY_RANGE_GREATER_EQUAL);
    if (hasData) {
      pQueryHandle->start = pQueryHandle->cur;
      return true;
    }
  }

  hasData = hasDataInCache_(pQueryHandle, taosArrayGetP(pQueryHandle->pTableList, 0));
  if (hasData) {
    TSKEY key = getQueryStartPositionInCache_rv(pQueryHandle, &pQueryHandle->cur.slot, &pQueryHandle->cur.pos, false);
  }

  pQueryHandle->start = pQueryHandle->cur;
  return hasData;
}

static bool doFindLessEqualData(STsdbQueryHandle *pQueryHandle) {
  SMeterObj *pTable = taosArrayGetP(pQueryHandle->pTableList, 0);

  bool hasData = hasDataInCache_(pQueryHandle, taosArrayGetP(pQueryHandle->pTableList, 0));
  if (hasData) {
    int64_t ret = getQueryStartPositionInCache_rv(pQueryHandle, &pQueryHandle->cur.slot, &pQueryHandle->cur.pos, false);
    if (ret > 0) {
      pQueryHandle->start = pQueryHandle->cur;
      return true;
    }
  }

  hasData = hasDataInDisk_rv(pQueryHandle->window, pTable, pQueryHandle->order);
  if (hasData) {
    hasData = getQualifiedDataBlock_rv(pQueryHandle, QUERY_RANGE_LESS_EQUAL);
    if (hasData) {
      pQueryHandle->start = pQueryHandle->cur;
    }
  }

  return hasData;
}

SArray *tsdbRetrieveDataRow(STsdbQueryHandle *pQueryHandle, SArray *pIdList, SQueryRowCond *pCond) {
  assert(pCond->rel == TSDB_TS_LESS_EQUAL || pCond->rel == TSDB_TS_GREATER_EQUAL);

  if (pQueryHandle == NULL) {
    return NULL;
  }

  STimeWindow     w = pQueryHandle->window;
  int32_t         order = pQueryHandle->order;
  SQueryHandlePos cur = pQueryHandle->cur;
  SQueryHandlePos start = pQueryHandle->start;

  TSKEY lastKey = pQueryHandle->lastKey;

  if (pCond->rel == TSDB_TS_LESS_EQUAL) {
    STimeWindow win = {.skey = pCond->ts, .ekey = 0};
    pQueryHandle->window = win;
    pQueryHandle->order = TSQL_SO_DESC;

    bool ret = doFindLessEqualData(pQueryHandle);
    if (ret) {
      SArray *r = tsdbRetrieveDataBlock(pQueryHandle, pIdList);
      assert(0);
    }
  } else {
    STimeWindow win = {.skey = pCond->ts, .ekey = INT64_MAX};
    pQueryHandle->window = win;
    pQueryHandle->lastKey = win.skey;

    pQueryHandle->order = TSQL_SO_ASC;
    bool ret = doFindGreaterEqualData(pQueryHandle);
    if (ret) {
      SArray *r = tsdbRetrieveDataBlock(pQueryHandle, pIdList);

      pQueryHandle->window = w;
      pQueryHandle->order = order;
      pQueryHandle->cur = cur;
      pQueryHandle->start = start;
      pQueryHandle->lastKey = lastKey;

      return r;
    }
  }
}
#endif

STsdbQueryHandle *tsdbQueryFromTagConds(STsdbQueryCond *pCond, int16_t stableId, const char *pTagFilterStr) {
  return NULL;
}

int32_t tsdbResetQuery(STsdbQueryHandle *pQueryHandle, STimeWindow *window, tsdbpos_t position, int16_t order) {
  if (order != 0 && order != 1) {
    return -1;
  }

  if (pQueryHandle == NULL || position == NULL) {
    return -1;
  }

  pQueryHandle->window = *window;

  tsdbDataBlockSeek(pQueryHandle, position);
  pQueryHandle->order = order;

  return 0;
}