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

#include "os.h"
#include "talgo.h"
#include "tchecksum.h"
#include "tcoding.h"
#include "tscompression.h"
#include "tsdbMain.h"

#define TSDB_GET_COMPCOL_LEN(nCols) (sizeof(SCompData) + sizeof(SCompCol) * (nCols) + sizeof(TSCKSUM))

static bool  tsdbShouldCreateNewLast(SRWHelper *pHelper);
static int   tsdbWriteBlockToFile(SRWHelper *pHelper, SFile *pFile, SDataCols *pDataCols, int rowsToWrite,
                                  SCompBlock *pCompBlock, bool isLast, bool isSuperBlock);
static int   compareKeyBlock(const void *arg1, const void *arg2);
static int   tsdbMergeDataWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols);
static int   compTSKEY(const void *key1, const void *key2);
static int   tsdbAdjustInfoSizeIfNeeded(SRWHelper *pHelper, size_t esize);
static int   tsdbInsertSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx);
static int   tsdbAddSubBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx, int rowsAdded);
static int   tsdbUpdateSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx);
static int   tsdbGetRowsInRange(SDataCols *pDataCols, TSKEY minKey, TSKEY maxKey);
static void  tsdbResetHelperFileImpl(SRWHelper *pHelper);
static int   tsdbInitHelperFile(SRWHelper *pHelper);
static void  tsdbDestroyHelperFile(SRWHelper *pHelper);
static void  tsdbResetHelperTableImpl(SRWHelper *pHelper);
static void  tsdbResetHelperTable(SRWHelper *pHelper);
static void  tsdbInitHelperTable(SRWHelper *pHelper);
static void  tsdbDestroyHelperTable(SRWHelper *pHelper);
static void  tsdbResetHelperBlockImpl(SRWHelper *pHelper);
static void  tsdbResetHelperBlock(SRWHelper *pHelper);
static int   tsdbInitHelperBlock(SRWHelper *pHelper);
static int   tsdbInitHelper(SRWHelper *pHelper, STsdbRepo *pRepo, tsdb_rw_helper_t type);
static int   tsdbCheckAndDecodeColumnData(SDataCol *pDataCol, char *content, int32_t len, int8_t comp, int numOfRows,
                                          int maxPoints, char *buffer, int bufferSize);
static int tsdbLoadBlockDataColsImpl(SRWHelper *pHelper, SCompBlock *pCompBlock, SDataCols *pDataCols, int16_t *colIds,
                                     int numOfColIds);
static int   tsdbLoadBlockDataImpl(SRWHelper *pHelper, SCompBlock *pCompBlock, SDataCols *pDataCols);
static int   tsdbEncodeSCompIdx(void **buf, SCompIdx *pIdx);
static void *tsdbDecodeSCompIdx(void *buf, SCompIdx *pIdx);
static void  tsdbDestroyHelperBlock(SRWHelper *pHelper);
static int   tsdbLoadColData(SRWHelper *pHelper, SFile *pFile, SCompBlock *pCompBlock, SCompCol *pCompCol,
                             SDataCol *pDataCol);

// ---------------------- INTERNAL FUNCTIONS ----------------------
int tsdbInitReadHelper(SRWHelper *pHelper, STsdbRepo *pRepo) {
  return tsdbInitHelper(pHelper, pRepo, TSDB_READ_HELPER);
}

int tsdbInitWriteHelper(SRWHelper *pHelper, STsdbRepo *pRepo) {
  return tsdbInitHelper(pHelper, pRepo, TSDB_WRITE_HELPER);
}

void tsdbDestroyHelper(SRWHelper *pHelper) {
  if (pHelper) {
    tzfree(pHelper->pBuffer);
    tzfree(pHelper->compBuffer);
    tsdbDestroyHelperFile(pHelper);
    tsdbDestroyHelperTable(pHelper);
    tsdbDestroyHelperBlock(pHelper);
    memset((void *)pHelper, 0, sizeof(*pHelper));
  }
}

void tsdbResetHelper(SRWHelper *pHelper) {
  if (pHelper) {
    // Reset the block part
    tsdbResetHelperBlockImpl(pHelper);

    // Reset the table part
    tsdbResetHelperTableImpl(pHelper);

    // Reset the file part
    tsdbCloseHelperFile(pHelper, false);
    tsdbResetHelperFileImpl(pHelper);

    pHelper->state = TSDB_HELPER_CLEAR_STATE;
  }
}

int tsdbSetAndOpenHelperFile(SRWHelper *pHelper, SFileGroup *pGroup) {
  ASSERT(pHelper != NULL && pGroup != NULL);

  // Clear the helper object
  tsdbResetHelper(pHelper);

  ASSERT(pHelper->state == TSDB_HELPER_CLEAR_STATE);

  // Set the files
  pHelper->files.fid = pGroup->fileId;
  pHelper->files.headF = pGroup->files[TSDB_FILE_TYPE_HEAD];
  pHelper->files.dataF = pGroup->files[TSDB_FILE_TYPE_DATA];
  pHelper->files.lastF = pGroup->files[TSDB_FILE_TYPE_LAST];
  if (helperType(pHelper) == TSDB_WRITE_HELPER) {
    tsdbGetDataFileName(pHelper->pRepo, pGroup->fileId, TSDB_FILE_TYPE_NHEAD, pHelper->files.nHeadF.fname);
    tsdbGetDataFileName(pHelper->pRepo, pGroup->fileId, TSDB_FILE_TYPE_NLAST, pHelper->files.nLastF.fname);
  }

  // Open the files
  if (tsdbOpenFile(&(pHelper->files.headF), O_RDONLY) < 0) goto _err;
  if (helperType(pHelper) == TSDB_WRITE_HELPER) {
    if (tsdbOpenFile(&(pHelper->files.dataF), O_RDWR) < 0) goto _err;
    if (tsdbOpenFile(&(pHelper->files.lastF), O_RDWR) < 0) goto _err;

    // Create and open .h
    if (tsdbOpenFile(&(pHelper->files.nHeadF), O_WRONLY | O_CREAT) < 0) return -1;
    // size_t tsize = TSDB_FILE_HEAD_SIZE + sizeof(SCompIdx) * pCfg->maxTables + sizeof(TSCKSUM);
    if (tsendfile(pHelper->files.nHeadF.fd, pHelper->files.headF.fd, NULL, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
      tsdbError("vgId:%d failed to sendfile %d bytes from file %s to %s since %s", REPO_ID(pHelper->pRepo),
                TSDB_FILE_HEAD_SIZE, pHelper->files.headF.fname, pHelper->files.nHeadF.fname, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    // Create and open .l file if should
    if (tsdbShouldCreateNewLast(pHelper)) {
      if (tsdbOpenFile(&(pHelper->files.nLastF), O_WRONLY | O_CREAT) < 0) goto _err;
      if (tsendfile(pHelper->files.nLastF.fd, pHelper->files.lastF.fd, NULL, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
        tsdbError("vgId:%d failed to sendfile %d bytes from file %s to %s since %s", REPO_ID(pHelper->pRepo),
                  TSDB_FILE_HEAD_SIZE, pHelper->files.lastF.fname, pHelper->files.nLastF.fname, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        goto _err;
      }
    }
  } else {
    if (tsdbOpenFile(&(pHelper->files.dataF), O_RDONLY) < 0) goto _err;
    if (tsdbOpenFile(&(pHelper->files.lastF), O_RDONLY) < 0) goto _err;
  }

  helperSetState(pHelper, TSDB_HELPER_FILE_SET_AND_OPEN);

  return tsdbLoadCompIdx(pHelper, NULL);

_err:
  return -1;
}

int tsdbCloseHelperFile(SRWHelper *pHelper, bool hasError) {
  if (pHelper->files.headF.fd > 0) {
    close(pHelper->files.headF.fd);
    pHelper->files.headF.fd = -1;
  }
  if (pHelper->files.dataF.fd > 0) {
    if (helperType(pHelper) == TSDB_WRITE_HELPER) {
      tsdbUpdateFileHeader(&(pHelper->files.dataF), 0);
      fsync(pHelper->files.dataF.fd);
    }
    close(pHelper->files.dataF.fd);
    pHelper->files.dataF.fd = -1;
  }
  if (pHelper->files.lastF.fd > 0) {
    if (helperType(pHelper) == TSDB_WRITE_HELPER) {
      fsync(pHelper->files.lastF.fd);
    }
    close(pHelper->files.lastF.fd);
    pHelper->files.lastF.fd = -1;
  }
  if (helperType(pHelper) == TSDB_WRITE_HELPER) {
    if (pHelper->files.nHeadF.fd > 0) {
      if (!hasError) tsdbUpdateFileHeader(&(pHelper->files.nHeadF), 0);
      fsync(pHelper->files.nHeadF.fd);
      close(pHelper->files.nHeadF.fd);
      pHelper->files.nHeadF.fd = -1;
      if (hasError) {
        (void)remove(pHelper->files.nHeadF.fname);
      } else {
        if (rename(pHelper->files.nHeadF.fname, pHelper->files.headF.fname) < 0) {
          tsdbError("failed to rename file from %s to %s since %s", pHelper->files.nHeadF.fname,
                    pHelper->files.headF.fname, strerror(errno));
          terrno = TAOS_SYSTEM_ERROR(errno);
          return -1;
        }
        pHelper->files.headF.info = pHelper->files.nHeadF.info;
      }
    }

    if (pHelper->files.nLastF.fd > 0) {
      if (!hasError) tsdbUpdateFileHeader(&(pHelper->files.nLastF), 0);
      fsync(pHelper->files.nLastF.fd);
      close(pHelper->files.nLastF.fd);
      pHelper->files.nLastF.fd = -1;
      if (hasError) {
        (void)remove(pHelper->files.nLastF.fname);
      } else {
        if (rename(pHelper->files.nLastF.fname, pHelper->files.lastF.fname) < 0) {
          tsdbError("failed to rename file from %s to %s since %s", pHelper->files.nLastF.fname,
                    pHelper->files.lastF.fname, strerror(errno));
          terrno = TAOS_SYSTEM_ERROR(errno);
          return -1;
        }
        pHelper->files.lastF.info = pHelper->files.nLastF.info;
      }
    }
  }
  return 0;
}

void tsdbSetHelperTable(SRWHelper *pHelper, STable *pTable, STsdbRepo *pRepo) {
  ASSERT(helperHasState(pHelper, TSDB_HELPER_FILE_SET_AND_OPEN | TSDB_HELPER_IDX_LOAD));

  // Clear members and state used by previous table
  tsdbResetHelperTable(pHelper);
  ASSERT(helperHasState(pHelper, (TSDB_HELPER_FILE_SET_AND_OPEN | TSDB_HELPER_IDX_LOAD)));

  pHelper->tableInfo.tid = pTable->tableId.tid;
  pHelper->tableInfo.uid = pTable->tableId.uid;
  STSchema *pSchema = tsdbGetTableSchema(pTable);
  pHelper->tableInfo.sversion = schemaVersion(pSchema);

  tdInitDataCols(pHelper->pDataCols[0], pSchema);
  tdInitDataCols(pHelper->pDataCols[1], pSchema);

  SCompIdx *pIdx = pHelper->pCompIdx + pTable->tableId.tid;
  if (pIdx->offset > 0 && pIdx->hasLast) {
    pHelper->hasOldLastBlock = true;
  }

  helperSetState(pHelper, TSDB_HELPER_TABLE_SET);
  ASSERT(pHelper->state == ((TSDB_HELPER_TABLE_SET << 1) - 1));
}

/**
 * Write part of of points from pDataCols to file
 *
 * @return: number of points written to file successfully
 *          -1 for failure
 */
int tsdbWriteDataBlock(SRWHelper *pHelper, SDataCols *pDataCols) {
  ASSERT(helperType(pHelper) == TSDB_WRITE_HELPER);
  ASSERT(pDataCols->numOfRows > 0);

  SCompBlock compBlock;
  int        rowsToWrite = 0;
  TSKEY      keyFirst = dataColsKeyFirst(pDataCols);

  STsdbCfg *pCfg = &pHelper->pRepo->config;

  ASSERT(helperHasState(pHelper, TSDB_HELPER_IDX_LOAD));
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;  // for change purpose

  // Load the SCompInfo part if neccessary
  ASSERT(helperHasState(pHelper, TSDB_HELPER_TABLE_SET));
  if (tsdbLoadCompInfo(pHelper, NULL) < 0) goto _err;

  if (pIdx->offset == 0 || (!pIdx->hasLast && keyFirst > pIdx->maxKey)) {  // Just append as a super block
    ASSERT(pHelper->hasOldLastBlock == false);
    rowsToWrite = pDataCols->numOfRows;
    SFile *pWFile = NULL;
    bool   isLast = false;

    if (rowsToWrite >= pCfg->minRowsPerFileBlock) {
      pWFile = &(pHelper->files.dataF);
    } else {
      isLast = true;
      pWFile = (pHelper->files.nLastF.fd > 0) ? &(pHelper->files.nLastF) : &(pHelper->files.lastF);
    }

    if (tsdbWriteBlockToFile(pHelper, pWFile, pDataCols, rowsToWrite, &compBlock, isLast, true) < 0) goto _err;

    if (tsdbInsertSuperBlock(pHelper, &compBlock, pIdx->numOfBlocks) < 0) goto _err;
  } else {  // (Has old data) AND ((has last block) OR (key overlap)), need to merge the block
    SCompBlock *pCompBlock = taosbsearch((void *)(&keyFirst), (void *)(pHelper->pCompInfo->blocks), pIdx->numOfBlocks,
                                         sizeof(SCompBlock), compareKeyBlock, TD_GE);

    int blkIdx = (pCompBlock == NULL) ? (pIdx->numOfBlocks - 1) : (pCompBlock - pHelper->pCompInfo->blocks);

    if (pCompBlock == NULL) {  // No key overlap, must has last block, just merge with the last block
      ASSERT(pIdx->hasLast && pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].last);
      rowsToWrite = tsdbMergeDataWithBlock(pHelper, blkIdx, pDataCols);
      if (rowsToWrite < 0) goto _err;
    } else {  // Has key overlap

      if (compareKeyBlock((void *)(&keyFirst), (void *)pCompBlock) == 0) {
        // Key overlap with the block, must merge with the block

        rowsToWrite = tsdbMergeDataWithBlock(pHelper, blkIdx, pDataCols);
        if (rowsToWrite < 0) goto _err;
      } else {  // Save as a super block in the middle
        rowsToWrite = tsdbGetRowsInRange(pDataCols, 0, pCompBlock->keyFirst - 1);
        ASSERT(rowsToWrite > 0);
        if (tsdbWriteBlockToFile(pHelper, &(pHelper->files.dataF), pDataCols, rowsToWrite, &compBlock, false, true) < 0)
          goto _err;
        if (tsdbInsertSuperBlock(pHelper, &compBlock, blkIdx) < 0) goto _err;
      }
    }
  }

  return rowsToWrite;

_err:
  return -1;
}

int tsdbMoveLastBlockIfNeccessary(SRWHelper *pHelper) {
  STsdbCfg *pCfg = &pHelper->pRepo->config;

  ASSERT(helperType(pHelper) == TSDB_WRITE_HELPER);
  SCompIdx * pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  SCompBlock compBlock;
  if ((pHelper->files.nLastF.fd > 0) && (pHelper->hasOldLastBlock)) {
    if (tsdbLoadCompInfo(pHelper, NULL) < 0) return -1;

    SCompBlock *pCompBlock = pHelper->pCompInfo->blocks + pIdx->numOfBlocks - 1;
    ASSERT(pCompBlock->last);

    if (pCompBlock->numOfSubBlocks > 1) {
      if (tsdbLoadBlockData(pHelper, blockAtIdx(pHelper, pIdx->numOfBlocks - 1)) < 0) return -1;
      ASSERT(pHelper->pDataCols[0]->numOfRows > 0 && pHelper->pDataCols[0]->numOfRows < pCfg->minRowsPerFileBlock);
      if (tsdbWriteBlockToFile(pHelper, &(pHelper->files.nLastF), pHelper->pDataCols[0],
                               pHelper->pDataCols[0]->numOfRows, &compBlock, true, true) < 0)
        return -1;

      if (tsdbUpdateSuperBlock(pHelper, &compBlock, pIdx->numOfBlocks - 1) < 0) return -1;

    } else {
      if (lseek(pHelper->files.lastF.fd, pCompBlock->offset, SEEK_SET) < 0) return -1;
      pCompBlock->offset = lseek(pHelper->files.nLastF.fd, 0, SEEK_END);
      if (pCompBlock->offset < 0) return -1;

      if (tsendfile(pHelper->files.nLastF.fd, pHelper->files.lastF.fd, NULL, pCompBlock->len) < pCompBlock->len)
        return -1;
    }

    pHelper->hasOldLastBlock = false;
  }

  return 0;
}

int tsdbWriteCompInfo(SRWHelper *pHelper) {
  off_t offset = 0;
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  if (!helperHasState(pHelper, TSDB_HELPER_INFO_LOAD)) {
    if (pIdx->offset > 0) {
      offset = lseek(pHelper->files.nHeadF.fd, 0, SEEK_END);
      if (offset < 0) {
        tsdbError("vgId:%d failed to lseed file %s since %s", REPO_ID(pHelper->pRepo), pHelper->files.nHeadF.fname,
                  strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }

      pIdx->offset = offset;
      ASSERT(pIdx->offset >= TSDB_FILE_HEAD_SIZE);

      if (tsendfile(pHelper->files.nHeadF.fd, pHelper->files.headF.fd, NULL, pIdx->len) < pIdx->len) {
        tsdbError("vgId:%d failed to send %d bytes from file %s to %s since %s", REPO_ID(pHelper->pRepo), pIdx->len,
                  pHelper->files.headF.fname, pHelper->files.nHeadF.fname, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
    }
  } else {
    pHelper->pCompInfo->delimiter = TSDB_FILE_DELIMITER;
    pHelper->pCompInfo->uid = pHelper->tableInfo.uid;
    pHelper->pCompInfo->checksum = 0;
    ASSERT((pIdx->len - sizeof(SCompInfo) - sizeof(TSCKSUM)) % sizeof(SCompBlock) == 0);
    taosCalcChecksumAppend(0, (uint8_t *)pHelper->pCompInfo, pIdx->len);
    offset = lseek(pHelper->files.nHeadF.fd, 0, SEEK_END);
    if (offset < 0) {
      tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pHelper->files.nHeadF.fname,
                strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
    pIdx->offset = offset;
    pIdx->uid = pHelper->tableInfo.uid;
    ASSERT(pIdx->offset >= TSDB_FILE_HEAD_SIZE);

    if (twrite(pHelper->files.nHeadF.fd, (void *)(pHelper->pCompInfo), pIdx->len) < pIdx->len) {
      tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(pHelper->pRepo), pIdx->len,
                pHelper->files.nHeadF.fname, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  return 0;
}

int tsdbWriteCompIdx(SRWHelper *pHelper) {
  STsdbCfg *pCfg = &pHelper->pRepo->config;

  ASSERT(helperType(pHelper) == TSDB_WRITE_HELPER);
  off_t offset = lseek(pHelper->files.nHeadF.fd, 0, SEEK_END);
  if (offset < 0) return -1;

  SFile *pFile = &(pHelper->files.nHeadF);
  pFile->info.offset = offset;

  void *buf = pHelper->pBuffer;
  for (uint32_t i = 0; i < pCfg->maxTables; i++) {
    SCompIdx *pCompIdx = pHelper->pCompIdx + i;
    if (pCompIdx->offset > 0) {
      int drift = POINTER_DISTANCE(buf, pHelper->pBuffer);
      if (tsizeof(pHelper->pBuffer) - drift < 128) {
        pHelper->pBuffer = trealloc(pHelper->pBuffer, tsizeof(pHelper->pBuffer) * 2);
      }
      buf = POINTER_SHIFT(pHelper->pBuffer, drift);
      taosEncodeVariantU32(&buf, i);
      tsdbEncodeSCompIdx(&buf, pCompIdx);
    }
  }

  int tsize = (char *)buf - (char *)pHelper->pBuffer + sizeof(TSCKSUM);
  taosCalcChecksumAppend(0, (uint8_t *)pHelper->pBuffer, tsize);

  if (twrite(pHelper->files.nHeadF.fd, (void *)pHelper->pBuffer, tsize) < tsize) return -1;
  pFile->info.len = tsize;
  return 0;
}

int tsdbLoadCompIdx(SRWHelper *pHelper, void *target) {
  STsdbCfg *pCfg = &(pHelper->pRepo->config);

  ASSERT(pHelper->state == TSDB_HELPER_FILE_SET_AND_OPEN);

  if (!helperHasState(pHelper, TSDB_HELPER_IDX_LOAD)) {
    // If not load from file, just load it in object
    SFile *pFile = &(pHelper->files.headF);
    int    fd = pFile->fd;

    memset(pHelper->pCompIdx, 0, tsizeof(pHelper->pCompIdx));
    if (pFile->info.offset > 0) {
      ASSERT(pFile->info.offset > TSDB_FILE_HEAD_SIZE);

      if (lseek(fd, pFile->info.offset, SEEK_SET) < 0) {
        tsdbError("vgId:%d failed to lseek file %s to %u since %s", REPO_ID(pHelper->pRepo), pFile->fname,
                  pFile->info.offset, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
      if ((pHelper->pBuffer = trealloc(pHelper->pBuffer, pFile->info.len)) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
      if (tread(fd, (void *)(pHelper->pBuffer), pFile->info.len) < pFile->info.len) {
        tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pHelper->pRepo), pFile->info.len,
                  pFile->fname, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
      if (!taosCheckChecksumWhole((uint8_t *)(pHelper->pBuffer), pFile->info.len)) {
        tsdbError("vgId:%d file %s SCompIdx part is corrupted. offset %u len %u", REPO_ID(pHelper->pRepo), pFile->fname,
                  pFile->info.offset, pFile->info.len);
        terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
        return -1;
      }

      // Decode it
      void *ptr = pHelper->pBuffer;
      while (POINTER_DISTANCE(ptr, pHelper->pBuffer) < (pFile->info.len - sizeof(TSCKSUM))) {
        uint32_t tid = 0;
        if ((ptr = taosDecodeVariantU32(ptr, &tid)) == NULL) return -1;
        ASSERT(tid > 0 && tid < pCfg->maxTables);

        if ((ptr = tsdbDecodeSCompIdx(ptr, pHelper->pCompIdx + tid)) == NULL) return -1;

        ASSERT(POINTER_DISTANCE(ptr, pHelper->pBuffer) <= pFile->info.len - sizeof(TSCKSUM));
      }

      if (lseek(fd, TSDB_FILE_HEAD_SIZE, SEEK_SET) < 0) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
    }
  }
  helperSetState(pHelper, TSDB_HELPER_IDX_LOAD);

  // Copy the memory for outside usage
  if (target) memcpy(target, pHelper->pCompIdx, tsizeof(pHelper->pCompIdx));

  return 0;
}

int tsdbLoadCompInfo(SRWHelper *pHelper, void *target) {
  ASSERT(helperHasState(pHelper, TSDB_HELPER_TABLE_SET));

  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;

  int fd = pHelper->files.headF.fd;

  if (!helperHasState(pHelper, TSDB_HELPER_INFO_LOAD)) {
    if (pIdx->offset > 0) {
      if (lseek(fd, pIdx->offset, SEEK_SET) < 0) return -1;

      pHelper->pCompInfo = trealloc((void *)pHelper->pCompInfo, pIdx->len);
      if (tread(fd, (void *)(pHelper->pCompInfo), pIdx->len) < pIdx->len) return -1;
      if (!taosCheckChecksumWhole((uint8_t *)pHelper->pCompInfo, pIdx->len)) return -1;
    }

    helperSetState(pHelper, TSDB_HELPER_INFO_LOAD);
  }

  if (target) memcpy(target, (void *)(pHelper->pCompInfo), pIdx->len);

  return 0;
}

int tsdbLoadCompData(SRWHelper *pHelper, SCompBlock *pCompBlock, void *target) {
  ASSERT(pCompBlock->numOfSubBlocks <= 1);
  SFile *pFile = (pCompBlock->last) ? &(pHelper->files.lastF) : &(pHelper->files.dataF);

  if (lseek(pFile->fd, pCompBlock->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  size_t tsize = TSDB_GET_COMPCOL_LEN(pCompBlock->numOfCols);
  pHelper->pCompData = trealloc((void *)pHelper->pCompData, tsize);
  if (pHelper->pCompData == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tread(pFile->fd, (void *)pHelper->pCompData, tsize) < tsize) {
    tsdbError("vgId:%d failed to read %zu bytes from file %s since %s", REPO_ID(pHelper->pRepo), tsize, pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)pHelper->pCompData, tsize)) {
    tsdbError("vgId:%d file %s is broken, offset %" PRId64 " size %zu", REPO_ID(pHelper->pRepo), pFile->fname,
              (int64_t)pCompBlock->offset, tsize);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  ASSERT(pCompBlock->numOfCols == pHelper->pCompData->numOfCols);

  if (target) memcpy(target, pHelper->pCompData, tsize);

  return 0;
}

void tsdbGetDataStatis(SRWHelper *pHelper, SDataStatis *pStatis, int numOfCols) {
  SCompData *pCompData = pHelper->pCompData;

  for (int i = 0, j = 0; i < numOfCols;) {
    if (j >= pCompData->numOfCols) {
      pStatis[i].numOfNull = -1;
      i++;
      continue;
    }

    if (pStatis[i].colId == pCompData->cols[j].colId) {
      pStatis[i].sum = pCompData->cols[j].sum;
      pStatis[i].max = pCompData->cols[j].max;
      pStatis[i].min = pCompData->cols[j].min;
      pStatis[i].maxIndex = pCompData->cols[j].maxIndex;
      pStatis[i].minIndex = pCompData->cols[j].minIndex;
      pStatis[i].numOfNull = pCompData->cols[j].numOfNull;
      i++;
      j++;
    } else if (pStatis[i].colId < pCompData->cols[j].colId) {
      pStatis[i].numOfNull = -1;
      i++;
    } else {
      j++;
    }
  }
}

int tsdbLoadBlockDataCols(SRWHelper *pHelper, SCompBlock *pCompBlock, int16_t *colIds, int numOfColIds) {
  ASSERT(pCompBlock->numOfSubBlocks >= 1);  // Must be super block

  int numOfSubBlocks = pCompBlock->numOfSubBlocks;
  if (numOfSubBlocks > 1) pCompBlock = (SCompBlock *)POINTER_SHIFT(pHelper->pCompInfo, pCompBlock->offset);

  tdResetDataCols(pHelper->pDataCols[0]);
  if (tsdbLoadBlockDataColsImpl(pHelper, pCompBlock, pHelper->pDataCols[0], colIds, numOfColIds) < 0) goto _err;
  for (int i = 1; i < numOfSubBlocks; i++) {
    tdResetDataCols(pHelper->pDataCols[1]);
    pCompBlock++;
    if (tsdbLoadBlockDataColsImpl(pHelper, pCompBlock, pHelper->pDataCols[1], colIds, numOfColIds) < 0) goto _err;
    if (tdMergeDataCols(pHelper->pDataCols[0], pHelper->pDataCols[1], pHelper->pDataCols[1]->numOfRows) < 0) goto _err;
  }

  return 0;

_err:
  return -1;
}

int tsdbLoadBlockData(SRWHelper *pHelper, SCompBlock *pCompBlock) {

  int numOfSubBlock = pCompBlock->numOfSubBlocks;
  if (numOfSubBlock > 1) pCompBlock = (SCompBlock *)POINTER_SHIFT(pHelper->pCompInfo, pCompBlock->offset);

  tdResetDataCols(pHelper->pDataCols[0]);
  if (tsdbLoadBlockDataImpl(pHelper, pCompBlock, pHelper->pDataCols[0]) < 0) goto _err;
  for (int i = 1; i < numOfSubBlock; i++) {
    tdResetDataCols(pHelper->pDataCols[1]);
    pCompBlock++;
    if (tsdbLoadBlockDataImpl(pHelper, pCompBlock, pHelper->pDataCols[1]) < 0) goto _err;
    if (tdMergeDataCols(pHelper->pDataCols[0], pHelper->pDataCols[1], pHelper->pDataCols[1]->numOfRows) < 0) goto _err;
  }

  return 0;

_err:
  return -1;
}

// ---------------------- INTERNAL FUNCTIONS ----------------------
static bool tsdbShouldCreateNewLast(SRWHelper *pHelper) {
  ASSERT(pHelper->files.lastF.fd > 0);
  struct stat st;
  if (fstat(pHelper->files.lastF.fd, &st) < 0) return true;
  if (st.st_size > 32 * 1024 + TSDB_FILE_HEAD_SIZE) return true;
  return false;
}

static int tsdbWriteBlockToFile(SRWHelper *pHelper, SFile *pFile, SDataCols *pDataCols, int rowsToWrite,
                                SCompBlock *pCompBlock, bool isLast, bool isSuperBlock) {
  STsdbCfg * pCfg = &(pHelper->pRepo->config);
  SCompData *pCompData = (SCompData *)(pHelper->pBuffer);
  int64_t    offset = 0;

  ASSERT(rowsToWrite > 0 && rowsToWrite <= pDataCols->numOfRows && rowsToWrite <= pCfg->maxRowsPerFileBlock);
  ASSERT(isLast ? rowsToWrite < pCfg->minRowsPerFileBlock : true);

  offset = lseek(pFile->fd, 0, SEEK_END);
  if (offset < 0) {
    tsdbError("vgId:%d failed to write block to file %s since %s", REPO_ID(pHelper->pRepo), pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  int nColsNotAllNull = 0;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    SDataCol *pDataCol = pDataCols->cols + ncol;
    SCompCol *pCompCol = pCompData->cols + nColsNotAllNull;

    if (isNEleNull(pDataCol, rowsToWrite)) {  // all data to commit are NULL, just ignore it
      continue;
    }

    memset(pCompCol, 0, sizeof(*pCompCol));

    pCompCol->colId = pDataCol->colId;
    pCompCol->type = pDataCol->type;
    if (tDataTypeDesc[pDataCol->type].getStatisFunc && ncol != 0) {
      (*tDataTypeDesc[pDataCol->type].getStatisFunc)(
          (TSKEY *)(pDataCols->cols[0].pData), pDataCol->pData, rowsToWrite, &(pCompCol->min), &(pCompCol->max),
          &(pCompCol->sum), &(pCompCol->minIndex), &(pCompCol->maxIndex), &(pCompCol->numOfNull));
    }
    nColsNotAllNull++;
  }

  ASSERT(nColsNotAllNull > 0 && nColsNotAllNull <= pDataCols->numOfCols);

  // Compress the data if neccessary
  int     tcol = 0;
  int32_t toffset = 0;
  int32_t tsize = TSDB_GET_COMPCOL_LEN(nColsNotAllNull);
  int32_t lsize = tsize;
  for (int ncol = 0; ncol < pDataCols->numOfCols; ncol++) {
    if (tcol >= nColsNotAllNull) break;

    SDataCol *pDataCol = pDataCols->cols + ncol;
    SCompCol *pCompCol = pCompData->cols + tcol;

    if (pDataCol->colId != pCompCol->colId) continue;
    void *tptr = (void *)((char *)pCompData + lsize);

    pCompCol->offset = toffset;

    int32_t tlen = dataColGetNEleLen(pDataCol, rowsToWrite);

    if (pCfg->compression) {
      if (pCfg->compression == TWO_STAGE_COMP) {
        pHelper->compBuffer = trealloc(pHelper->compBuffer, tlen + COMP_OVERFLOW_BYTES);
        if (pHelper->compBuffer == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          goto _err;
        }
      }

      pCompCol->len = (*(tDataTypeDesc[pDataCol->type].compFunc))((char *)pDataCol->pData, tlen, rowsToWrite, tptr,
                                                                  tsizeof(pHelper->pBuffer) - lsize, pCfg->compression,
                                                                  pHelper->compBuffer, tsizeof(pHelper->compBuffer));
    } else {
      pCompCol->len = tlen;
      memcpy(tptr, pDataCol->pData, pCompCol->len);
    }

    // Add checksum
    pCompCol->len += sizeof(TSCKSUM);
    taosCalcChecksumAppend(0, (uint8_t *)tptr, pCompCol->len);

    toffset += pCompCol->len;
    lsize += pCompCol->len;
    tcol++;
  }

  pCompData->delimiter = TSDB_FILE_DELIMITER;
  pCompData->uid = pHelper->tableInfo.uid;
  pCompData->numOfCols = nColsNotAllNull;

  taosCalcChecksumAppend(0, (uint8_t *)pCompData, tsize);

  // Write the whole block to file
  if (twrite(pFile->fd, (void *)pCompData, lsize) < lsize) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", REPO_ID(helperRepo(pHelper)), lsize, pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // Update pCompBlock membership vairables
  pCompBlock->last = isLast;
  pCompBlock->offset = offset;
  pCompBlock->algorithm = pCfg->compression;
  pCompBlock->numOfRows = rowsToWrite;
  pCompBlock->sversion = pHelper->tableInfo.sversion;
  pCompBlock->len = (int32_t)lsize;
  pCompBlock->numOfSubBlocks = isSuperBlock ? 1 : 0;
  pCompBlock->numOfCols = nColsNotAllNull;
  pCompBlock->keyFirst = dataColsKeyFirst(pDataCols);
  pCompBlock->keyLast = dataColsKeyAt(pDataCols, rowsToWrite - 1);

  tsdbDebug("vgId:%d tid:%d a block of data is written to file %s, offset %" PRId64
            " numOfRows %d len %d numOfCols %" PRId16 " keyFirst %" PRId64 " keyLast %" PRId64,
            REPO_ID(helperRepo(pHelper)), pHelper->tableInfo.tid, pFile->fname, (int64_t)(pCompBlock->offset),
            (int)(pCompBlock->numOfRows), pCompBlock->len, pCompBlock->numOfCols, pCompBlock->keyFirst,
            pCompBlock->keyLast);

  return 0;

_err:
  return -1;
}

static int compareKeyBlock(const void *arg1, const void *arg2) {
  TSKEY       key = *(TSKEY *)arg1;
  SCompBlock *pBlock = (SCompBlock *)arg2;

  if (key < pBlock->keyFirst) {
    return -1;
  } else if (key > pBlock->keyLast) {
    return 1;
  }

  return 0;
}

static int tsdbMergeDataWithBlock(SRWHelper *pHelper, int blkIdx, SDataCols *pDataCols) {
  // TODO: set pHelper->hasOldBlock
  int        rowsWritten = 0;
  SCompBlock compBlock = {0};
  STsdbCfg * pCfg = &pHelper->pRepo->config;

  ASSERT(pDataCols->numOfRows > 0);
  TSKEY keyFirst = dataColsKeyFirst(pDataCols);

  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  ASSERT(blkIdx < pIdx->numOfBlocks);

  // SCompBlock *pCompBlock = pHelper->pCompInfo->blocks + blkIdx;
  ASSERT(blockAtIdx(pHelper, blkIdx)->numOfSubBlocks >= 1);
  ASSERT(keyFirst >= blockAtIdx(pHelper, blkIdx)->keyFirst);
  // ASSERT(compareKeyBlock((void *)&keyFirst, (void *)pCompBlock) == 0);

  if (keyFirst > blockAtIdx(pHelper, blkIdx)->keyLast) {  // Merge with the last block by append
    ASSERT(blockAtIdx(pHelper, blkIdx)->numOfRows < pCfg->minRowsPerFileBlock && blkIdx == pIdx->numOfBlocks - 1);
    int defaultRowsToWrite = pCfg->maxRowsPerFileBlock * 4 / 5;  // TODO: make a interface

    rowsWritten = MIN((defaultRowsToWrite - blockAtIdx(pHelper, blkIdx)->numOfRows), pDataCols->numOfRows);
    if ((blockAtIdx(pHelper, blkIdx)->numOfSubBlocks < TSDB_MAX_SUBBLOCKS) &&
        (blockAtIdx(pHelper, blkIdx)->numOfRows + rowsWritten < pCfg->minRowsPerFileBlock) &&
        (pHelper->files.nLastF.fd) < 0) {
      if (tsdbWriteBlockToFile(pHelper, &(pHelper->files.lastF), pDataCols, rowsWritten, &compBlock, true, false) < 0)
        goto _err;
      if (tsdbAddSubBlock(pHelper, &compBlock, blkIdx, rowsWritten) < 0) goto _err;
    } else {
      // Load
      if (tsdbLoadBlockData(pHelper, blockAtIdx(pHelper, blkIdx)) < 0) goto _err;
      ASSERT(pHelper->pDataCols[0]->numOfRows <= blockAtIdx(pHelper, blkIdx)->numOfRows);
      // Merge
      if (tdMergeDataCols(pHelper->pDataCols[0], pDataCols, rowsWritten) < 0) goto _err;
      // Write
      SFile *pWFile = NULL;
      bool   isLast = false;
      if (pHelper->pDataCols[0]->numOfRows >= pCfg->minRowsPerFileBlock) {
        pWFile = &(pHelper->files.dataF);
      } else {
        isLast = true;
        pWFile = (pHelper->files.nLastF.fd > 0) ? &(pHelper->files.nLastF) : &(pHelper->files.lastF);
      }
      if (tsdbWriteBlockToFile(pHelper, pWFile, pHelper->pDataCols[0], pHelper->pDataCols[0]->numOfRows, &compBlock,
                               isLast, true) < 0)
        goto _err;
      if (tsdbUpdateSuperBlock(pHelper, &compBlock, blkIdx) < 0) goto _err;
    }

    ASSERT(pHelper->hasOldLastBlock);
    pHelper->hasOldLastBlock = false;
  } else {
    // Key must overlap with the block
    ASSERT(keyFirst <= blockAtIdx(pHelper, blkIdx)->keyLast);

    TSKEY keyLimit = (blkIdx == pIdx->numOfBlocks - 1) ? INT64_MAX : blockAtIdx(pHelper, blkIdx + 1)->keyFirst - 1;

    // rows1: number of rows must merge in this block
    int rows1 =
        tsdbGetRowsInRange(pDataCols, blockAtIdx(pHelper, blkIdx)->keyFirst, blockAtIdx(pHelper, blkIdx)->keyLast);
    // rows2: max number of rows the block can have more
    int rows2 = pCfg->maxRowsPerFileBlock - blockAtIdx(pHelper, blkIdx)->numOfRows;
    // rows3: number of rows between this block and the next block
    int rows3 = tsdbGetRowsInRange(pDataCols, blockAtIdx(pHelper, blkIdx)->keyFirst, keyLimit);

    ASSERT(rows3 >= rows1);

    if ((rows2 >= rows1) && (blockAtIdx(pHelper, blkIdx)->numOfSubBlocks < TSDB_MAX_SUBBLOCKS) &&
        ((!blockAtIdx(pHelper, blkIdx)->last) ||
         ((rows1 + blockAtIdx(pHelper, blkIdx)->numOfRows < pCfg->minRowsPerFileBlock) &&
          (pHelper->files.nLastF.fd < 0)))) {
      rowsWritten = rows1;
      bool   isLast = false;
      SFile *pFile = NULL;

      if (blockAtIdx(pHelper, blkIdx)->last) {
        isLast = true;
        pFile = &(pHelper->files.lastF);
      } else {
        pFile = &(pHelper->files.dataF);
      }

      if (tsdbWriteBlockToFile(pHelper, pFile, pDataCols, rows1, &compBlock, isLast, false) < 0) goto _err;
      if (tsdbAddSubBlock(pHelper, &compBlock, blkIdx, rowsWritten) < 0) goto _err;
    } else {  // Load-Merge-Write
      // Load
      if (tsdbLoadBlockData(pHelper, blockAtIdx(pHelper, blkIdx)) < 0) goto _err;
      if (blockAtIdx(pHelper, blkIdx)->last) pHelper->hasOldLastBlock = false;

      rowsWritten = rows3;

      int iter1 = 0;  // iter over pHelper->pDataCols[0]
      int iter2 = 0;  // iter over pDataCols
      int round = 0;
      // tdResetDataCols(pHelper->pDataCols[1]);
      while (true) {
        if (iter1 >= pHelper->pDataCols[0]->numOfRows && iter2 >= rows3) break;
        tdMergeTwoDataCols(pHelper->pDataCols[1], pHelper->pDataCols[0], &iter1, pHelper->pDataCols[0]->numOfRows,
                           pDataCols, &iter2, rowsWritten, pCfg->maxRowsPerFileBlock * 4 / 5);
        ASSERT(pHelper->pDataCols[1]->numOfRows > 0);
        if (tsdbWriteBlockToFile(pHelper, &(pHelper->files.dataF), pHelper->pDataCols[1],
                                 pHelper->pDataCols[1]->numOfRows, &compBlock, false, true) < 0)
          goto _err;
        if (round == 0) {
          tsdbUpdateSuperBlock(pHelper, &compBlock, blkIdx);
        } else {
          tsdbInsertSuperBlock(pHelper, &compBlock, blkIdx);
        }
        round++;
        blkIdx++;
      }
    }
  }

  return rowsWritten;

_err:
  return -1;
}

static int compTSKEY(const void *key1, const void *key2) {
  if (*(TSKEY *)key1 > *(TSKEY *)key2) {
    return 1;
  } else if (*(TSKEY *)key1 == *(TSKEY *)key2) {
    return 0;
  } else {
    return -1;
  }
}

static int tsdbAdjustInfoSizeIfNeeded(SRWHelper *pHelper, size_t esize) {
  if (tsizeof((void *)pHelper->pCompInfo) <= esize) {
    size_t tsize = esize + sizeof(SCompBlock) * 16;
    pHelper->pCompInfo = (SCompInfo *)trealloc(pHelper->pCompInfo, tsize);
    if (pHelper->pCompInfo == NULL) return -1;
  }

  return 0;
}

static int tsdbInsertSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx) {
  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;

  ASSERT(blkIdx >= 0 && blkIdx <= pIdx->numOfBlocks);
  ASSERT(pCompBlock->numOfSubBlocks == 1);

  // Adjust memory if no more room
  if (pIdx->len == 0) pIdx->len = sizeof(SCompData) + sizeof(TSCKSUM);
  if (tsdbAdjustInfoSizeIfNeeded(pHelper, pIdx->len + sizeof(SCompInfo)) < 0) goto _err;

  // Change the offset
  for (int i = 0; i < pIdx->numOfBlocks; i++) {
    SCompBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
    if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += sizeof(SCompBlock);
  }

  // Memmove if needed
  int tsize = pIdx->len - (sizeof(SCompInfo) + sizeof(SCompBlock) * blkIdx);
  if (tsize > 0) {
    ASSERT(sizeof(SCompInfo) + sizeof(SCompBlock) * (blkIdx + 1) < tsizeof(pHelper->pCompInfo));
    ASSERT(sizeof(SCompInfo) + sizeof(SCompBlock) * (blkIdx + 1) + tsize <= tsizeof(pHelper->pCompInfo));
    memmove((void *)((char *)pHelper->pCompInfo + sizeof(SCompInfo) + sizeof(SCompBlock) * (blkIdx + 1)),
            (void *)((char *)pHelper->pCompInfo + sizeof(SCompInfo) + sizeof(SCompBlock) * blkIdx), tsize);
  }
  pHelper->pCompInfo->blocks[blkIdx] = *pCompBlock;

  pIdx->numOfBlocks++;
  pIdx->len += sizeof(SCompBlock);
  ASSERT(pIdx->len <= tsizeof(pHelper->pCompInfo));
  pIdx->maxKey = pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].keyLast;
  pIdx->hasLast = pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].last;

  if (pIdx->numOfBlocks > 1) {
    ASSERT(pHelper->pCompInfo->blocks[0].keyLast < pHelper->pCompInfo->blocks[1].keyFirst);
  }

  tsdbDebug("vgId:%d tid:%d a super block is inserted at index %d", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid,
            blkIdx);

  return 0;

_err:
  return -1;
}

static int tsdbAddSubBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx, int rowsAdded) {
  ASSERT(pCompBlock->numOfSubBlocks == 0);

  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;
  ASSERT(blkIdx >= 0 && blkIdx < pIdx->numOfBlocks);

  SCompBlock *pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;
  ASSERT(pSCompBlock->numOfSubBlocks >= 1 && pSCompBlock->numOfSubBlocks < TSDB_MAX_SUBBLOCKS);

  size_t spaceNeeded =
      (pSCompBlock->numOfSubBlocks == 1) ? pIdx->len + sizeof(SCompBlock) * 2 : pIdx->len + sizeof(SCompBlock);
  if (tsdbAdjustInfoSizeIfNeeded(pHelper, spaceNeeded) < 0) goto _err;

  pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  // Add the sub-block
  if (pSCompBlock->numOfSubBlocks > 1) {
    size_t tsize = pIdx->len - (pSCompBlock->offset + pSCompBlock->len);
    if (tsize > 0) {
      memmove((void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len + sizeof(SCompBlock)),
              (void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len), tsize);

      for (int i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
        SCompBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
        if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += sizeof(SCompBlock);
      }
    }

    *(SCompBlock *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len) = *pCompBlock;

    pSCompBlock->numOfSubBlocks++;
    ASSERT(pSCompBlock->numOfSubBlocks <= TSDB_MAX_SUBBLOCKS);
    pSCompBlock->len += sizeof(SCompBlock);
    pSCompBlock->numOfRows += rowsAdded;
    pSCompBlock->keyFirst = MIN(pSCompBlock->keyFirst, pCompBlock->keyFirst);
    pSCompBlock->keyLast = MAX(pSCompBlock->keyLast, pCompBlock->keyLast);
    pIdx->len += sizeof(SCompBlock);
  } else {  // Need to create two sub-blocks
    void *ptr = NULL;
    for (int i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
      SCompBlock *pTCompBlock = pHelper->pCompInfo->blocks + i;
      if (pTCompBlock->numOfSubBlocks > 1) {
        ptr = POINTER_SHIFT(pHelper->pCompInfo, pTCompBlock->offset);
        break;
      }
    }

    if (ptr == NULL) ptr = POINTER_SHIFT(pHelper->pCompInfo, pIdx->len - sizeof(TSCKSUM));

    size_t tsize = pIdx->len - ((char *)ptr - (char *)(pHelper->pCompInfo));
    if (tsize > 0) {
      memmove(POINTER_SHIFT(ptr, sizeof(SCompBlock) * 2), ptr, tsize);
      for (int i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
        SCompBlock *pTCompBlock = pHelper->pCompInfo->blocks + i;
        if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset += (sizeof(SCompBlock) * 2);
      }
    }

    ((SCompBlock *)ptr)[0] = *pSCompBlock;
    ((SCompBlock *)ptr)[0].numOfSubBlocks = 0;

    ((SCompBlock *)ptr)[1] = *pCompBlock;

    pSCompBlock->numOfSubBlocks = 2;
    pSCompBlock->numOfRows += rowsAdded;
    pSCompBlock->offset = ((char *)ptr) - ((char *)pHelper->pCompInfo);
    pSCompBlock->len = sizeof(SCompBlock) * 2;
    pSCompBlock->keyFirst = MIN(((SCompBlock *)ptr)[0].keyFirst, ((SCompBlock *)ptr)[1].keyFirst);
    pSCompBlock->keyLast = MAX(((SCompBlock *)ptr)[0].keyLast, ((SCompBlock *)ptr)[1].keyLast);

    pIdx->len += (sizeof(SCompBlock) * 2);
  }

  pIdx->maxKey = pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].keyLast;
  pIdx->hasLast = pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].last;

  tsdbDebug("vgId:%d tid:%d a subblock is added at index %d", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid, blkIdx);

  return 0;

_err:
  return -1;
}

static int tsdbUpdateSuperBlock(SRWHelper *pHelper, SCompBlock *pCompBlock, int blkIdx) {
  ASSERT(pCompBlock->numOfSubBlocks == 1);

  SCompIdx *pIdx = pHelper->pCompIdx + pHelper->tableInfo.tid;

  ASSERT(blkIdx >= 0 && blkIdx < pIdx->numOfBlocks);

  SCompBlock *pSCompBlock = pHelper->pCompInfo->blocks + blkIdx;

  ASSERT(pSCompBlock->numOfSubBlocks >= 1);

  // Delete the sub blocks it has
  if (pSCompBlock->numOfSubBlocks > 1) {
    size_t tsize = pIdx->len - (pSCompBlock->offset + pSCompBlock->len);
    if (tsize > 0) {
      memmove((void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset),
              (void *)((char *)(pHelper->pCompInfo) + pSCompBlock->offset + pSCompBlock->len), tsize);
    }

    for (int i = blkIdx + 1; i < pIdx->numOfBlocks; i++) {
      SCompBlock *pTCompBlock = &pHelper->pCompInfo->blocks[i];
      if (pTCompBlock->numOfSubBlocks > 1) pTCompBlock->offset -= (sizeof(SCompBlock) * pSCompBlock->numOfSubBlocks);
    }

    pIdx->len -= (sizeof(SCompBlock) * pSCompBlock->numOfSubBlocks);
  }

  *pSCompBlock = *pCompBlock;

  pIdx->maxKey = pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].keyLast;
  pIdx->hasLast = pHelper->pCompInfo->blocks[pIdx->numOfBlocks - 1].last;

  tsdbDebug("vgId:%d tid:%d a super block is updated at index %d", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid,
            blkIdx);

  return 0;
}

// Get the number of rows in range [minKey, maxKey]
static int tsdbGetRowsInRange(SDataCols *pDataCols, TSKEY minKey, TSKEY maxKey) {
  if (pDataCols->numOfRows == 0) return 0;

  ASSERT(minKey <= maxKey);
  TSKEY keyFirst = dataColsKeyFirst(pDataCols);
  TSKEY keyLast = dataColsKeyLast(pDataCols);
  ASSERT(keyFirst <= keyLast);

  if (minKey > keyLast || maxKey < keyFirst) return 0;

  void *ptr1 = taosbsearch((void *)&minKey, (void *)pDataCols->cols[0].pData, pDataCols->numOfRows, sizeof(TSKEY),
                           compTSKEY, TD_GE);
  ASSERT(ptr1 != NULL);

  void *ptr2 = taosbsearch((void *)&maxKey, (void *)pDataCols->cols[0].pData, pDataCols->numOfRows, sizeof(TSKEY),
                           compTSKEY, TD_LE);
  ASSERT(ptr2 != NULL);

  if ((TSKEY *)ptr2 - (TSKEY *)ptr1 < 0) return 0;

  return ((TSKEY *)ptr2 - (TSKEY *)ptr1) + 1;
}

static void tsdbResetHelperFileImpl(SRWHelper *pHelper) {
  memset((void *)&pHelper->files, 0, sizeof(pHelper->files));
  pHelper->files.fid = -1;
  pHelper->files.headF.fd = -1;
  pHelper->files.dataF.fd = -1;
  pHelper->files.lastF.fd = -1;
  pHelper->files.nHeadF.fd = -1;
  pHelper->files.nLastF.fd = -1;
}

static int tsdbInitHelperFile(SRWHelper *pHelper) {
  STsdbCfg *pCfg = &pHelper->pRepo->config;
  size_t    tsize = sizeof(SCompIdx) * pCfg->maxTables + sizeof(TSCKSUM);
  pHelper->pCompIdx = (SCompIdx *)tmalloc(tsize);
  if (pHelper->pCompIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  tsdbResetHelperFileImpl(pHelper);
  return 0;
}

static void tsdbDestroyHelperFile(SRWHelper *pHelper) {
  tsdbCloseHelperFile(pHelper, false);
  tsdbResetHelperFileImpl(pHelper);
  tzfree(pHelper->pCompIdx);
}

// ---------- Operations on Helper Table part
static void tsdbResetHelperTableImpl(SRWHelper *pHelper) {
  memset((void *)&pHelper->tableInfo, 0, sizeof(SHelperTable));
  pHelper->hasOldLastBlock = false;
}

static void tsdbResetHelperTable(SRWHelper *pHelper) {
  tsdbResetHelperBlock(pHelper);
  tsdbResetHelperTableImpl(pHelper);
  helperClearState(pHelper, (TSDB_HELPER_TABLE_SET | TSDB_HELPER_INFO_LOAD));
}

static void tsdbInitHelperTable(SRWHelper *pHelper) { tsdbResetHelperTableImpl(pHelper); }

static void tsdbDestroyHelperTable(SRWHelper *pHelper) { tzfree((void *)pHelper->pCompInfo); }

// ---------- Operations on Helper Block part
static void tsdbResetHelperBlockImpl(SRWHelper *pHelper) {
  tdResetDataCols(pHelper->pDataCols[0]);
  tdResetDataCols(pHelper->pDataCols[1]);
}

static void tsdbResetHelperBlock(SRWHelper *pHelper) {
  tsdbResetHelperBlockImpl(pHelper);
  // helperClearState(pHelper, TSDB_HELPER_)
}

static int tsdbInitHelperBlock(SRWHelper *pHelper) {
  STsdbRepo *pRepo = helperRepo(pHelper);
  STsdbMeta *pMeta = pHelper->pRepo->tsdbMeta;

  pHelper->pDataCols[0] = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pRepo->config.maxRowsPerFileBlock);
  pHelper->pDataCols[1] = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pRepo->config.maxRowsPerFileBlock);
  if (pHelper->pDataCols[0] == NULL || pHelper->pDataCols[1] == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  tsdbResetHelperBlockImpl(pHelper);

  return 0;
}

static void tsdbDestroyHelperBlock(SRWHelper *pHelper) {
  tzfree(pHelper->pCompData);
  tdFreeDataCols(pHelper->pDataCols[0]);
  tdFreeDataCols(pHelper->pDataCols[1]);
}

static int tsdbInitHelper(SRWHelper *pHelper, STsdbRepo *pRepo, tsdb_rw_helper_t type) {
  STsdbCfg *pCfg = &pRepo->config;
  memset((void *)pHelper, 0, sizeof(*pHelper));
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  helperType(pHelper) = type;
  helperRepo(pHelper) = pRepo;
  helperState(pHelper) = TSDB_HELPER_CLEAR_STATE;

  // Init file part
  if (tsdbInitHelperFile(pHelper) < 0) goto _err;

  // Init table part
  tsdbInitHelperTable(pHelper);

  // Init block part
  if (tsdbInitHelperBlock(pHelper) < 0) goto _err;

  // TODO: pMeta->maxRowBytes and pMeta->maxCols may change here causing invalid write
  pHelper->pBuffer =
      tmalloc(sizeof(SCompData) + (sizeof(SCompCol) + sizeof(TSCKSUM) + COMP_OVERFLOW_BYTES) * pMeta->maxCols +
              pMeta->maxRowBytes * pCfg->maxRowsPerFileBlock + sizeof(TSCKSUM));
  if (pHelper->pBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  return 0;

_err:
  tsdbDestroyHelper(pHelper);
  return -1;
}

static int tsdbCheckAndDecodeColumnData(SDataCol *pDataCol, char *content, int32_t len, int8_t comp, int numOfRows,
                                        int maxPoints, char *buffer, int bufferSize) {
  // Verify by checksum
  if (!taosCheckChecksumWhole((uint8_t *)content, len)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  // Decode the data
  if (comp) {
    // // Need to decompress
    pDataCol->len = (*(tDataTypeDesc[pDataCol->type].decompFunc))(
        content, len - sizeof(TSCKSUM), numOfRows, pDataCol->pData, pDataCol->spaceSize, comp, buffer, bufferSize);
    if (pDataCol->type == TSDB_DATA_TYPE_BINARY || pDataCol->type == TSDB_DATA_TYPE_NCHAR) {
      dataColSetOffset(pDataCol, numOfRows);
    }
  } else {
    // No need to decompress, just memcpy it
    pDataCol->len = len - sizeof(TSCKSUM);
    memcpy(pDataCol->pData, content, pDataCol->len);
    if (pDataCol->type == TSDB_DATA_TYPE_BINARY || pDataCol->type == TSDB_DATA_TYPE_NCHAR) {
      dataColSetOffset(pDataCol, numOfRows);
    }
  }
  return 0;
}

static int tsdbLoadColData(SRWHelper *pHelper, SFile *pFile, SCompBlock *pCompBlock, SCompCol *pCompCol,
                           SDataCol *pDataCol) {
  ASSERT(pDataCol->colId == pCompCol->colId);
  int tsize = pDataCol->bytes * pCompBlock->numOfRows + COMP_OVERFLOW_BYTES;
  pHelper->pBuffer = trealloc(pHelper->pBuffer, pCompCol->len);
  if (pHelper->pBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  pHelper->compBuffer = trealloc(pHelper->compBuffer, tsize);
  if (pHelper->compBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (lseek(pFile->fd, pCompCol->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tread(pFile->fd, pHelper->pBuffer, pCompCol->len) < pCompCol->len) {
    tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pHelper->pRepo), pCompCol->len, pFile->fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tsdbCheckAndDecodeColumnData(pDataCol, pHelper->pBuffer, pCompCol->len, pCompBlock->algorithm,
                                   pCompBlock->numOfRows, pHelper->pRepo->config.maxRowsPerFileBlock,
                                   pHelper->compBuffer, tsizeof(pHelper->compBuffer)) < 0) {
    tsdbError("vgId:%d file %s is broken at column %d offset %" PRId64, REPO_ID(pHelper->pRepo), pFile->fname, pCompCol->colId,
              (int64_t)pCompCol->offset);
    return -1;
  }

  return 0;
}

static int tsdbLoadBlockDataColsImpl(SRWHelper *pHelper, SCompBlock *pCompBlock, SDataCols *pDataCols, int16_t *colIds, int numOfColIds) {
  ASSERT(pCompBlock->numOfSubBlocks <= 1);

  SFile *   pFile = (pCompBlock->last) ? &(pHelper->files.lastF) : &(pHelper->files.dataF);

  if (tsdbLoadCompData(pHelper, pCompBlock, NULL) < 0) goto _err;

  int dcol = 0;
  int ccol = 0;
  for (int i = 0; i < numOfColIds; i++) {
    int16_t   colId = colIds[i];
    SDataCol *pDataCol = NULL;
    SCompCol *pCompCol = NULL;

    while (true) {
      ASSERT(dcol < pDataCols->numOfCols);
      pDataCol = &pDataCols->cols[dcol];
      ASSERT(pDataCol->colId <= colId);
      if (pDataCol->colId == colId) break;
      dcol++;
    }

    ASSERT(pDataCol->colId == colId);

    while (ccol < pCompBlock->numOfCols) {
      pCompCol = &pHelper->pCompData->cols[ccol];
      if (pCompCol->colId >= colId) break;
      ccol++;
    }

    if (ccol >= pCompBlock->numOfCols || pCompCol->colId > colId) {
      dataColSetNEleNull(pDataCol, pCompBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
      continue;
    }

    ASSERT(pCompCol->colId == pDataCol->colId);

    if (tsdbLoadColData(pHelper, pFile, pCompBlock, pCompCol, pDataCol) < 0) goto _err;
    dcol++;
    ccol++;
  }

  return 0;

_err:
  return -1;
}

static int tsdbLoadBlockDataImpl(SRWHelper *pHelper, SCompBlock *pCompBlock, SDataCols *pDataCols) {
  ASSERT(pCompBlock->numOfSubBlocks <= 1);

  SFile *pFile = (pCompBlock->last) ? &(pHelper->files.lastF) : &(pHelper->files.dataF);

  pHelper->pBuffer = trealloc(pHelper->pBuffer, pCompBlock->len);
  if (pHelper->pBuffer == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  SCompData *pCompData = (SCompData *)pHelper->pBuffer;

  int fd = pFile->fd;
  if (lseek(fd, pCompBlock->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d tid:%d failed to lseek file %s since %s", REPO_ID(pHelper->pRepo), pHelper->tableInfo.tid,
              pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  if (tread(fd, (void *)pCompData, pCompBlock->len) < pCompBlock->len) {
    tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pHelper->pRepo), pCompBlock->len,
              pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  ASSERT(pCompData->numOfCols == pCompBlock->numOfCols);

  int32_t tsize = TSDB_GET_COMPCOL_LEN(pCompBlock->numOfCols);
  if (!taosCheckChecksumWhole((uint8_t *)pCompData, tsize)) {
    tsdbError("vgId:%d file %s block data is corrupted offset %" PRId64 " len %d", REPO_ID(pHelper->pRepo),
              pFile->fname, (int64_t)(pCompBlock->offset), pCompBlock->len);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    goto _err;
  }

  pDataCols->numOfRows = pCompBlock->numOfRows;

  // Recover the data
  int ccol = 0;
  int dcol = 0;
  while (dcol < pDataCols->numOfCols) {
    SDataCol *pDataCol = &(pDataCols->cols[dcol]);
    if (ccol >= pCompData->numOfCols) {
      // Set current column as NULL and forward
      dataColSetNEleNull(pDataCol, pCompBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
      continue;
    }

    SCompCol *pCompCol = &(pCompData->cols[ccol]);

    if (pCompCol->colId == pDataCol->colId) {
      if (pCompBlock->algorithm == TWO_STAGE_COMP) {
        int zsize = pDataCol->bytes * pCompBlock->numOfRows + COMP_OVERFLOW_BYTES;
        if (pCompCol->type == TSDB_DATA_TYPE_BINARY || pCompCol->type == TSDB_DATA_TYPE_NCHAR) {
          zsize += (sizeof(VarDataLenT) * pCompBlock->numOfRows);
        }
        pHelper->compBuffer = trealloc(pHelper->compBuffer, zsize);
        if (pHelper->compBuffer == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          goto _err;
        }
      }
      if (tsdbCheckAndDecodeColumnData(pDataCol, (char *)pCompData + tsize + pCompCol->offset, pCompCol->len,
                                       pCompBlock->algorithm, pCompBlock->numOfRows, pDataCols->maxPoints,
                                       pHelper->compBuffer, tsizeof(pHelper->compBuffer)) < 0) {
        tsdbError("vgId:%d file %s is broken at column %d offset %" PRId64, REPO_ID(pHelper->pRepo), pFile->fname,
                  pCompCol->colId, (int64_t)pCompCol->offset);
        goto _err;
      }
      dcol++;
      ccol++;
    } else if (pCompCol->colId < pDataCol->colId) {
      ccol++;
    } else {
      // Set current column as NULL and forward
      dataColSetNEleNull(pDataCol, pCompBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
    }
  }

  return 0;

_err:
  return -1;
}

static int tsdbEncodeSCompIdx(void **buf, SCompIdx *pIdx) {
  int tlen = 0;

  tlen += taosEncodeVariantU32(buf, pIdx->len);
  tlen += taosEncodeVariantU32(buf, pIdx->offset);
  tlen += taosEncodeFixedU8(buf, pIdx->hasLast);
  tlen += taosEncodeVariantU32(buf, pIdx->numOfBlocks);
  tlen += taosEncodeFixedU64(buf, pIdx->uid);
  tlen += taosEncodeFixedU64(buf, pIdx->maxKey);

  return tlen;
}

static void *tsdbDecodeSCompIdx(void *buf, SCompIdx *pIdx) {
  uint8_t  hasLast = 0;
  uint32_t numOfBlocks = 0;
  uint64_t value = 0;

  if ((buf = taosDecodeVariantU32(buf, &(pIdx->len))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(pIdx->offset))) == NULL) return NULL;
  if ((buf = taosDecodeFixedU8(buf, &(hasLast))) == NULL) return NULL;
  pIdx->hasLast = hasLast;
  if ((buf = taosDecodeVariantU32(buf, &(numOfBlocks))) == NULL) return NULL;
  pIdx->numOfBlocks = numOfBlocks;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->uid = (int64_t)value;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->maxKey = (TSKEY)value;

  return buf;
}
