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

#define _DEFAULT_SOURCE
#include <vnodeDataFilterFunc.h>
#include "os.h"

#include "taosmsg.h"
#include "textbuffer.h"
#include "tscJoinProcess.h"
#include "ttime.h"
#include "vnode.h"
#include "vnodeRead.h"
#include "vnodeUtil.h"

#include "vnodeQueryIO.h"
#include "vnodeQueryImpl.h"

#define ALL_CACHE_BLOCKS_CHECKED(q)                            \
  (((q)->slot == (q)->currentSlot && QUERY_IS_ASC_QUERY(q)) || \
   ((q)->slot == (q)->firstSlot && (!QUERY_IS_ASC_QUERY(q))))

#define FORWARD_CACHE_BLOCK_CHECK_SLOT(slot, step, maxblocks) (slot) = ((slot) + (step) + (maxblocks)) % (maxblocks);

static bool isGroupbyEachTable(SSqlGroupbyExpr *pGroupbyExpr, tSidSet *pSidset) {
  if (pGroupbyExpr == NULL || pGroupbyExpr->numOfGroupCols == 0) {
    return false;
  }

  for (int32_t i = 0; i < pGroupbyExpr->numOfGroupCols; ++i) {
    SColIndexEx *pColIndex = &pGroupbyExpr->columnInfo[i];
    if (pColIndex->flag == TSDB_COL_TAG) {
      assert(pSidset->numOfSids == pSidset->numOfSubSet);
      return true;
    }
  }

  return false;
}

static bool doCheckWithPrevQueryRange(SQuery *pQuery, TSKEY nextKey) {
  if ((nextKey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (nextKey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
    return false;
  }

  return true;
}

/**
 * The start position of the first check cache block is located before starting the loop.
 * And the start position for next cache blocks needs to be decided before checking each cache block.
 */
static void setStartPositionForCacheBlock(SQuery *pQuery, SCacheBlock *pBlock, bool *firstCheckSlot) {
  if (!(*firstCheckSlot)) {
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      pQuery->pos = 0;
    } else {
      pQuery->pos = pBlock->numOfPoints - 1;
    }
  } else {
    (*firstCheckSlot) = false;
  }
}

static void enableExecutionForNextTable(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[i]);
    if (pResInfo != NULL) {
      pResInfo->complete = false;
    }
  }
}

static bool needToLoadDataBlock(SQuery *pQuery, SDataStatis *pDataStatis, SQLFunctionCtx *pCtx,
                                int32_t numOfTotalPoints) {
  if (pDataStatis == NULL) {
    return true;
  }

  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    int32_t                  colIndex = pFilterInfo->info.colIdx;

    // this column not valid in current data block
    if (colIndex < 0 || pDataStatis[colIndex].colId != pFilterInfo->info.data.colId) {
      continue;
    }

    // not support pre-filter operation on binary/nchar data type
    if (!vnodeSupportPrefilter(pFilterInfo->info.data.type)) {
      continue;
    }

    // all points in current column are NULL, no need to check its boundary value
    if (pDataStatis[colIndex].numOfNull == numOfTotalPoints) {
      continue;
    }

    if (pFilterInfo->info.data.type == TSDB_DATA_TYPE_FLOAT) {
      float minval = *(double *)(&pDataStatis[colIndex].min);
      float maxval = *(double *)(&pDataStatis[colIndex].max);

      for (int32_t i = 0; i < pFilterInfo->numOfFilters; ++i) {
        if (pFilterInfo->pFilters[i].fp(&pFilterInfo->pFilters[i], (char *)&minval, (char *)&maxval)) {
          return true;
        }
      }
    } else {
      for (int32_t i = 0; i < pFilterInfo->numOfFilters; ++i) {
        if (pFilterInfo->pFilters[i].fp(&pFilterInfo->pFilters[i], (char *)&pDataStatis[colIndex].min,
                                        (char *)&pDataStatis[colIndex].max)) {
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

SArray* loadDataBlockOnDemand(SQueryRuntimeEnv* pRuntimeEnv, SDataBlockInfo* pBlockInfo, SDataStatis** pStatis) {
  SQuery* pQuery = pRuntimeEnv->pQuery;
  STsdbQueryHandle* pQueryHandle = pRuntimeEnv->pQueryHandle;
  
  uint32_t     r = 0;
  SArray *     pDataBlock = NULL;
  
  STimeWindow *w = &pQueryHandle->window;
  
  if (pQuery->numOfFilterCols > 0) {
    r = BLK_DATA_ALL_NEEDED;
  } else {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
      int32_t colId = pQuery->pSelectExpr[i].pBase.colInfo.colId;
      
      r |= aAggs[functionId].dataReqFunc(&pRuntimeEnv->pCtx[i], w->skey, w->ekey, colId);
    }
    
    if (pRuntimeEnv->pTSBuf > 0 || isIntervalQuery(pQuery)) {
      r |= BLK_DATA_ALL_NEEDED;
    }
  }
  
  if (r == BLK_DATA_NO_NEEDED) {
    //      qTrace("QInfo:%p vid:%d sid:%d id:%s, slot:%d, data block ignored, brange:%" PRId64 "-%" PRId64 ",
    //      rows:%d", GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot,
    //             pBlock->keyFirst, pBlock->keyLast, pBlock->numOfPoints);
  } else if (r == BLK_DATA_FILEDS_NEEDED) {
    if (tsdbRetrieveDataBlockStatisInfo(pRuntimeEnv->pQueryHandle, pStatis) != TSDB_CODE_SUCCESS) {
      //        return DISK_DATA_LOAD_FAILED;
    }
    
    if (pStatis == NULL) {
      pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
    }
  } else {
    assert(r == BLK_DATA_ALL_NEEDED);
    if (tsdbRetrieveDataBlockStatisInfo(pRuntimeEnv->pQueryHandle, pStatis) != TSDB_CODE_SUCCESS) {
      //        return DISK_DATA_LOAD_FAILED;
    }
    
    /*
     * if this block is completed included in the query range, do more filter operation
     * filter the data block according to the value filter condition.
     * no need to load the data block, continue for next block
     */
    if (!needToLoadDataBlock(pQuery, *pStatis, pRuntimeEnv->pCtx, pBlockInfo->size)) {
#if defined(_DEBUG_VIEW)
      dTrace("QInfo:%p fileId:%d, slot:%d, block discarded by per-filter", GET_QINFO_ADDR(pQuery), pQuery->fileId,
               pQuery->slot);
#endif
      //        return DISK_DATA_DISCARDED;
    }
    
    pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
  }
  
  return pDataBlock;
}

static void queryOnMultiDataFiles_(SQInfo *pQInfo, SMeterDataInfo *pMeterDataInfo) {
  SQuery *               pQuery = &pQInfo->query;
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  SMeterObj *         pTempMeter = getMeterObj(pSupporter->pMetersHashTable, pSupporter->pMeterSidExtInfo[0]->sid);
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pTempMeter->searchAlgorithm];

  SQueryFilesInfo *pVnodeFileInfo = &pRuntimeEnv->vnodeFileInfo;

  dTrace("QInfo:%p start to check data blocks in %d files", pQInfo, pVnodeFileInfo->numOfFiles);

  STsdbQueryHandle *pQueryHandle = pRuntimeEnv->pQueryHandle;
  while (tsdbNextDataBlock(pQueryHandle)) {
    if (isQueryKilled(pQuery)) {
      break;
    }

    // prepare the SMeterDataInfo struct for each table

    SDataBlockInfo blockInfo = tsdbRetrieveDataBlockInfo(pQueryHandle);
    SMeterObj *    pMeterObj = getMeterObj(pSupporter->pMetersHashTable, blockInfo.sid);
    
    pQInfo->pObj = pMeterObj;
    pRuntimeEnv->pMeterObj = pMeterObj;
    
    SMeterDataInfo *pTableDataInfo = NULL;
    for (int32_t i = 0; i < pSupporter->pSidSet->numOfSids; ++i) {
      if (pMeterDataInfo[i].pMeterObj == pMeterObj) {
        pTableDataInfo = &pMeterDataInfo[i];
        break;
      }
    }
    
    assert(pTableDataInfo != NULL);
    SMeterQueryInfo *pMeterQueryInfo = pTableDataInfo->pMeterQInfo;

    if (pTableDataInfo->pMeterQInfo == NULL) {
      pTableDataInfo->pMeterQInfo = createMeterQueryInfo(pSupporter, pMeterObj->sid, pQuery->skey, pQuery->ekey);
    }

    restoreIntervalQueryRange(pRuntimeEnv, pMeterQueryInfo);

    SDataStatis *pStatis = NULL;
    SArray *pDataBlock = loadDataBlockOnDemand(pRuntimeEnv, &blockInfo, &pStatis);
    
    TSKEY nextKey = blockInfo.window.ekey;
    if (pQuery->intervalTime == 0) {
      setExecutionContext(pSupporter, pMeterQueryInfo, pTableDataInfo->meterOrderIdx, pTableDataInfo->groupIdx,
                          nextKey);
    } else {  // interval query
      setIntervalQueryRange(pMeterQueryInfo, pSupporter, nextKey);
      int32_t ret = setAdditionalInfo(pSupporter, pTableDataInfo->meterOrderIdx, pMeterQueryInfo);
      if (ret != TSDB_CODE_SUCCESS) {
        pQInfo->killed = 1;
        return;
      }
    }

    stableApplyFunctionsOnBlock_(pSupporter, pTableDataInfo, &blockInfo, pStatis, pDataBlock, searchFn);
  }
}

static bool multimeterMultioutputHelper(SQInfo *pQInfo, bool *dataInDisk, bool *dataInCache, int32_t index,
                                        int32_t start) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  SMeterSidExtInfo **pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;
  SQueryRuntimeEnv * pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *           pQuery = &pQInfo->query;

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  SMeterObj *pMeterObj = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[index]->sid);
  if (pMeterObj == NULL) {
    dError("QInfo:%p do not find required meter id: %d, all meterObjs id is:", pQInfo, pMeterSidExtInfo[index]->sid);
    return false;
  }

  vnodeSetTagValueInParam(pSupporter->pSidSet, pRuntimeEnv, pMeterSidExtInfo[index]);

  dTrace("QInfo:%p query on (%d): vid:%d sid:%d meterId:%s, qrange:%" PRId64 "-%" PRId64, pQInfo, index - start,
         pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey);

  pQInfo->pObj = pMeterObj;
  pQuery->lastKey = pQuery->skey;
  pRuntimeEnv->pMeterObj = pMeterObj;

  vnodeUpdateQueryColumnIndex(pQuery, pRuntimeEnv->pMeterObj);
  vnodeUpdateFilterColumnIndex(pQuery);

//  vnodeCheckIfDataExists(pRuntimeEnv, pMeterObj, dataInDisk, dataInCache);

  // data in file or cache is not qualified for the query. abort
  if (!(dataInCache || dataInDisk)) {
    dTrace("QInfo:%p vid:%d sid:%d meterId:%s, qrange:%" PRId64 "-%" PRId64 ", nores, %p", pQInfo, pMeterObj->vnode,
           pMeterObj->sid, pMeterObj->meterId, pQuery->skey, pQuery->ekey, pQuery);
    return false;
  }

  if (pRuntimeEnv->pTSBuf != NULL) {
    if (pRuntimeEnv->cur.vnodeIndex == -1) {
      int64_t tag = pRuntimeEnv->pCtx[0].tag.i64Key;
      STSElem elem = tsBufGetElemStartPos(pRuntimeEnv->pTSBuf, 0, tag);

      // failed to find data with the specified tag value
      if (elem.vnode < 0) {
        return false;
      }
    } else {
      tsBufSetCursor(pRuntimeEnv->pTSBuf, &pRuntimeEnv->cur);
    }
  }

  initCtxOutputBuf(pRuntimeEnv);
  return true;
}

static int64_t doCheckMetersInGroup(SQInfo *pQInfo, int32_t index, int32_t start) {
  SQuery *               pQuery = &pQInfo->query;
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  bool dataInDisk = true;
  bool dataInCache = true;
  if (!multimeterMultioutputHelper(pQInfo, &dataInDisk, &dataInCache, index, start)) {
    return 0;
  }

#if DEFAULT_IO_ENGINE == IO_ENGINE_MMAP
  for (int32_t i = 0; i < pRuntimeEnv->numOfFiles; ++i) {
    resetMMapWindow(&pRuntimeEnv->pVnodeFiles[i]);
  }
#endif

  SPointInterpoSupporter pointInterpSupporter = {0};
  pointInterpSupporterInit(pQuery, &pointInterpSupporter);
  assert(0);
  
//  if (!normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &pointInterpSupporter, NULL)) {
//    pointInterpSupporterDestroy(&pointInterpSupporter);
//    return 0;
//  }

  /*
   * here we set the value for before and after the specified time into the
   * parameter for interpolation query
   */
  pointInterpSupporterSetData(pQInfo, &pointInterpSupporter);
  pointInterpSupporterDestroy(&pointInterpSupporter);

  vnodeScanAllData(pRuntimeEnv);

  // first/last_row query, do not invoke the finalize for super table query
  doFinalizeResult(pRuntimeEnv);

  int64_t numOfRes = getNumOfResult(pRuntimeEnv);
  assert(numOfRes == 1 || numOfRes == 0);

  // accumulate the point interpolation result
  if (numOfRes > 0) {
    pQuery->pointsRead += numOfRes;
    forwardCtxOutputBuf(pRuntimeEnv, numOfRes);
  }

  return numOfRes;
}

/**
 * super table query handler
 * 1. super table projection query, group-by on normal columns query, ts-comp query
 * 2. point interpolation query, last row query
 *
 * @param pQInfo
 */
static void vnodeSTableSeqProcessor(SQInfo *pQInfo) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  SMeterSidExtInfo **pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;
  SQueryRuntimeEnv * pRuntimeEnv = &pSupporter->runtimeEnv;

  SQuery * pQuery = &pQInfo->query;
  tSidSet *pSids = pSupporter->pSidSet;

  int32_t vid = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[0]->sid)->vnode;

  if (isPointInterpoQuery(pQuery)) {
    resetCtxOutputBuf(pRuntimeEnv);

    assert(pQuery->limit.offset == 0 && pQuery->limit.limit != 0);

    while (pSupporter->subgroupIdx < pSids->numOfSubSet) {
      int32_t start = pSids->starterPos[pSupporter->subgroupIdx];
      int32_t end = pSids->starterPos[pSupporter->subgroupIdx + 1] - 1;

      if (isFirstLastRowQuery(pQuery)) {
        dTrace("QInfo:%p last_row query on vid:%d, numOfGroups:%d, current group:%d", pQInfo, vid, pSids->numOfSubSet,
               pSupporter->subgroupIdx);

        TSKEY   key = -1;
        int32_t index = -1;

        // choose the last key for one group
        pSupporter->meterIdx = start;

        for (int32_t k = start; k <= end; ++k, pSupporter->meterIdx++) {
          if (isQueryKilled(pQuery)) {
            setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
            return;
          }

          // get the last key of meters that belongs to this group
          SMeterObj *pMeterObj = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[k]->sid);
          if (pMeterObj != NULL) {
            if (key < pMeterObj->lastKey) {
              key = pMeterObj->lastKey;
              index = k;
            }
          }
        }

        pQuery->skey = key;
        pQuery->ekey = key;
        pSupporter->rawSKey = key;
        pSupporter->rawEKey = key;

        int64_t num = doCheckMetersInGroup(pQInfo, index, start);
        assert(num >= 0);
      } else {
        dTrace("QInfo:%p interp query on vid:%d, numOfGroups:%d, current group:%d", pQInfo, vid, pSids->numOfSubSet,
               pSupporter->subgroupIdx);

        for (int32_t k = start; k <= end; ++k) {
          if (isQueryKilled(pQuery)) {
            setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
            return;
          }

          pQuery->skey = pSupporter->rawSKey;
          pQuery->ekey = pSupporter->rawEKey;

          int64_t num = doCheckMetersInGroup(pQInfo, k, start);
          if (num == 1) {
            break;
          }
        }
      }

      pSupporter->subgroupIdx++;

      // output buffer is full, return to client
      if (pQuery->pointsRead >= pQuery->pointsToRead) {
        break;
      }
    }
  } else {
    /*
     * 1. super table projection query, 2. group-by on normal columns query, 3. ts-comp query
     */
    assert(pSupporter->meterIdx >= 0);

    /*
     * if the subgroup index is larger than 0, results generated by group by tbname,k is existed.
     * we need to return it to client in the first place.
     */
    if (pSupporter->subgroupIdx > 0) {
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
      pQInfo->pointsRead += pQuery->pointsRead;

      if (pQuery->pointsRead > 0) {
        return;
      }
    }

    if (pSupporter->meterIdx >= pSids->numOfSids) {
      return;
    }

    resetCtxOutputBuf(pRuntimeEnv);
    resetTimeWindowInfo(pRuntimeEnv, &pRuntimeEnv->windowResInfo);

    while (pSupporter->meterIdx < pSupporter->numOfMeters) {
      int32_t k = pSupporter->meterIdx;

      if (isQueryKilled(pQuery)) {
        setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
        return;
      }

      TSKEY skey = pQInfo->pTableQuerySupporter->pMeterSidExtInfo[k]->key;
      if (skey > 0) {
        pQuery->skey = skey;
      }

      bool dataInDisk = true;
      bool dataInCache = true;
      if (!multimeterMultioutputHelper(pQInfo, &dataInDisk, &dataInCache, k, 0)) {
        pQuery->skey = pSupporter->rawSKey;
        pQuery->ekey = pSupporter->rawEKey;

        pSupporter->meterIdx++;
        continue;
      }

#if DEFAULT_IO_ENGINE == IO_ENGINE_MMAP
      for (int32_t i = 0; i < pRuntimeEnv->numOfFiles; ++i) {
        resetMMapWindow(&pRuntimeEnv->pVnodeFiles[i]);
      }
#endif

      SPointInterpoSupporter pointInterpSupporter = {0};
      assert(0);
//      if (normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &pointInterpSupporter, NULL) == false) {
//        pQuery->skey = pSupporter->rawSKey;
//        pQuery->ekey = pSupporter->rawEKey;
//
//        pSupporter->meterIdx++;
//        continue;
//      }

      // TODO handle the limit problem
      if (pQuery->numOfFilterCols == 0 && pQuery->limit.offset > 0) {
        forwardQueryStartPosition(pRuntimeEnv);

        if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
          pQuery->skey = pSupporter->rawSKey;
          pQuery->ekey = pSupporter->rawEKey;

          pSupporter->meterIdx++;
          continue;
        }
      }

      vnodeScanAllData(pRuntimeEnv);

      pQuery->pointsRead = getNumOfResult(pRuntimeEnv);
      doSkipResults(pRuntimeEnv);

      // the limitation of output result is reached, set the query completed
      if (doRevisedResultsByLimit(pQInfo)) {
        pSupporter->meterIdx = pSupporter->pSidSet->numOfSids;
        break;
      }

      // enable execution for next table, when handling the projection query
      enableExecutionForNextTable(pRuntimeEnv);

      if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
        /*
         * query range is identical in terms of all meters involved in query,
         * so we need to restore them at the *beginning* of query on each meter,
         * not the consecutive query on meter on which is aborted due to buffer limitation
         * to ensure that, we can reset the query range once query on a meter is completed.
         */
        pQuery->skey = pSupporter->rawSKey;
        pQuery->ekey = pSupporter->rawEKey;
        pSupporter->meterIdx++;

        pQInfo->pTableQuerySupporter->pMeterSidExtInfo[k]->key = pQuery->lastKey;

        // if the buffer is full or group by each table, we need to jump out of the loop
        if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL) ||
            isGroupbyEachTable(pQuery->pGroupbyExpr, pSupporter->pSidSet)) {
          break;
        }

      } else {  // forward query range
        pQuery->skey = pQuery->lastKey;

        // all data in the result buffer are skipped due to the offset, continue to retrieve data from current meter
        if (pQuery->pointsRead == 0) {
          assert(!Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL));
          continue;
        } else {
          pQInfo->pTableQuerySupporter->pMeterSidExtInfo[k]->key = pQuery->lastKey;
          // buffer is full, wait for the next round to retrieve data from current meter
          assert(Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL));
          break;
        }
      }
    }
  }

  /*
   * 1. super table projection query, group-by on normal columns query, ts-comp query
   * 2. point interpolation query, last row query
   *
   * group-by on normal columns query and last_row query do NOT invoke the finalizer here,
   * since the finalize stage will be done at the client side.
   *
   * projection query, point interpolation query do not need the finalizer.
   *
   * Only the ts-comp query requires the finalizer function to be executed here.
   */
  if (isTSCompQuery(pQuery)) {
    doFinalizeResult(pRuntimeEnv);
  }

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->cur = pRuntimeEnv->pTSBuf->cur;
  }

  // todo refactor
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

    for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
      SWindowStatus *pStatus = &pWindowResInfo->pResult[i].status;
      pStatus->closed = true;  // enable return all results for group by normal columns

      SWindowResult *pResult = &pWindowResInfo->pResult[i];
      for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
        pResult->numOfRows = MAX(pResult->numOfRows, pResult->resultInfo[j].numOfRes);
      }
    }

    pQInfo->pTableQuerySupporter->subgroupIdx = 0;
    pQuery->pointsRead = 0;
    copyFromWindowResToSData(pQInfo, pWindowResInfo->pResult);
  }

  pQInfo->pointsRead += pQuery->pointsRead;
  pQuery->pointsOffset = pQuery->pointsToRead;

  dTrace(
      "QInfo %p vid:%d, numOfMeters:%d, index:%d, numOfGroups:%d, %d points returned, totalRead:%d totalReturn:%d,"
      "next skey:%" PRId64 ", offset:%" PRId64,
      pQInfo, vid, pSids->numOfSids, pSupporter->meterIdx, pSids->numOfSubSet, pQuery->pointsRead, pQInfo->pointsRead,
      pQInfo->pointsReturned, pQuery->skey, pQuery->limit.offset);
}

static void doOrderedScan(SQInfo *pQInfo) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQuery *               pQuery = &pQInfo->query;

  if (pSupporter->pMeterDataInfo == NULL) {
    pSupporter->pMeterDataInfo = calloc(pSupporter->pSidSet->numOfSids, sizeof(SMeterDataInfo));
  }

  SMeterSidExtInfo **pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;
  
  tSidSet* pSidset = pSupporter->pSidSet;
  int32_t groupId = 0;

  for (int32_t i = 0; i < pSidset->numOfSids; ++i) {  // load all meter meta info
    SMeterObj *pMeterObj = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[i]->sid);
    if (pMeterObj == NULL) {
      dError("QInfo:%p failed to find required sid:%d", pQInfo, pMeterSidExtInfo[i]->sid);
      continue;
    }

    if (i >= pSidset->starterPos[groupId + 1]) {
      groupId += 1;
    }

    SMeterDataInfo *pOneMeterDataInfo = &pSupporter->pMeterDataInfo[i];
    assert(pOneMeterDataInfo->pMeterObj == NULL);
    
    setMeterDataInfo(pOneMeterDataInfo, pMeterObj, i, groupId);
    pOneMeterDataInfo->pMeterQInfo = createMeterQueryInfo(pSupporter, pMeterObj->sid, pQuery->skey, pQuery->ekey);
  }

  queryOnMultiDataFiles_(pQInfo, pSupporter->pMeterDataInfo);
  if (pQInfo->code != TSDB_CODE_SUCCESS) {
    return;
  }
}

static void setupMeterQueryInfoForSupplementQuery(STableQuerySupportObj *pSupporter) {
  SQuery *pQuery = pSupporter->runtimeEnv.pQuery;

  for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
    SMeterQueryInfo *pMeterQueryInfo = pSupporter->pMeterDataInfo[i].pMeterQInfo;
    changeMeterQueryInfoForSuppleQuery(pQuery, pMeterQueryInfo, pSupporter->rawSKey, pSupporter->rawEKey);
  }
}

static void doMultiMeterSupplementaryScan(SQInfo *pQInfo) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = &pQInfo->query;

  if (!needSupplementaryScan(pQuery)) {
    dTrace("QInfo:%p no need to do supplementary scan, query completed", pQInfo);
    return;
  }

  SET_SUPPLEMENT_SCAN_FLAG(pRuntimeEnv);
  disableFunctForSuppleScan(pSupporter, pQuery->order.order);

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->pTSBuf->cur.order = pRuntimeEnv->pTSBuf->cur.order ^ 1u;
  }

  SWAP(pSupporter->rawSKey, pSupporter->rawEKey, TSKEY);
  setupMeterQueryInfoForSupplementQuery(pSupporter);

  int64_t st = taosGetTimestampMs();

  doOrderedScan(pQInfo);

  int64_t et = taosGetTimestampMs();
  dTrace("QInfo:%p supplementary scan completed, elapsed time: %lldms", pQInfo, et - st);

  /*
   * restore the env
   * the meter query info is not reset to the original state
   */
  SWAP(pSupporter->rawSKey, pSupporter->rawEKey, TSKEY);
  enableFunctForMasterScan(pRuntimeEnv, pQuery->order.order);

  if (pRuntimeEnv->pTSBuf != NULL) {
    pRuntimeEnv->pTSBuf->cur.order = pRuntimeEnv->pTSBuf->cur.order ^ 1;
  }

  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
}

static void vnodeMultiMeterQueryProcessor(SQInfo *pQInfo) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *               pQuery = &pQInfo->query;

  if (pSupporter->subgroupIdx > 0) {
    /*
     * if the subgroupIdx > 0, the query process must be completed yet, we only need to
     * copy the data into output buffer
     */
    if (pQuery->intervalTime > 0) {
      copyResToQueryResultBuf(pSupporter, pQuery);

#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif
    } else {
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
    }

    pQInfo->pointsRead += pQuery->pointsRead;

    if (pQuery->pointsRead == 0) {
      vnodePrintQueryStatistics(pSupporter);
    }

    dTrace("QInfo:%p points returned:%d, totalRead:%d totalReturn:%d", pQInfo, pQuery->pointsRead, pQInfo->pointsRead,
           pQInfo->pointsReturned);
    return;
  }

  pSupporter->pMeterDataInfo = (SMeterDataInfo *)calloc(1, sizeof(SMeterDataInfo) * pSupporter->numOfMeters);
  if (pSupporter->pMeterDataInfo == NULL) {
    dError("QInfo:%p failed to allocate memory, %s", pQInfo, strerror(errno));
    pQInfo->code = -TSDB_CODE_SERV_OUT_OF_MEMORY;
    return;
  }

  dTrace("QInfo:%p query start, qrange:%" PRId64 "-%" PRId64 ", order:%d, group:%d", pQInfo, pSupporter->rawSKey,
         pSupporter->rawEKey, pQuery->order.order, pSupporter->pSidSet->numOfSubSet);

  dTrace("QInfo:%p main query scan start", pQInfo);
  int64_t st = taosGetTimestampMs();
  doOrderedScan(pQInfo);
  
  int64_t et = taosGetTimestampMs();
  dTrace("QInfo:%p main scan completed, elapsed time: %lldms, supplementary scan start, order:%d", pQInfo, et - st,
         pQuery->order.order ^ 1u);

  if (pQuery->intervalTime > 0) {
    for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
      SMeterQueryInfo *pMeterQueryInfo = pSupporter->pMeterDataInfo[i].pMeterQInfo;
      closeAllTimeWindow(&pMeterQueryInfo->windowResInfo);
    }
  } else {  // close results for group result
    closeAllTimeWindow(&pRuntimeEnv->windowResInfo);
  }

  doMultiMeterSupplementaryScan(pQInfo);

  if (isQueryKilled(pQuery)) {
    dTrace("QInfo:%p query killed, abort", pQInfo);
    return;
  }

  if (pQuery->intervalTime > 0 || isSumAvgRateQuery(pQuery)) {
    assert(pSupporter->subgroupIdx == 0 && pSupporter->numOfGroupResultPages == 0);

    if (mergeMetersResultToOneGroups(pSupporter) == TSDB_CODE_SUCCESS) {
      copyResToQueryResultBuf(pSupporter, pQuery);

#ifdef _DEBUG_VIEW
      displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif
    }
  } else {  // not a interval query
    copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
  }

  // handle the limitation of output buffer
  pQInfo->pointsRead += pQuery->pointsRead;
  dTrace("QInfo:%p points returned:%d, totalRead:%d totalReturn:%d", pQInfo, pQuery->pointsRead, pQInfo->pointsRead,
         pQInfo->pointsReturned);
}

/*
 * in each query, this function will be called only once, no retry for further result.
 *
 * select count(*)/top(field,k)/avg(field name) from table_name [where ts>now-1a];
 * select count(*) from table_name group by status_column;
 */
static void vnodeSingleTableFixedOutputProcessor(SQInfo *pQInfo) {
  SQuery *          pQuery = &pQInfo->query;
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->pTableQuerySupporter->runtimeEnv;

  vnodeScanAllData(pRuntimeEnv);
  doFinalizeResult(pRuntimeEnv);

  if (isQueryKilled(pQuery)) {
    return;
  }

  // since the numOfOutputElems must be identical for all sql functions that are allowed to be executed simutanelously.
  pQuery->pointsRead = getNumOfResult(pRuntimeEnv);
  //  assert(pQuery->pointsRead <= pQuery->pointsToRead &&
  //         Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED | QUERY_NO_DATA_TO_CHECK));

  // must be top/bottom query if offset > 0
  if (pQuery->limit.offset > 0) {
    assert(isTopBottomQuery(pQuery));
  }

  doSkipResults(pRuntimeEnv);
  doRevisedResultsByLimit(pQInfo);

  pQInfo->pointsRead = pQuery->pointsRead;
}

static void vnodeSingleTableMultiOutputProcessor(SQInfo *pQInfo) {
#if 0
  SQuery *   pQuery = &pQInfo->query;
  SMeterObj *pMeterObj = pQInfo->pObj;

  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->pTableQuerySupporter->runtimeEnv;

  // for ts_comp query, re-initialized is not allowed
  if (!isTSCompQuery(pQuery)) {
    resetCtxOutputBuf(pRuntimeEnv);
  }

  while (1) {
    vnodeScanAllData(pRuntimeEnv);
    doFinalizeResult(pRuntimeEnv);

    if (isQueryKilled(pQuery)) {
      return;
    }

    pQuery->pointsRead = getNumOfResult(pRuntimeEnv);
    if (pQuery->limit.offset > 0 && pQuery->numOfFilterCols > 0 && pQuery->pointsRead > 0) {
      doSkipResults(pRuntimeEnv);
    }

    /*
     * 1. if pQuery->pointsRead == 0, pQuery->limit.offset >= 0, still need to check data
     * 2. if pQuery->pointsRead > 0, pQuery->limit.offset must be 0
     */
    if (pQuery->pointsRead > 0 || Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED | QUERY_NO_DATA_TO_CHECK)) {
      break;
    }

    TSKEY nextTimestamp = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->nextPos);
    assert(nextTimestamp > 0 || ((nextTimestamp < 0) && Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)));

    dTrace("QInfo:%p vid:%d sid:%d id:%s, skip current result, offset:%" PRId64 ", next qrange:%" PRId64 "-%" PRId64,
           pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->limit.offset, pQuery->lastKey,
           pQuery->ekey);

    resetCtxOutputBuf(pRuntimeEnv);
  }

  doRevisedResultsByLimit(pQInfo);
  pQInfo->pointsRead += pQuery->pointsRead;

  if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {
    TSKEY nextTimestamp = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->nextPos);
    assert(nextTimestamp > 0 || ((nextTimestamp < 0) && Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)));

    dTrace("QInfo:%p vid:%d sid:%d id:%s, query abort due to buffer limitation, next qrange:%" PRId64 "-%" PRId64,
           pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->lastKey, pQuery->ekey);
  }

  dTrace("QInfo:%p vid:%d sid:%d id:%s, %d points returned, totalRead:%d totalReturn:%d", pQInfo, pMeterObj->vnode,
         pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, pQInfo->pointsRead, pQInfo->pointsReturned);

  pQuery->pointsOffset = pQuery->pointsToRead;  // restore the available buffer
  if (!isTSCompQuery(pQuery)) {
    assert(pQuery->pointsRead <= pQuery->pointsToRead);
  }
  
#endif
}

static void vnodeSingleMeterIntervalMainLooper(STableQuerySupportObj *pSupporter, SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  while (1) {
    initCtxOutputBuf(pRuntimeEnv);
    vnodeScanAllData(pRuntimeEnv);

    if (isQueryKilled(pQuery)) {
      return;
    }

    assert(!Q_STATUS_EQUAL(pQuery->over, QUERY_NOT_COMPLETED));
    doFinalizeResult(pRuntimeEnv);

    // here we can ignore the records in case of no interpolation
    // todo handle offset, in case of top/bottom interval query
    if ((pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) && pQuery->limit.offset > 0 &&
        pQuery->interpoType == TSDB_INTERPO_NONE) {
      // maxOutput <= 0, means current query does not generate any results
      int32_t numOfClosed = numOfClosedTimeWindow(&pRuntimeEnv->windowResInfo);

      int32_t c = MIN(numOfClosed, pQuery->limit.offset);
      clearFirstNTimeWindow(pRuntimeEnv, c);
      pQuery->limit.offset -= c;
    }

    if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
      break;
    }

    // load the data block for the next retrieve
//    loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->nextPos);
    if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {
      break;
    }
  }
}

/* handle time interval query on single table */
static void vnodeSingleTableIntervalProcessor(SQInfo *pQInfo) {
  SQuery *   pQuery = &(pQInfo->query);
  SMeterObj *pMeterObj = pQInfo->pObj;

  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  int32_t numOfInterpo = 0;

  while (1) {
    resetCtxOutputBuf(pRuntimeEnv);
    vnodeSingleMeterIntervalMainLooper(pSupporter, pRuntimeEnv);

    if (pQuery->intervalTime > 0) {
      pSupporter->subgroupIdx = 0;  // always start from 0
      pQuery->pointsRead = 0;
      copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);

      clearFirstNTimeWindow(pRuntimeEnv, pSupporter->subgroupIdx);
    }

    // the offset is handled at prepare stage if no interpolation involved
    if (pQuery->interpoType == TSDB_INTERPO_NONE) {
      doRevisedResultsByLimit(pQInfo);
      break;
    } else {
      taosInterpoSetStartInfo(&pRuntimeEnv->interpoInfo, pQuery->pointsRead, pQuery->interpoType);
      SData **pInterpoBuf = pRuntimeEnv->pInterpoBuf;

      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        memcpy(pInterpoBuf[i]->data, pQuery->sdata[i]->data, pQuery->pointsRead * pQuery->pSelectExpr[i].resBytes);
      }

      numOfInterpo = 0;
      pQuery->pointsRead = vnodeQueryResultInterpolate(pQInfo, (tFilePage **)pQuery->sdata, (tFilePage **)pInterpoBuf,
                                                       pQuery->pointsRead, &numOfInterpo);

      dTrace("QInfo: %p interpo completed, final:%d", pQInfo, pQuery->pointsRead);
      if (pQuery->pointsRead > 0 || Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED | QUERY_NO_DATA_TO_CHECK)) {
        doRevisedResultsByLimit(pQInfo);
        break;
      }

      // no result generated yet, continue retrieve data
      pQuery->pointsRead = 0;
    }
  }

  // all data scanned, the group by normal column can return
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {  // todo refactor with merge interval time result
    pSupporter->subgroupIdx = 0;
    pQuery->pointsRead = 0;
    copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
    clearFirstNTimeWindow(pRuntimeEnv, pSupporter->subgroupIdx);
  }

  pQInfo->pointsRead += pQuery->pointsRead;
  pQInfo->pointsInterpo += numOfInterpo;

  dTrace("%p vid:%d sid:%d id:%s, %d points returned %d points interpo, totalRead:%d totalInterpo:%d totalReturn:%d",
         pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, numOfInterpo,
         pQInfo->pointsRead - pQInfo->pointsInterpo, pQInfo->pointsInterpo, pQInfo->pointsReturned);
}

void vnodeSingleTableQuery(SSchedMsg *pMsg) {
  SQInfo *pQInfo = (SQInfo *)pMsg->ahandle;

  if (pQInfo == NULL || pQInfo->pTableQuerySupporter == NULL) {
    dTrace("%p freed abort query", pQInfo);
    return;
  }

  if (pQInfo->killed) {
    dTrace("QInfo:%p it is already killed, abort", pQInfo);
    vnodeDecRefCount(pQInfo);

    return;
  }

  assert(pQInfo->refCount >= 1);

  SQuery *               pQuery = &pQInfo->query;
  SMeterObj *            pMeterObj = pQInfo->pObj;
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  assert(pRuntimeEnv->pMeterObj == pMeterObj);

  dTrace("vid:%d sid:%d id:%s, query thread is created, numOfQueries:%d, QInfo:%p", pMeterObj->vnode, pMeterObj->sid,
         pMeterObj->meterId, pMeterObj->numOfQueries, pQInfo);

  if (vnodeHasRemainResults(pQInfo)) {
    /*
     * There are remain results that are not returned due to result interpolation
     * So, we do keep in this procedure instead of launching retrieve procedure for next results.
     */
    int32_t numOfInterpo = 0;

    int32_t remain = taosNumOfRemainPoints(&pRuntimeEnv->interpoInfo);
    pQuery->pointsRead = vnodeQueryResultInterpolate(pQInfo, (tFilePage **)pQuery->sdata,
                                                     (tFilePage **)pRuntimeEnv->pInterpoBuf, remain, &numOfInterpo);

    doRevisedResultsByLimit(pQInfo);

    pQInfo->pointsInterpo += numOfInterpo;
    pQInfo->pointsRead += pQuery->pointsRead;

    dTrace(
        "QInfo:%p vid:%d sid:%d id:%s, %d points returned %d points interpo, totalRead:%d totalInterpo:%d "
        "totalReturn:%d",
        pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, numOfInterpo,
        pQInfo->pointsRead, pQInfo->pointsInterpo, pQInfo->pointsReturned);

    sem_post(&pQInfo->dataReady);
    vnodeDecRefCount(pQInfo);

    return;
  }

  // here we have scan all qualified data in both data file and cache
  if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
    // continue to get push data from the group result
    if (isGroupbyNormalCol(pQuery->pGroupbyExpr) ||
        (pQuery->intervalTime > 0 && pQInfo->pointsReturned < pQuery->limit.limit)) {
      // todo limit the output for interval query?
      pQuery->pointsRead = 0;
      pSupporter->subgroupIdx = 0;  // always start from 0

      if (pRuntimeEnv->windowResInfo.size > 0) {
        copyFromWindowResToSData(pQInfo, pRuntimeEnv->windowResInfo.pResult);
        pQInfo->pointsRead += pQuery->pointsRead;

        clearFirstNTimeWindow(pRuntimeEnv, pSupporter->subgroupIdx);

        if (pQuery->pointsRead > 0) {
          dTrace("QInfo:%p vid:%d sid:%d id:%s, %d points returned %d from group results, totalRead:%d totalReturn:%d",
                 pQInfo, pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead, pQInfo->pointsRead,
                 pQInfo->pointsInterpo, pQInfo->pointsReturned);

          sem_post(&pQInfo->dataReady);
          vnodeDecRefCount(pQInfo);

          return;
        }
      }
    }

    pQInfo->over = 1;
    dTrace("QInfo:%p vid:%d sid:%d id:%s, query over, %d points are returned", pQInfo, pMeterObj->vnode, pMeterObj->sid,
           pMeterObj->meterId, pQInfo->pointsRead);

    vnodePrintQueryStatistics(pSupporter);
    sem_post(&pQInfo->dataReady);

    vnodeDecRefCount(pQInfo);
    return;
  }

  /* number of points returned during this query  */
  pQuery->pointsRead = 0;

  int64_t st = taosGetTimestampUs();

  // group by normal column, sliding window query, interval query are handled by interval query processor
  if (pQuery->intervalTime != 0 || isGroupbyNormalCol(pQuery->pGroupbyExpr)) {  // interval (down sampling operation)
    assert(pQuery->checkBufferInLoop == 0 && pQuery->pointsOffset == pQuery->pointsToRead);
    vnodeSingleTableIntervalProcessor(pQInfo);
  } else {
    if (isFixedOutputQuery(pQuery)) {
      assert(pQuery->checkBufferInLoop == 0);
      vnodeSingleTableFixedOutputProcessor(pQInfo);
    } else {  // diff/add/multiply/subtract/division
      assert(pQuery->checkBufferInLoop == 1);
      vnodeSingleTableMultiOutputProcessor(pQInfo);
    }
  }

  // record the total elapsed time
  pQInfo->useconds += (taosGetTimestampUs() - st);

  /* check if query is killed or not */
  if (isQueryKilled(pQuery)) {
    dTrace("QInfo:%p query is killed", pQInfo);
    pQInfo->over = 1;
  } else {
    dTrace("QInfo:%p vid:%d sid:%d id:%s, meter query thread completed, %d points are returned", pQInfo,
           pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->pointsRead);
  }

  sem_post(&pQInfo->dataReady);
  vnodeDecRefCount(pQInfo);
}

void vnodeMultiMeterQuery(SSchedMsg *pMsg) {
  SQInfo *pQInfo = (SQInfo *)pMsg->ahandle;

  if (pQInfo == NULL || pQInfo->pTableQuerySupporter == NULL) {
    return;
  }

  if (pQInfo->killed) {
    vnodeDecRefCount(pQInfo);
    dTrace("QInfo:%p it is already killed, abort", pQInfo);
    return;
  }

  assert(pQInfo->refCount >= 1);

  SQuery *pQuery = &pQInfo->query;
  pQuery->pointsRead = 0;

  int64_t st = taosGetTimestampUs();
  if (pQuery->intervalTime > 0 ||
      (isFixedOutputQuery(pQuery) && (!isPointInterpoQuery(pQuery)) && !isGroupbyNormalCol(pQuery->pGroupbyExpr))) {
    assert(pQuery->checkBufferInLoop == 0);
    vnodeMultiMeterQueryProcessor(pQInfo);
  } else {
    assert((pQuery->checkBufferInLoop == 1 && pQuery->intervalTime == 0) || isPointInterpoQuery(pQuery) ||
           isGroupbyNormalCol(pQuery->pGroupbyExpr));

    vnodeSTableSeqProcessor(pQInfo);
  }

  /* record the total elapsed time */
  pQInfo->useconds += (taosGetTimestampUs() - st);
  pQInfo->over = isQueryKilled(pQuery) ? 1 : 0;

  taosInterpoSetStartInfo(&pQInfo->pTableQuerySupporter->runtimeEnv.interpoInfo, pQuery->pointsRead,
                          pQInfo->query.interpoType);

  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  if (pQuery->pointsRead == 0) {
    pQInfo->over = 1;
    dTrace("QInfo:%p over, %d meters queried, %d points are returned", pQInfo, pSupporter->numOfMeters,
           pQInfo->pointsRead);
    vnodePrintQueryStatistics(pSupporter);
  }

  sem_post(&pQInfo->dataReady);
  vnodeDecRefCount(pQInfo);
}
