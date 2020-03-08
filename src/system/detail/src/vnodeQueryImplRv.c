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

enum {
  TS_JOIN_TS_EQUAL = 0,
  TS_JOIN_TS_NOT_EQUALS = 1,
  TS_JOIN_TAG_NOT_EQUALS = 2,
};

static int32_t doMergeMetersResultsToGroupRes(STableQuerySupportObj *pSupporter, SQuery *pQuery,
                                              SQueryRuntimeEnv *pRuntimeEnv, SMeterDataInfo *pMeterDataInfo,
                                              int32_t start, int32_t end);

static void setWindowResOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pResult);

static void    resetMergeResultBuf(SQuery *pQuery, SQLFunctionCtx *pCtx, SResultInfo *pResultInfo);
static int32_t flushFromResultBuf(STableQuerySupportObj *pSupporter, const SQuery *pQuery,
                                  const SQueryRuntimeEnv *pRuntimeEnv);
static bool    functionNeedToExecute(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx, int32_t functionId);
static void    getNextTimeWindow(SQuery *pQuery, STimeWindow *pTimeWindow);

static int32_t getGroupResultId(int32_t groupIndex) {
  int32_t base = 200000;
  return base + (groupIndex * 10000);
}

bool isGroupbyNormalCol(SSqlGroupbyExpr *pGroupbyExpr) {
  if (pGroupbyExpr == NULL || pGroupbyExpr->numOfGroupCols == 0) {
    return false;
  }

  for (int32_t i = 0; i < pGroupbyExpr->numOfGroupCols; ++i) {
    SColIndexEx *pColIndex = &pGroupbyExpr->columnInfo[i];
    if (pColIndex->flag == TSDB_COL_NORMAL) {
      /*
       * make sure the normal column locates at the second position if tbname exists in group by clause
       */
      if (pGroupbyExpr->numOfGroupCols > 1) {
        assert(pColIndex->colIdx > 0);
      }

      return true;
    }
  }

  return false;
}

int16_t getGroupbyColumnType(SQuery *pQuery, SSqlGroupbyExpr *pGroupbyExpr) {
  assert(pGroupbyExpr != NULL);

  int32_t colId = -2;
  int16_t type = TSDB_DATA_TYPE_NULL;

  for (int32_t i = 0; i < pGroupbyExpr->numOfGroupCols; ++i) {
    SColIndexEx *pColIndex = &pGroupbyExpr->columnInfo[i];
    if (pColIndex->flag == TSDB_COL_NORMAL) {
      colId = pColIndex->colId;
      break;
    }
  }

  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    if (colId == pQuery->colList[i].data.colId) {
      type = pQuery->colList[i].data.type;
      break;
    }
  }

  return type;
}

bool isSelectivityWithTagsQuery(SQuery *pQuery) {
  bool    hasTags = false;
  int32_t numOfSelectivity = 0;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functId == TSDB_FUNC_TAG_DUMMY || functId == TSDB_FUNC_TS_DUMMY) {
      hasTags = true;
      continue;
    }

    if ((aAggs[functId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
      numOfSelectivity++;
    }
  }

  if (numOfSelectivity > 0 && hasTags) {
    return true;
  }

  return false;
}

bool doRevisedResultsByLimit(SQInfo *pQInfo) {
  SQuery *pQuery = &pQInfo->query;

  if ((pQuery->limit.limit > 0) && (pQuery->pointsRead + pQInfo->pointsRead > pQuery->limit.limit)) {
    pQuery->pointsRead = pQuery->limit.limit - pQInfo->pointsRead;

    setQueryStatus(pQuery, QUERY_COMPLETED);  // query completed
    return true;
  }

  return false;
}

static void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void *inputData,
                          char *primaryColumnData, int32_t size, int32_t functionId, SDataStatis *pStatis, bool hasNull,
                          void *param, int32_t scanFlag);

void createQueryResultInfo(SQuery *pQuery, SWindowResult *pResultRow, bool isSTableQuery, SPosInfo *posInfo);

/**
 *
 * @param pQuery
 * @param pDataBlockInfo
 * @param forwardStep
 * @return  TRUE means query not completed, FALSE means query is completed
 */
static bool queryPausedInCurrentBlock(SQuery *pQuery, SDataBlockInfo *pDataBlockInfo, int32_t forwardStep) {
  // current query completed
  //  if ((pQuery->lastKey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
  //      (pQuery->lastKey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
  //    setQueryStatus(pQuery, QUERY_COMPLETED);
  //    return true;
  //  }

  // output buffer is full, pause current query
  if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {
    assert((QUERY_IS_ASC_QUERY(pQuery) && forwardStep + pQuery->pos <= pDataBlockInfo->size) ||
           (!QUERY_IS_ASC_QUERY(pQuery) && pQuery->pos - forwardStep + 1 >= 0));

    return true;
  }

  if (Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED)) {
    return true;
  }
  
  return false;
}

static SField *getFieldInfo(SQuery *pQuery, SBlockInfo *pDataBlockInfo, SField *pFields, int32_t column) {
  // no SField info exist, or column index larger than the output column, no result.
  if (pFields == NULL || column >= pQuery->numOfOutputCols) {
    return NULL;
  }

  SColIndexEx *pColIndexEx = &pQuery->pSelectExpr[column].pBase.colInfo;

  // for a tag column, no corresponding field info
  if (TSDB_COL_IS_TAG(pColIndexEx->flag)) {
    return NULL;
  }

  /*
   * Choose the right column field info by field id, since the file block may be out of date,
   * which means the newest table schema is not equalled to the schema of this block.
   */
  for (int32_t i = 0; i < pDataBlockInfo->numOfCols; ++i) {
    if (pColIndexEx->colId == pFields[i].colId) {
      return &pFields[i];
    }
  }

  return NULL;
}

/*
 * not null data in two cases:
 * 1. tags data: isTag == true;
 * 2. data locate in file, numOfNullPoints == 0 or pFields does not needed to be loaded
 */
static bool hasNullVal(SQuery *pQuery, int32_t col, SBlockInfo *pDataBlockInfo, SField *pFields) {
  bool ret = true;

  if (TSDB_COL_IS_TAG(pQuery->pSelectExpr[col].pBase.colInfo.flag)) {
    ret = false;
  } else {
    if (pFields == NULL) {
      ret = false;
    } else {
      SField *pField = getFieldInfo(pQuery, pDataBlockInfo, pFields, col);
      if (pField != NULL && pField->numOfNullPoints == 0) {
        ret = false;
      }
    }
  }

  return ret;
}

static SDataStatis* getStatisInfo(SQuery* pQuery, SDataStatis* pStatis, SDataBlockInfo *pDataBlockInfo, int32_t columnIndex) {
  // no SField info exist, or column index larger than the output column, no result.
  if (pStatis == NULL) {
    return NULL;
  }
  
  SColIndexEx *pColIndexEx = &pQuery->pSelectExpr[columnIndex].pBase.colInfo;
  
  // for a tag column, no corresponding field info
  if (TSDB_COL_IS_TAG(pColIndexEx->flag)) {
    return NULL;
  }
  
  /*
   * Choose the right column field info by field id, since the file block may be out of date,
   * which means the newest table schema is not equalled to the schema of this block.
   */
  for (int32_t i = 0; i < pDataBlockInfo->numOfCols; ++i) {
    if (pColIndexEx->colId == pStatis[i].colId) {
      return &pStatis[i];
    }
  }
  
  return NULL;
}

static bool hasNullVal_(SQuery *pQuery, int32_t col, SDataBlockInfo *pDataBlockInfo, SDataStatis *pStatis, SDataStatis** pColStatis) {
  if (TSDB_COL_IS_TAG(pQuery->pSelectExpr[col].pBase.colInfo.flag) || pStatis == NULL) {
    return false;
  }
  
  *pColStatis = getStatisInfo(pQuery, pStatis, pDataBlockInfo, col);
  if ((*pColStatis) != NULL && (*pColStatis)->numOfNullPoints == 0) {
    return false;
  }
  
  return true;
}

static SWindowResult *doSetTimeWindowFromKey(SQueryRuntimeEnv *pRuntimeEnv, SWindowResInfo *pWindowResInfo, char *pData,
                                             int16_t bytes) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int32_t *p1 = (int32_t *)taosHashGet(pWindowResInfo->hashList, pData, bytes);
  if (p1 != NULL) {
    pWindowResInfo->curIndex = *p1;
  } else {  // more than the capacity, reallocate the resources
    if (pWindowResInfo->size >= pWindowResInfo->capacity) {
      int64_t newCap = pWindowResInfo->capacity * 2;

      char *t = realloc(pWindowResInfo->pResult, newCap * sizeof(SWindowResult));
      if (t != NULL) {
        pWindowResInfo->pResult = (SWindowResult *)t;
        memset(&pWindowResInfo->pResult[pWindowResInfo->capacity], 0, sizeof(SWindowResult) * pWindowResInfo->capacity);
      } else {
        // todo
      }

      for (int32_t i = pWindowResInfo->capacity; i < newCap; ++i) {
        SPosInfo pos = {-1, -1};
        createQueryResultInfo(pQuery, &pWindowResInfo->pResult[i], pRuntimeEnv->stableQuery, &pos);
      }

      pWindowResInfo->capacity = newCap;
    }

    // add a new result set for a new group
    pWindowResInfo->curIndex = pWindowResInfo->size++;
    taosHashAdd(pWindowResInfo->hashList, pData, bytes, (char *)&pWindowResInfo->curIndex, sizeof(int32_t));
  }

  return getWindowResult(pWindowResInfo, pWindowResInfo->curIndex);
}

// get the correct time window according to the handled timestamp
static STimeWindow getActiveTimeWindow(SWindowResInfo *pWindowResInfo, int64_t ts, SQuery *pQuery) {
  STimeWindow w = {0};

  if (pWindowResInfo->curIndex == -1) {  // the first window, from the previous stored value
    w.skey = pWindowResInfo->prevSKey;
    w.ekey = w.skey + pQuery->intervalTime - 1;
  } else {
    int32_t slot = curTimeWindow(pWindowResInfo);
    w = getWindowResult(pWindowResInfo, slot)->window;
  }

  if (w.skey > ts || w.ekey < ts) {
    int64_t st = w.skey;

    if (st > ts) {
      st -= ((st - ts + pQuery->slidingTime - 1) / pQuery->slidingTime) * pQuery->slidingTime;
    }

    int64_t et = st + pQuery->intervalTime - 1;
    if (et < ts) {
      st += ((ts - et + pQuery->slidingTime - 1) / pQuery->slidingTime) * pQuery->slidingTime;
    }

    w.skey = st;
    w.ekey = w.skey + pQuery->intervalTime - 1;
  }

  /*
   * query border check, skey should not be bounded by the query time range, since the value skey will
   * be used as the time window index value. So we only change ekey of time window accordingly.
   */
  if (w.ekey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) {
    w.ekey = pQuery->ekey;
  }

  assert(ts >= w.skey && ts <= w.ekey && w.skey != 0);

  return w;
}

static int32_t addNewWindowResultBuf(SWindowResult *pWindowRes, SQueryDiskbasedResultBuf *pResultBuf, int32_t sid,
                                     int32_t numOfRowsPerPage) {
  if (pWindowRes->pos.pageId != -1) {
    return 0;
  }

  tFilePage *pData = NULL;

  // in the first scan, new space needed for results
  int32_t pageId = -1;
  SIDList list = getDataBufPagesIdList(pResultBuf, sid);

  if (list.size == 0) {
    pData = getNewDataBuf(pResultBuf, sid, &pageId);
  } else {
    pageId = getLastPageId(&list);
    pData = getResultBufferPageById(pResultBuf, pageId);

    if (pData->numOfElems >= numOfRowsPerPage) {
      pData = getNewDataBuf(pResultBuf, sid, &pageId);
      if (pData != NULL) {
        assert(pData->numOfElems == 0);  // number of elements must be 0 for new allocated buffer
      }
    }
  }

  if (pData == NULL) {
    return -1;
  }

  // set the number of rows in current disk page
  if (pWindowRes->pos.pageId == -1) {  // not allocated yet, allocate new buffer
    pWindowRes->pos.pageId = pageId;
    pWindowRes->pos.rowId = pData->numOfElems++;
  }

  return 0;
}

static int32_t setWindowOutputBufByKey(SQueryRuntimeEnv *pRuntimeEnv, SWindowResInfo *pWindowResInfo, int32_t sid,
                                       STimeWindow *win) {
  assert(win->skey <= win->ekey);
  SQueryDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, pWindowResInfo, (char *)&win->skey, TSDB_KEYSIZE);
  if (pWindowRes == NULL) {
    return -1;
  }

  // not assign result buffer yet, add new result buffer
  if (pWindowRes->pos.pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pWindowRes, pResultBuf, sid, pRuntimeEnv->numOfRowsPerPage);
    if (ret != 0) {
      return -1;
    }
  }

  // set time window for current result
  pWindowRes->window = *win;

  setWindowResOutputBuf(pRuntimeEnv, pWindowRes);
  initCtxOutputBuf(pRuntimeEnv);

  return TSDB_CODE_SUCCESS;
}

static SWindowStatus *getTimeWindowResStatus(SWindowResInfo *pWindowResInfo, int32_t slot) {
  assert(slot >= 0 && slot < pWindowResInfo->size);
  return &pWindowResInfo->pResult[slot].status;
}

static int32_t getForwardStepsInBlock(int32_t numOfPoints, __block_search_fn_t searchFn, TSKEY ekey, int16_t pos,
                                      int16_t order, int64_t *pData) {
  int32_t endPos = searchFn((char *)pData, numOfPoints, ekey, order);
  int32_t forwardStep = 0;

  if (endPos >= 0) {
    forwardStep = (order == TSQL_SO_ASC) ? (endPos - pos) : (pos - endPos);
    assert(forwardStep >= 0);

    // endPos data is equalled to the key so, we do need to read the element in endPos
    if (pData[endPos] == ekey) {
      forwardStep += 1;
    }
  }

  return forwardStep;
}

/**
 * NOTE: the query status only set for the first scan of master scan.
 */
static void doCheckQueryCompleted(SQueryRuntimeEnv *pRuntimeEnv, TSKEY lastKey, SWindowResInfo *pWindowResInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (pRuntimeEnv->scanFlag != MASTER_SCAN || (!isIntervalQuery(pQuery))) {
    return;
  }

  // no qualified results exist, abort check
  if (pWindowResInfo->size == 0) {
    return;
  }

  // query completed
  if ((lastKey >= pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
      (lastKey <= pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
    closeAllTimeWindow(pWindowResInfo);

    pWindowResInfo->curIndex = pWindowResInfo->size - 1;
    setQueryStatus(pQuery, QUERY_COMPLETED | QUERY_RESBUF_FULL);
  } else {  // set the current index to be the last unclosed window
    int32_t i = 0;
    int64_t skey = 0;

    for (i = 0; i < pWindowResInfo->size; ++i) {
      SWindowResult *pResult = &pWindowResInfo->pResult[i];
      if (pResult->status.closed) {
        continue;
      }

      if ((pResult->window.ekey <= lastKey && QUERY_IS_ASC_QUERY(pQuery)) ||
          (pResult->window.skey >= lastKey && !QUERY_IS_ASC_QUERY(pQuery))) {
        closeTimeWindow(pWindowResInfo, i);
      } else {
        skey = pResult->window.skey;
        break;
      }
    }

    // all windows are closed, set the last one to be the skey
    if (skey == 0) {
      assert(i == pWindowResInfo->size);
      pWindowResInfo->curIndex = pWindowResInfo->size - 1;
    } else {
      pWindowResInfo->curIndex = i;
    }

    pWindowResInfo->prevSKey = pWindowResInfo->pResult[pWindowResInfo->curIndex].window.skey;

    // the number of completed slots are larger than the threshold, dump to client immediately.
    int32_t n = numOfClosedTimeWindow(pWindowResInfo);
    if (n > pWindowResInfo->threshold) {
      setQueryStatus(pQuery, QUERY_RESBUF_FULL);
    }

    dTrace("QInfo:%p total window:%d, closed:%d", GET_QINFO_ADDR(pQuery), pWindowResInfo->size, n);
  }

  assert(pWindowResInfo->prevSKey != 0);
}

static int32_t getNumOfRowsInTimeWindow(SQuery *pQuery, SDataBlockInfo *pDataBlockInfo, TSKEY *pPrimaryColumn,
                                        int32_t startPos, TSKEY ekey, __block_search_fn_t searchFn,
                                        bool updateLastKey) {
  assert(startPos >= 0 && startPos < pDataBlockInfo->size);

  int32_t num = -1;
  int32_t order = pQuery->order.order;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    if (ekey < pDataBlockInfo->window.ekey) {
      if (ekey == 1583213579999) {
        int32_t k = 1;
      }
      num = getForwardStepsInBlock(pDataBlockInfo->size, searchFn, ekey, startPos, order, pPrimaryColumn);
      if (num == 0) {  // no qualified data in current block, do not update the lastKey value
        assert(ekey < pPrimaryColumn[startPos]);
      } else {
        if (updateLastKey) {
          pQuery->lastKey = pPrimaryColumn[startPos + (num - 1)] + step;
        }
      }
    } else {
      num = pDataBlockInfo->size - startPos;
      if (updateLastKey) {
        pQuery->lastKey = pDataBlockInfo->window.ekey + step;
      }
    }
  } else {  // desc
    if (ekey > pDataBlockInfo->window.skey) {
      num = getForwardStepsInBlock(pDataBlockInfo->size, searchFn, ekey, startPos, order, pPrimaryColumn);
      if (num == 0) {  // no qualified data in current block, do not update the lastKey value
        assert(ekey > pPrimaryColumn[startPos]);
      } else {
        if (updateLastKey) {
          pQuery->lastKey = pPrimaryColumn[startPos - (num - 1)] + step;
        }
      }
    } else {
      num = startPos + 1;
      if (updateLastKey) {
        pQuery->lastKey = pDataBlockInfo->window.skey + step;
      }
    }
  }

  assert(num >= 0);
  return num;
}

static void doBlockwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SWindowStatus *pStatus, STimeWindow *pWin,
                                      int32_t startPos, int32_t forwardStep) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  if (IS_MASTER_SCAN(pRuntimeEnv) || pStatus->closed) {
    for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
      int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;

      pCtx[k].nStartQueryTimestamp = pWin->skey;
      pCtx[k].size = forwardStep;
      pCtx[k].startOffset = (QUERY_IS_ASC_QUERY(pQuery)) ? startPos : startPos - (forwardStep - 1);

      if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
        pCtx[k].ptsList = (TSKEY *)((char *)pRuntimeEnv->primaryColBuffer->data + pCtx[k].startOffset * TSDB_KEYSIZE);
      }

      if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
        aAggs[functionId].xFunction(&pCtx[k]);
      }
    }
  }
}

static void doRowwiseApplyFunctions(SQueryRuntimeEnv *pRuntimeEnv, SWindowStatus *pStatus, STimeWindow *pWin,
                                    int32_t offset) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  if (IS_MASTER_SCAN(pRuntimeEnv) || pStatus->closed) {
    for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
      pCtx[k].nStartQueryTimestamp = pWin->skey;

      int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;
      if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
        aAggs[functionId].xFunctionF(&pCtx[k], offset);
      }
    }
  }
}

static int32_t getNextQualifiedWindow(SQueryRuntimeEnv *pRuntimeEnv, STimeWindow *pNextWin,
                                      SWindowResInfo *pWindowResInfo, SDataBlockInfo *pDataBlockInfo, TSKEY *primaryKeys,
                                      __block_search_fn_t searchFn) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  while (1) {
    getNextTimeWindow(pQuery, pNextWin);

    if (pWindowResInfo->startTime > pNextWin->skey || (pNextWin->skey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
        (pNextWin->ekey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
      return -1;
    }

    // next time window is not in current block
    if ((pNextWin->skey > pDataBlockInfo->window.ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
        (pNextWin->ekey < pDataBlockInfo->window.skey && !QUERY_IS_ASC_QUERY(pQuery))) {
      return -1;
    }

    TSKEY startKey = -1;
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      startKey = pNextWin->skey;
      if (startKey < pQuery->skey) {
        startKey = pQuery->skey;
      }
    } else {
      startKey = pNextWin->ekey;
      if (startKey > pQuery->skey) {
        startKey = pQuery->skey;
      }
    }

    int32_t startPos = searchFn((char *)primaryKeys, pDataBlockInfo->size, startKey, pQuery->order.order);

    /*
     * This time window does not cover any data, try next time window,
     * this case may happen when the time window is too small
     */
    if ((primaryKeys[startPos] > pNextWin->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
        (primaryKeys[startPos] < pNextWin->skey && !QUERY_IS_ASC_QUERY(pQuery))) {
      continue;
    }

    //    if (pNextWin->ekey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) {
    //      pNextWin->ekey = pQuery->ekey;
    //    }
    //    if (pNextWin->skey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery)) {
    //      pNextWin->skey = pQuery->ekey;
    //    }

    return startPos;
  }
}

static TSKEY reviseWindowEkey(SQuery *pQuery, STimeWindow *pWindow) {
  TSKEY ekey = -1;
  if (QUERY_IS_ASC_QUERY(pQuery)) {
    ekey = pWindow->ekey;
    if (ekey > pQuery->ekey) {
      ekey = pQuery->ekey;
    }
  } else {
    ekey = pWindow->skey;
    if (ekey < pQuery->ekey) {
      ekey = pQuery->ekey;
    }
  }

  return ekey;
}

/**
 *
 * @param pRuntimeEnv
 * @param forwardStep
 * @param primaryKeyCol
 * @param pFields
 * @param isDiskFileBlock
 * @return                  the incremental number of output value, so it maybe 0 for fixed number of query,
 *                          such as count/min/max etc.
 */
static int32_t blockwiseApplyAllFunctions(SQueryRuntimeEnv *pRuntimeEnv, int32_t forwardStep, SDataStatis *pStatis,
                                          SDataBlockInfo *pDataBlockInfo, SWindowResInfo *pWindowResInfo,
                                          __block_search_fn_t searchFn, SArray* pDataBlock) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  
  SColumnInfoEx_ *pColInfo = NULL;
  TSKEY* primaryKeyCol = NULL;
  
  if (pDataBlock != NULL) {
    pColInfo = taosArrayGet(pDataBlock, 0);
    primaryKeyCol = (TSKEY*) (pColInfo->pData->data);
  }
  
  pQuery->pos = 0;
  int64_t prevNumOfRes = getNumOfResult(pRuntimeEnv);

  SArithmeticSupport *sasArray = calloc((size_t)pQuery->numOfOutputCols, sizeof(SArithmeticSupport));

  for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;

    SDataStatis* tpField = NULL;
    bool  hasNull = hasNullVal_(pQuery, k, pDataBlockInfo, pStatis, &tpField);
    char *dataBlock = getDataBlocks_(pRuntimeEnv, &sasArray[k], k, forwardStep, pDataBlock);
    
    setExecParams(pQuery, &pCtx[k], dataBlock, (char *)primaryKeyCol, forwardStep, functionId, tpField,
                  hasNull, &sasArray[k], pRuntimeEnv->scanFlag);
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  if (isIntervalQuery(pQuery)) {
    int32_t offset = GET_COL_DATA_POS(pQuery, 0, step);
    TSKEY   ts = primaryKeyCol[offset];

    STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);
    if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pRuntimeEnv->pMeterObj->sid, &win) != TSDB_CODE_SUCCESS) {
      return 0;
    }

    TSKEY ekey = reviseWindowEkey(pQuery, &win);
    forwardStep = getNumOfRowsInTimeWindow(pQuery, pDataBlockInfo, primaryKeyCol, pQuery->pos, ekey, searchFn, true);

    SWindowStatus *pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
    doBlockwiseApplyFunctions(pRuntimeEnv, pStatus, &win, pQuery->pos, forwardStep);

    int32_t     index = pWindowResInfo->curIndex;
    STimeWindow nextWin = win;

    while (1) {
      int32_t startPos =
          getNextQualifiedWindow(pRuntimeEnv, &nextWin, pWindowResInfo, pDataBlockInfo, primaryKeyCol, searchFn);
      if (startPos < 0) {
        break;
      }

      // null data, failed to allocate more memory buffer
      int32_t sid = pRuntimeEnv->pMeterObj->sid;
      if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, sid, &nextWin) != TSDB_CODE_SUCCESS) {
        break;
      }

      ekey = reviseWindowEkey(pQuery, &nextWin);
      forwardStep = getNumOfRowsInTimeWindow(pQuery, pDataBlockInfo, primaryKeyCol, startPos, ekey, searchFn, true);

      pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
      doBlockwiseApplyFunctions(pRuntimeEnv, pStatus, &nextWin, startPos, forwardStep);
    }

    pWindowResInfo->curIndex = index;
  } else {
    /*
     * the sqlfunctionCtx parameters should be set done before all functions are invoked,
     * since the selectivity + tag_prj query needs all parameters been set done.
     * tag_prj function are changed to be TSDB_FUNC_TAG_DUMMY
     */
    for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
      int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;
      if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
        aAggs[functionId].xFunction(&pCtx[k]);
      }
    }
  }

  /*
   * No need to calculate the number of output results for group-by normal columns, interval query
   * because the results of group by normal column is put into intermediate buffer.
   */
  int32_t num = 0;
  if (!isIntervalQuery(pQuery)) {
    num = getNumOfResult(pRuntimeEnv) - prevNumOfRes;
  }

  tfree(sasArray);
  return (int32_t)num;
}

static int32_t setGroupResultOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, char *pData, int16_t type, int16_t bytes) {
  if (isNull(pData, type)) {  // ignore the null value
    return -1;
  }

  int32_t GROUPRESULTID = 1;

  SQueryDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, &pRuntimeEnv->windowResInfo, pData, bytes);
  if (pWindowRes == NULL) {
    return -1;
  }

  // not assign result buffer yet, add new result buffer
  if (pWindowRes->pos.pageId == -1) {
    int32_t ret = addNewWindowResultBuf(pWindowRes, pResultBuf, GROUPRESULTID, pRuntimeEnv->numOfRowsPerPage);
    if (ret != 0) {
      return -1;
    }
  }

  setWindowResOutputBuf(pRuntimeEnv, pWindowRes);
  initCtxOutputBuf(pRuntimeEnv);
  return TSDB_CODE_SUCCESS;
}

static char *getGroupbyColumnData(SQuery *pQuery, SData **data, int16_t *type, int16_t *bytes) {
  char *groupbyColumnData = NULL;

  SSqlGroupbyExpr *pGroupbyExpr = pQuery->pGroupbyExpr;

  for (int32_t k = 0; k < pGroupbyExpr->numOfGroupCols; ++k) {
    if (pGroupbyExpr->columnInfo[k].flag == TSDB_COL_TAG) {
      continue;
    }

    int16_t colIndex = -1;
    int32_t colId = pGroupbyExpr->columnInfo[k].colId;

    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      if (pQuery->colList[i].data.colId == colId) {
        colIndex = i;
        break;
      }
    }

    assert(colIndex >= 0 && colIndex < pQuery->numOfCols);

    *type = pQuery->colList[colIndex].data.type;
    *bytes = pQuery->colList[colIndex].data.bytes;

    groupbyColumnData = doGetDataBlocks(pQuery, data, pQuery->colList[colIndex].colIdxInBuf);
    break;
  }

  return groupbyColumnData;
}

static int32_t doTSJoinFilter(SQueryRuntimeEnv *pRuntimeEnv, int32_t offset) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  STSElem         elem = tsBufGetElem(pRuntimeEnv->pTSBuf);
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  // compare tag first
  if (pCtx[0].tag.i64Key != elem.tag) {
    return TS_JOIN_TAG_NOT_EQUALS;
  }

  TSKEY key = *(TSKEY *)(pCtx[0].aInputElemBuf + TSDB_KEYSIZE * offset);

#if defined(_DEBUG_VIEW)
  printf("elem in comp ts file:%" PRId64 ", key:%" PRId64
         ", tag:%d, id:%s, query order:%d, ts order:%d, traverse:%d, index:%d\n",
         elem.ts, key, elem.tag, pRuntimeEnv->pMeterObj->meterId, pQuery->order.order, pRuntimeEnv->pTSBuf->tsOrder,
         pRuntimeEnv->pTSBuf->cur.order, pRuntimeEnv->pTSBuf->cur.tsIndex);
#endif

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    if (key < elem.ts) {
      return TS_JOIN_TS_NOT_EQUALS;
    } else if (key > elem.ts) {
      assert(false);
    }
  } else {
    if (key > elem.ts) {
      return TS_JOIN_TS_NOT_EQUALS;
    } else if (key < elem.ts) {
      assert(false);
    }
  }

  return TS_JOIN_TS_EQUAL;
}

static bool functionNeedToExecute(SQueryRuntimeEnv *pRuntimeEnv, SQLFunctionCtx *pCtx, int32_t functionId) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);

  if (pResInfo->complete || functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TS_DUMMY) {
    return false;
  }

  // in the supplementary scan, only the following functions need to be executed
  if (IS_SUPPLEMENT_SCAN(pRuntimeEnv) &&
      !(functionId == TSDB_FUNC_LAST_DST || functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_FIRST ||
        functionId == TSDB_FUNC_LAST || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TS)) {
    return false;
  }

  return true;
}

static int32_t rowwiseApplyAllFunctions(SQueryRuntimeEnv *pRuntimeEnv, int32_t *forwardStep, SField *pFields,
                                        SBlockInfo *pDataBlockInfo, SWindowResInfo *pWindowResInfo) {
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  TSKEY *         primaryKeyCol = (TSKEY *)pRuntimeEnv->primaryColBuffer->data;

  bool    isDiskFileBlock = IS_FILE_BLOCK(pRuntimeEnv->blockStatus);
  SData **data = pRuntimeEnv->colDataBuffer;

  int64_t prevNumOfRes = 0;
  bool    groupbyStateValue = isGroupbyNormalCol(pQuery->pGroupbyExpr);

  if (!groupbyStateValue) {
    prevNumOfRes = getNumOfResult(pRuntimeEnv);
  }

  SArithmeticSupport *sasArray = calloc((size_t)pQuery->numOfOutputCols, sizeof(SArithmeticSupport));

  int16_t type = 0;
  int16_t bytes = 0;

  char *groupbyColumnData = NULL;
  if (groupbyStateValue) {
    groupbyColumnData = getGroupbyColumnData(pQuery, data, &type, &bytes);
  }

  for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;

    bool  hasNull = hasNullVal(pQuery, k, pDataBlockInfo, pFields);
    char *dataBlock = getDataBlocks(pRuntimeEnv, &sasArray[k], k, *forwardStep);

    TSKEY ts = pQuery->skey;  // QUERY_IS_ASC_QUERY(pQuery) ? pRuntimeEnv->intervalWindow.skey :
    setExecParams(pQuery, &pCtx[k], dataBlock, (char *)primaryKeyCol, (*forwardStep), functionId, pFields, hasNull,
                  &sasArray[k], pRuntimeEnv->scanFlag);
  }

  // set the input column data
  for (int32_t k = 0; k < pQuery->numOfFilterCols; ++k) {
    SSingleColumnFilterInfo *pFilterInfo = &pQuery->pFilterInfo[k];
    /*
     * NOTE: here the tbname/tags column cannot reach here, since it will never be a filter column,
     * so we do NOT check if is a tag or not
     */
    pFilterInfo->pData = doGetDataBlocks(pQuery, data, pFilterInfo->info.colIdxInBuf);
  }

  int32_t numOfRes = 0;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  // from top to bottom in desc
  // from bottom to top in asc order
  if (pRuntimeEnv->pTSBuf != NULL) {
    SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);
    qTrace("QInfo:%p process data rows, numOfRows:%d, query order:%d, ts comp order:%d", pQInfo, *forwardStep,
           pQuery->order.order, pRuntimeEnv->pTSBuf->cur.order);
  }

  int32_t j = 0;
  TSKEY   lastKey = -1;

  for (j = 0; j < (*forwardStep); ++j) {
    int32_t offset = GET_COL_DATA_POS(pQuery, j, step);

    if (pRuntimeEnv->pTSBuf != NULL) {
      int32_t r = doTSJoinFilter(pRuntimeEnv, offset);
      if (r == TS_JOIN_TAG_NOT_EQUALS) {
        break;
      } else if (r == TS_JOIN_TS_NOT_EQUALS) {
        continue;
      } else {
        assert(r == TS_JOIN_TS_EQUAL);
      }
    }

    if (pQuery->numOfFilterCols > 0 && (!vnodeDoFilterData(pQuery, offset))) {
      continue;
    }

    // interval window query
    if (isIntervalQuery(pQuery)) {
      // decide the time window according to the primary timestamp
      int64_t     ts = primaryKeyCol[offset];
      STimeWindow win = getActiveTimeWindow(pWindowResInfo, ts, pQuery);

      int32_t ret = setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, pRuntimeEnv->pMeterObj->sid, &win);
      if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
        continue;
      }

      // all startOffset are identical
      offset -= pCtx[0].startOffset;

      SWindowStatus *pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
      doRowwiseApplyFunctions(pRuntimeEnv, pStatus, &win, offset);

      lastKey = ts;
      STimeWindow nextWin = win;
      int32_t     index = pWindowResInfo->curIndex;
      int32_t     sid = pRuntimeEnv->pMeterObj->sid;

      while (1) {
        getNextTimeWindow(pQuery, &nextWin);
        if (pWindowResInfo->startTime > nextWin.skey || (nextWin.skey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
            (nextWin.skey > pQuery->skey && !QUERY_IS_ASC_QUERY(pQuery))) {
          break;
        }

        if (ts < nextWin.skey || ts > nextWin.ekey) {
          break;
        }

        // null data, failed to allocate more memory buffer
        if (setWindowOutputBufByKey(pRuntimeEnv, pWindowResInfo, sid, &nextWin) != TSDB_CODE_SUCCESS) {
          break;
        }

        pStatus = getTimeWindowResStatus(pWindowResInfo, curTimeWindow(pWindowResInfo));
        doRowwiseApplyFunctions(pRuntimeEnv, pStatus, &nextWin, offset);
      }

      pWindowResInfo->curIndex = index;
    } else {  // other queries
      // decide which group this rows belongs to according to current state value
      if (groupbyStateValue) {
        char *stateVal = groupbyColumnData + bytes * offset;

        int32_t ret = setGroupResultOutputBuf(pRuntimeEnv, stateVal, type, bytes);
        if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
          continue;
        }
      }

      // update the lastKey
      lastKey = primaryKeyCol[offset];

      // all startOffset are identical
      offset -= pCtx[0].startOffset;

      for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
        int32_t functionId = pQuery->pSelectExpr[k].pBase.functionId;
        if (functionNeedToExecute(pRuntimeEnv, &pCtx[k], functionId)) {
          aAggs[functionId].xFunctionF(&pCtx[k], offset);
        }
      }
    }

    if (pRuntimeEnv->pTSBuf != NULL) {
      // if timestamp filter list is empty, quit current query
      if (!tsBufNextPos(pRuntimeEnv->pTSBuf)) {
        setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
        break;
      }
    }

    /*
     * pointsOffset is the maximum available space in result buffer update the actual forward step for query that
     * requires checking buffer during loop
     */
    if ((pQuery->checkBufferInLoop == 1) && (++numOfRes) >= pQuery->pointsOffset) {
      pQuery->lastKey = lastKey + step;
      *forwardStep = j + 1;
      break;
    }
  }

  free(sasArray);

  /*
   * No need to calculate the number of output results for group-by normal columns, interval query
   * because the results of group by normal column is put into intermediate buffer.
   */
  int32_t num = 0;
  if (!groupbyStateValue && !isIntervalQuery(pQuery)) {
    num = getNumOfResult(pRuntimeEnv) - prevNumOfRes;
  }

  return num;
}

static int32_t reviseForwardSteps(SQueryRuntimeEnv *pRuntimeEnv, int32_t forwardStep) {
  /*
   * 1. If value filter exists, we try all data in current block, and do not set the QUERY_RESBUF_FULL flag.
   *
   * 2. In case of top/bottom/ts_comp query, the checkBufferInLoop == 1 and pQuery->numOfFilterCols
   * may be 0 or not. We do not check the capacity of output buffer, since the filter function will do it.
   *
   * 3. In handling the query of secondary query of join, tsBuf servers as a ts filter.
   */
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (isTopBottomQuery(pQuery) || isTSCompQuery(pQuery) || pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) {
    return forwardStep;
  }

  // current buffer does not have enough space, try in the next loop
  if ((pQuery->checkBufferInLoop == 1) && (pQuery->pointsOffset <= forwardStep)) {
    forwardStep = pQuery->pointsOffset;
  }

  return forwardStep;
}

static int32_t tableApplyFunctionsOnBlock(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pDataBlockInfo,
                                          SDataStatis *pStatis, __block_search_fn_t searchFn, int32_t *numOfRes,
                                          SWindowResInfo *pWindowResInfo, SArray* pDataBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  pQuery->lastKey = pDataBlockInfo->window.ekey + step;

  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL || isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    //    *numOfRes = rowwiseApplyAllFunctions(pRuntimeEnv, &newForwardStep, pFields, pDataBlockInfo, pWindowResInfo);
  } else {
    *numOfRes = blockwiseApplyAllFunctions(pRuntimeEnv, pDataBlockInfo->size, pStatis, pDataBlockInfo, pWindowResInfo,
                                           searchFn, pDataBlock);
  }

  TSKEY lastKey = (QUERY_IS_ASC_QUERY(pQuery)) ? pDataBlockInfo->window.ekey : pDataBlockInfo->window.skey;
  doCheckQueryCompleted(pRuntimeEnv, lastKey, pWindowResInfo);  // todo refactor merge

  // interval query with limit applied
  if (isIntervalQuery(pQuery) && pQuery->limit.limit > 0 &&
      (pQuery->limit.limit + pQuery->limit.offset) <= numOfClosedTimeWindow(pWindowResInfo) &&
      pRuntimeEnv->scanFlag == MASTER_SCAN) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
  }

  assert(*numOfRes >= 0);

  // check if buffer is large enough for accommodating all qualified points
  if (*numOfRes > 0 && pQuery->checkBufferInLoop == 1) {
    pQuery->pointsOffset -= *numOfRes;
    if (pQuery->pointsOffset <= 0) {  // todo return correct numOfRes for ts_comp function
      pQuery->pointsOffset = 0;
      setQueryStatus(pQuery, QUERY_RESBUF_FULL);
    }
  }

  return 0;
}

void setExecParams(SQuery *pQuery, SQLFunctionCtx *pCtx, void *inputData,
                   char *primaryColumnData, int32_t size, int32_t functionId, SDataStatis *pStatis, bool hasNull,
                   void *param, int32_t scanFlag) {
  int32_t startOffset = 0;//(QUERY_IS_ASC_QUERY(pQuery)) ? pQuery->pos : pQuery->pos - (size - 1);
  pCtx->scanFlag = scanFlag;

  pCtx->aInputElemBuf = inputData;
  pCtx->hasNull = hasNull;

  if (pStatis != NULL) {
    pCtx->preAggVals.isSet = true;
    pCtx->preAggVals.minIndex = pStatis->minIndex;
    pCtx->preAggVals.maxIndex = pStatis->maxIndex;
    pCtx->preAggVals.sum = pStatis->sum;
    pCtx->preAggVals.max = pStatis->max;
    pCtx->preAggVals.min = pStatis->min;
    pCtx->preAggVals.numOfNull = pStatis->numOfNullPoints;
  } else {
    pCtx->preAggVals.isSet = false;
  }

  if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0 && (primaryColumnData != NULL)) {
    pCtx->ptsList = (int64_t *)(primaryColumnData + startOffset * TSDB_KEYSIZE);
  }

  if (functionId >= TSDB_FUNC_FIRST_DST && functionId <= TSDB_FUNC_LAST_DST) {
    // last_dist or first_dist function
    // store the first&last timestamp into the intermediate buffer [1], the true
    // value may be null but timestamp will never be null
    pCtx->ptsList = (int64_t *)(primaryColumnData + startOffset * TSDB_KEYSIZE);
  } else if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_TWA ||
             functionId == TSDB_FUNC_DIFF || (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_AVG_IRATE)) {
    /*
     * leastsquares function needs two columns of input, currently, the x value of linear equation is set to
     * timestamp column, and the y-value is the column specified in pQuery->pSelectExpr[i].colIdxInBuffer
     *
     * top/bottom function needs timestamp to indicate when the
     * top/bottom values emerge, so does diff function
     */
    if (functionId == TSDB_FUNC_TWA) {
      STwaInfo *pTWAInfo = GET_RES_INFO(pCtx)->interResultBuf;
      pTWAInfo->SKey = pQuery->skey;
      pTWAInfo->EKey = pQuery->ekey;
    }

    pCtx->ptsList = (int64_t *)(primaryColumnData + startOffset * TSDB_KEYSIZE);

  } else if (functionId == TSDB_FUNC_ARITHM) {
    pCtx->param[1].pz = param;
  }

  pCtx->startOffset = startOffset;
  pCtx->size = size;

#if defined(_DEBUG_VIEW)
  int64_t *tsList = (int64_t *)(primaryColumnData + startOffset * TSDB_KEYSIZE);
  int64_t  s = tsList[0];
  int64_t  e = tsList[size - 1];

//    if (IS_DATA_BLOCK_LOADED(blockStatus)) {
//        dTrace("QInfo:%p query ts:%lld-%lld, offset:%d, rows:%d, bstatus:%d,
//        functId:%d", GET_QINFO_ADDR(pQuery),
//               s, e, startOffset, size, blockStatus, functionId);
//    } else {
//        dTrace("QInfo:%p block not loaded, bstatus:%d",
//        GET_QINFO_ADDR(pQuery), blockStatus);
//    }
#endif
}

// set the output buffer for the selectivity + tag query
static void setCtxTagColumnInfo(SQuery *pQuery, SQLFunctionCtx *pCtx) {
  if (isSelectivityWithTagsQuery(pQuery)) {
    int32_t         num = 0;
    SQLFunctionCtx *p = NULL;

    int16_t tagLen = 0;

    SQLFunctionCtx **pTagCtx = calloc(pQuery->numOfOutputCols, POINTER_BYTES);
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      SSqlFuncExprMsg *pSqlFuncMsg = &pQuery->pSelectExpr[i].pBase;
      if (pSqlFuncMsg->functionId == TSDB_FUNC_TAG_DUMMY || pSqlFuncMsg->functionId == TSDB_FUNC_TS_DUMMY) {
        tagLen += pCtx[i].outputBytes;
        pTagCtx[num++] = &pCtx[i];
      } else if ((aAggs[pSqlFuncMsg->functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
        p = &pCtx[i];
      } else if (pSqlFuncMsg->functionId == TSDB_FUNC_TS || pSqlFuncMsg->functionId == TSDB_FUNC_TAG) {
        // tag function may be the group by tag column
        // ts may be the required primary timestamp column
        continue;
      } else {
        // the column may be the normal column, group by normal_column, the functionId is TSDB_FUNC_PRJ
      }
    }

    p->tagInfo.pTagCtxList = pTagCtx;
    p->tagInfo.numOfTagCols = num;
    p->tagInfo.tagsLen = tagLen;
  }
}

static void setWindowResultInfo(SResultInfo *pResultInfo, SQuery *pQuery, bool isStableQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    setResultInfoBuf(&pResultInfo[i], pQuery->pSelectExpr[i].interResBytes, isStableQuery);
  }
}

static int32_t setupQueryRuntimeEnv(SMeterObj *pMeterObj, SQuery *pQuery, SQueryRuntimeEnv *pRuntimeEnv,
                                    SColumnModel *pTagsSchema, int16_t order, bool isSTableQuery) {
  dTrace("QInfo:%p setup runtime env", GET_QINFO_ADDR(pQuery));

  pRuntimeEnv->pMeterObj = pMeterObj;
  pRuntimeEnv->pQuery = pQuery;

  pRuntimeEnv->resultInfo = calloc(pQuery->numOfOutputCols, sizeof(SResultInfo));
  pRuntimeEnv->pCtx = (SQLFunctionCtx *)calloc(pQuery->numOfOutputCols, sizeof(SQLFunctionCtx));

  if (pRuntimeEnv->resultInfo == NULL || pRuntimeEnv->pCtx == NULL) {
    goto _error_clean;
  }

  pRuntimeEnv->offset[0] = 0;
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SSqlFuncExprMsg *pSqlFuncMsg = &pQuery->pSelectExpr[i].pBase;
    SColIndexEx *    pColIndexEx = &pSqlFuncMsg->colInfo;

    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

    if (TSDB_COL_IS_TAG(pSqlFuncMsg->colInfo.flag)) {  // process tag column info
      SSchema *pSchema = getColumnModelSchema(pTagsSchema, pColIndexEx->colIdx);

      pCtx->inputType = pSchema->type;
      pCtx->inputBytes = pSchema->bytes;
    } else {
      pCtx->inputType = GET_COLUMN_TYPE(pQuery, i);
      pCtx->inputBytes = GET_COLUMN_BYTES(pQuery, i);
    }

    pCtx->ptsOutputBuf = NULL;

    pCtx->outputBytes = pQuery->pSelectExpr[i].resBytes;
    pCtx->outputType = pQuery->pSelectExpr[i].resType;

    pCtx->order = pQuery->order.order;
    pCtx->functionId = pSqlFuncMsg->functionId;

    pCtx->numOfParams = pSqlFuncMsg->numOfParams;
    for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
      int16_t type = pSqlFuncMsg->arg[j].argType;
      int16_t bytes = pSqlFuncMsg->arg[j].argBytes;
      if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
        tVariantCreateFromBinary(&pCtx->param[j], pSqlFuncMsg->arg->argValue.pz, bytes, type);
      } else {
        tVariantCreateFromBinary(&pCtx->param[j], (char *)&pSqlFuncMsg->arg[j].argValue.i64, bytes, type);
      }
    }

    // set the order information for top/bottom query
    int32_t functionId = pCtx->functionId;

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      int32_t f = pQuery->pSelectExpr[0].pBase.functionId;
      assert(f == TSDB_FUNC_TS || f == TSDB_FUNC_TS_DUMMY);

      pCtx->param[2].i64Key = order;
      pCtx->param[2].nType = TSDB_DATA_TYPE_BIGINT;
      pCtx->param[3].i64Key = functionId;
      pCtx->param[3].nType = TSDB_DATA_TYPE_BIGINT;

      pCtx->param[1].i64Key = pQuery->order.orderColId;
    }

    if (i > 0) {
      pRuntimeEnv->offset[i] = pRuntimeEnv->offset[i - 1] + pRuntimeEnv->pCtx[i - 1].outputBytes;
    }
  }

  // set the intermediate result output buffer
  setWindowResultInfo(pRuntimeEnv->resultInfo, pQuery, isSTableQuery);

  // if it is group by normal column, do not set output buffer, the output buffer is pResult
  if (!isGroupbyNormalCol(pQuery->pGroupbyExpr) && !isSTableQuery) {
    resetCtxOutputBuf(pRuntimeEnv);
  }

  setCtxTagColumnInfo(pQuery, pRuntimeEnv->pCtx);

  // for loading block data in memory
  assert(vnodeList[pMeterObj->vnode].cfg.rowsInFileBlock == pMeterObj->pointsPerFileBlock);
  return TSDB_CODE_SUCCESS;

_error_clean:
  tfree(pRuntimeEnv->resultInfo);
  tfree(pRuntimeEnv->pCtx);

  return TSDB_CODE_SERV_OUT_OF_MEMORY;
}

static void teardownQueryRuntimeEnv(SQueryRuntimeEnv *pRuntimeEnv) {
  if (pRuntimeEnv->pQuery == NULL) {
    return;
  }

  SQuery *pQuery = pRuntimeEnv->pQuery;

  dTrace("QInfo:%p teardown runtime env", GET_QINFO_ADDR(pQuery));
  cleanupTimeWindowInfo(&pRuntimeEnv->windowResInfo, pQuery->numOfOutputCols);

  if (pRuntimeEnv->pCtx != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

      for (int32_t j = 0; j < pCtx->numOfParams; ++j) {
        tVariantDestroy(&pCtx->param[j]);
      }

      tVariantDestroy(&pCtx->tag);
      tfree(pCtx->tagInfo.pTagCtxList);
      tfree(pRuntimeEnv->resultInfo[i].interResultBuf);
    }

    tfree(pRuntimeEnv->resultInfo);
    tfree(pRuntimeEnv->pCtx);
  }

  if (pQuery && (!PRIMARY_TSCOL_LOADED(pQuery))) {
    tfree(pRuntimeEnv->primaryColBuffer);
  }

  if (pRuntimeEnv->vnodeFileInfo.pFileInfo != NULL) {
    pRuntimeEnv->vnodeFileInfo.numOfFiles = 0;
    free(pRuntimeEnv->vnodeFileInfo.pFileInfo);
  }

  taosDestoryInterpoInfo(&pRuntimeEnv->interpoInfo);

  if (pRuntimeEnv->pInterpoBuf != NULL) {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      tfree(pRuntimeEnv->pInterpoBuf[i]);
    }

    tfree(pRuntimeEnv->pInterpoBuf);
  }

  destroyDiskbasedResultBuf(pRuntimeEnv->pResultBuf);
  pRuntimeEnv->pTSBuf = tsBufDestory(pRuntimeEnv->pTSBuf);
}

bool isQueryKilled(SQuery *pQuery) {
  return false;

  SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);

  /*
   * check if the queried meter is going to be deleted.
   * if it will be deleted soon, stop current query ASAP.
   */
  SMeterObj *pMeterObj = pQInfo->pObj;
  if (vnodeIsMeterState(pMeterObj, TSDB_METER_STATE_DROPPING)) {
    pQInfo->killed = 1;
    return true;
  }

  return (pQInfo->killed == 1);
}

bool isFixedOutputQuery(SQuery *pQuery) {
  if (pQuery->intervalTime != 0) {
    return false;
  }

  // Note:top/bottom query is fixed output query
  if (isTopBottomQuery(pQuery) || isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    return true;
  }

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SSqlFuncExprMsg *pExprMsg = &pQuery->pSelectExpr[i].pBase;

    // ignore the ts_comp function
    if (i == 0 && pExprMsg->functionId == TSDB_FUNC_PRJ && pExprMsg->numOfParams == 1 &&
        pExprMsg->colInfo.colIdx == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      continue;
    }

    if (pExprMsg->functionId == TSDB_FUNC_TS || pExprMsg->functionId == TSDB_FUNC_TS_DUMMY) {
      continue;
    }

    if (!IS_MULTIOUTPUT(aAggs[pExprMsg->functionId].nStatus)) {
      return true;
    }
  }

  return false;
}

bool isPointInterpoQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionID = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionID == TSDB_FUNC_INTERP || functionID == TSDB_FUNC_LAST_ROW) {
      return true;
    }
  }

  return false;
}

// TODO REFACTOR:MERGE WITH CLIENT-SIDE FUNCTION
bool isSumAvgRateQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (functionId == TSDB_FUNC_SUM_RATE || functionId == TSDB_FUNC_SUM_IRATE || functionId == TSDB_FUNC_AVG_RATE ||
        functionId == TSDB_FUNC_AVG_IRATE) {
      return true;
    }
  }

  return false;
}

bool isTopBottomQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      return true;
    }
  }

  return false;
}

bool isFirstLastRowQuery(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionID = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionID == TSDB_FUNC_LAST_ROW) {
      return true;
    }
  }

  return false;
}

bool notHasQueryTimeRange(SQuery *pQuery) {
  return (pQuery->skey == 0 && pQuery->ekey == INT64_MAX && QUERY_IS_ASC_QUERY(pQuery)) ||
         (pQuery->skey == INT64_MAX && pQuery->ekey == 0 && (!QUERY_IS_ASC_QUERY(pQuery)));
}

bool isTSCompQuery(SQuery *pQuery) { return pQuery->pSelectExpr[0].pBase.functionId == TSDB_FUNC_TS_COMP; }

bool needSupplementaryScan(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG) {
      continue;
    }

    if (((functionId == TSDB_FUNC_LAST || functionId == TSDB_FUNC_LAST_DST) && QUERY_IS_ASC_QUERY(pQuery)) ||
        ((functionId == TSDB_FUNC_FIRST || functionId == TSDB_FUNC_FIRST_DST) && !QUERY_IS_ASC_QUERY(pQuery))) {
      return true;
    }
  }

  return false;
}
/////////////////////////////////////////////////////////////////////////////////////////////

void doGetAlignedIntervalQueryRangeImpl(SQuery *pQuery, int64_t pKey, int64_t keyFirst, int64_t keyLast,
                                        int64_t *actualSkey, int64_t *actualEkey, int64_t *skey, int64_t *ekey) {
  assert(pKey >= keyFirst && pKey <= keyLast);
  *skey = taosGetIntervalStartTimestamp(pKey, pQuery->intervalTime, pQuery->intervalTimeUnit, pQuery->precision);

  if (keyFirst > (INT64_MAX - pQuery->intervalTime)) {
    /*
     * if the actualSkey > INT64_MAX - pQuery->intervalTime, the query duration between
     * actualSkey and actualEkey must be less than one interval.Therefore, no need to adjust the query ranges.
     */
    assert(keyLast - keyFirst < pQuery->intervalTime);

    *actualSkey = keyFirst;
    *actualEkey = keyLast;

    *ekey = INT64_MAX;
    return;
  }

  *ekey = *skey + pQuery->intervalTime - 1;

  if (*skey < keyFirst) {
    *actualSkey = keyFirst;
  } else {
    *actualSkey = *skey;
  }

  if (*ekey < keyLast) {
    *actualEkey = *ekey;
  } else {
    *actualEkey = keyLast;
  }
}

static bool doGetQueryPos(TSKEY key, STableQuerySupportObj *pSupporter, SPointInterpoSupporter *pPointInterpSupporter) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  SMeterObj *       pMeterObj = pRuntimeEnv->pMeterObj;

  /* key in query range. If not, no qualified in disk file */
  if (key != -1 && key <= pQuery->ekey) {
    if (isPointInterpoQuery(pQuery)) { /* no qualified data in this query range */
      return getNeighborPoints(pSupporter, pMeterObj, pPointInterpSupporter);
    } else {
      return true;
    }
  } else {  // key > pQuery->ekey, abort for normal query, continue for interp query
    if (isPointInterpoQuery(pQuery)) {
      return getNeighborPoints(pSupporter, pMeterObj, pPointInterpSupporter);
    } else {
      return false;
    }
  }
}

static bool doSetDataInfo(STableQuerySupportObj *pSupporter, SPointInterpoSupporter *pPointInterpSupporter,
                          SMeterObj *pMeterObj, TSKEY nextKey) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (isFirstLastRowQuery(pQuery)) {
    /*
     * if the pQuery->skey != pQuery->ekey for last_row query,
     * the query range is existed, so set them both the value of nextKey
     */
    if (pQuery->skey != pQuery->ekey) {
      assert(pQuery->skey >= pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery) && nextKey >= pQuery->ekey &&
             nextKey <= pQuery->skey);

      pQuery->skey = nextKey;
      pQuery->ekey = nextKey;
    }

    return getNeighborPoints(pSupporter, pMeterObj, pPointInterpSupporter);
  } else {
    return true;
  }
}

// TODO refactor code, the best way to implement the last_row is utilizing the iterator
bool normalizeUnBoundLastRowQuery(STableQuerySupportObj *pSupporter, SPointInterpoSupporter *pPointInterpSupporter) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;

  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  assert(!QUERY_IS_ASC_QUERY(pQuery) && notHasQueryTimeRange(pQuery));
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

  TSKEY lastKey = -1;

  pQuery->fileId = -1;
  vnodeFreeFieldsEx(pRuntimeEnv);

  // keep in-memory cache status in local variables in case that it may be changed by write operation
  getBasicCacheInfoSnapshot(pQuery, pMeterObj->pCache, pMeterObj->vnode);

  SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;
  if (pCacheInfo != NULL && pCacheInfo->cacheBlocks != NULL && pQuery->numOfBlocks > 0) {
    pQuery->fileId = -1;
    TSKEY key = pMeterObj->lastKey;

    pQuery->skey = key;
    pQuery->ekey = key;
    pQuery->lastKey = pQuery->skey;

    /*
     * cache block may have been flushed to disk, and no data in cache anymore.
     * So, copy cache block to local buffer is required.
     */
    lastKey = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, false);
    if (lastKey < 0) {  // data has been flushed to disk, try again search in file
      lastKey = getQueryPositionForCacheInvalid(pRuntimeEnv, searchFn);

      if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED)) {
        return false;
      }
    }
  } else {  // no data in cache, try file
    TSKEY key = pMeterObj->lastKeyOnFile;

    pQuery->skey = key;
    pQuery->ekey = key;
    pQuery->lastKey = pQuery->skey;

    bool ret = getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_LESS_EQUAL, searchFn);
    if (!ret) {  // no data in file, return false;
      return false;
    }

    lastKey = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
  }

  assert(lastKey <= pQuery->skey);

  pQuery->skey = lastKey;
  pQuery->ekey = lastKey;
  pQuery->lastKey = pQuery->skey;

  return getNeighborPoints(pSupporter, pMeterObj, pPointInterpSupporter);
}

static int64_t getGreaterEqualTimestamp(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *            pQuery = pRuntimeEnv->pQuery;
  SMeterObj *         pMeterObj = pRuntimeEnv->pMeterObj;
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    return -1;
  }

  TSKEY key = -1;

  SPositionInfo p = {0};
  {  // todo refactor save the context
    savePointPosition(&p, pQuery->fileId, pQuery->slot, pQuery->pos);
  }

  SWAP(pQuery->skey, pQuery->ekey, TSKEY);
  pQuery->lastKey = pQuery->skey;
  pQuery->order.order ^= 1u;

  if (getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_GREATER_EQUAL, searchFn)) {
    key = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
  } else {  // set no data in file
    key = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, false);
  }

  SWAP(pQuery->skey, pQuery->ekey, TSKEY);
  pQuery->order.order ^= 1u;
  pQuery->lastKey = pQuery->skey;

  pQuery->fileId = p.fileId;
  pQuery->pos = p.pos;
  pQuery->slot = p.slot;

  return key;
}

/**
 * determine the first query range, according to raw query range [skey, ekey] and group-by interval.
 * the time interval for aggregating is not enforced to check its validation, the minimum interval is not less than
 * 10ms, which is guaranteed by parser at client-side
 */
bool normalizedFirstQueryRange(bool dataInDisk, bool dataInCache, STableQuerySupportObj *pSupporter,
                               SPointInterpoSupporter *pPointInterpSupporter, int64_t *key) {
  SQueryRuntimeEnv *  pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *            pQuery = pRuntimeEnv->pQuery;
  SMeterObj *         pMeterObj = pRuntimeEnv->pMeterObj;
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

  if (QUERY_IS_ASC_QUERY(pQuery)) {
    // todo: the action return as the getQueryStartPositionInCache function
    if (dataInDisk && getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_GREATER_EQUAL, searchFn)) {
      TSKEY nextKey = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
      assert(nextKey >= pQuery->skey);

      if (key != NULL) {
        *key = nextKey;
      }

      return doGetQueryPos(nextKey, pSupporter, pPointInterpSupporter);
    }

    // set no data in file
    pQuery->fileId = -1;
    SCacheInfo *pCacheInfo = (SCacheInfo *)pMeterObj->pCache;

    /* if it is a interpolation query, the any points in cache that is greater than the query range is required */
    if (pCacheInfo == NULL || pCacheInfo->cacheBlocks == NULL || pCacheInfo->numOfBlocks == 0 || !dataInCache) {
      return false;
    }

    TSKEY nextKey = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, false);

    if (key != NULL) {
      *key = nextKey;
    }

    return doGetQueryPos(nextKey, pSupporter, pPointInterpSupporter);

  } else {              // descending order
    if (dataInCache) {  // todo handle error
      TSKEY nextKey = getQueryStartPositionInCache(pRuntimeEnv, &pQuery->slot, &pQuery->pos, false);
      assert(nextKey == -1 || nextKey <= pQuery->skey);

      if (key != NULL) {
        *key = nextKey;
      }

      if (nextKey != -1) {  // find qualified data in cache
        if (nextKey >= pQuery->ekey) {
          return doSetDataInfo(pSupporter, pPointInterpSupporter, pMeterObj, nextKey);
        } else {
          /*
           * nextKey < pQuery->ekey && nextKey < pQuery->lastKey, query range is
           * larger than all data, abort
           *
           * NOTE: Interp query does not reach here, since for all interp query,
           * the query order is ascending order.
           */
          return false;
        }
      } else {  // all data in cache are greater than pQuery->skey, try file
      }
    }

    if (dataInDisk && getQualifiedDataBlock(pMeterObj, pRuntimeEnv, QUERY_RANGE_LESS_EQUAL, searchFn)) {
      TSKEY nextKey = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
      assert(nextKey <= pQuery->skey);

      if (key != NULL) {
        *key = nextKey;
      }

      // key in query range. If not, no qualified in disk file
      if (nextKey >= pQuery->ekey) {
        return doSetDataInfo(pSupporter, pPointInterpSupporter, pMeterObj, nextKey);
      } else {  // In case of all queries, the value of false will be returned if key < pQuery->ekey
        return false;
      }
    }
  }

  return false;
}

static void setScanLimitationByResultBuffer(SQuery *pQuery) {
  if (isTopBottomQuery(pQuery)) {
    pQuery->checkBufferInLoop = 0;
  } else if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    pQuery->checkBufferInLoop = 0;
  } else {
    bool hasMultioutput = false;
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      SSqlFuncExprMsg *pExprMsg = &pQuery->pSelectExpr[i].pBase;
      if (pExprMsg->functionId == TSDB_FUNC_TS || pExprMsg->functionId == TSDB_FUNC_TS_DUMMY) {
        continue;
      }

      hasMultioutput = IS_MULTIOUTPUT(aAggs[pExprMsg->functionId].nStatus);
      if (!hasMultioutput) {
        break;
      }
    }

    pQuery->checkBufferInLoop = hasMultioutput ? 1 : 0;
  }

  pQuery->pointsOffset = pQuery->pointsToRead;
}

/*
 * todo add more parameters to check soon..
 */
bool vnodeParametersSafetyCheck(SQuery *pQuery) {
  // load data column information is incorrect
  for (int32_t i = 0; i < pQuery->numOfCols - 1; ++i) {
    if (pQuery->colList[i].data.colId == pQuery->colList[i + 1].data.colId) {
      dError("QInfo:%p invalid data load column for query", GET_QINFO_ADDR(pQuery));
      return false;
    }
  }
  return true;
}

static void updateOffsetVal(SQueryRuntimeEnv *pRuntimeEnv, SBlockInfo *pDataBlockInfo, void *pBlock) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  /*
   *  The actually qualified points that can be skipped needs to be calculated if query is
   *  done in current data block
   */
  if ((pQuery->ekey <= pDataBlockInfo->keyLast && QUERY_IS_ASC_QUERY(pQuery)) ||
      (pQuery->ekey >= pDataBlockInfo->keyFirst && !QUERY_IS_ASC_QUERY(pQuery))) {
    // force load timestamp data blocks
    if (IS_DISK_DATA_BLOCK(pQuery)) {
      getTimestampInDiskBlock(pRuntimeEnv, 0);
    }

    // update the pQuery->limit.offset value, and pQuery->pos value
    TSKEY *keys = (TSKEY *)pRuntimeEnv->primaryColBuffer->data;

    int32_t i = 0;
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      for (i = pQuery->pos; i < pDataBlockInfo->size && pQuery->limit.offset > 0; ++i) {
        if (keys[i] <= pQuery->ekey) {
          pQuery->limit.offset -= 1;
        } else {
          break;
        }
      }

    } else {
      for (i = pQuery->pos; i >= 0 && pQuery->limit.offset > 0; --i) {
        if (keys[i] >= pQuery->ekey) {
          pQuery->limit.offset -= 1;
        } else {
          break;
        }
      }
    }

    if (((i == pDataBlockInfo->size || keys[i] > pQuery->ekey) && QUERY_IS_ASC_QUERY(pQuery)) ||
        ((i < 0 || keys[i] < pQuery->ekey) && !QUERY_IS_ASC_QUERY(pQuery))) {
      setQueryStatus(pQuery, QUERY_COMPLETED);
      pQuery->pos = -1;
    } else {
      pQuery->pos = i;
    }
  } else {
    if (QUERY_IS_ASC_QUERY(pQuery)) {
      pQuery->pos += pQuery->limit.offset;
    } else {
      pQuery->pos -= pQuery->limit.offset;
    }

    assert(pQuery->pos >= 0 && pQuery->pos <= pDataBlockInfo->size - 1);

    if (IS_DISK_DATA_BLOCK(pQuery)) {
      pQuery->skey = getTimestampInDiskBlock(pRuntimeEnv, pQuery->pos);
    } else {
      pQuery->skey = getTimestampInCacheBlock(pRuntimeEnv, pBlock, pQuery->pos);
    }

    // update the offset value
    pQuery->lastKey = pQuery->skey;
    pQuery->limit.offset = 0;
  }
}

// todo ignore the avg/sum/min/max/count/stddev/top/bottom functions, of which
// the scan order is not matter
static bool onlyOneQueryType(SQuery *pQuery, int32_t functId, int32_t functIdDst) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;

    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG ||
        functionId == TSDB_FUNC_TAG_DUMMY) {
      continue;
    }

    if (functionId != functId && functionId != functIdDst) {
      return false;
    }
  }

  return true;
}

static bool onlyFirstQuery(SQuery *pQuery) { return onlyOneQueryType(pQuery, TSDB_FUNC_FIRST, TSDB_FUNC_FIRST_DST); }

static bool onlyLastQuery(SQuery *pQuery) { return onlyOneQueryType(pQuery, TSDB_FUNC_LAST, TSDB_FUNC_LAST_DST); }

static void changeExecuteScanOrder(SQuery *pQuery, bool metricQuery) {
  // in case of point-interpolation query, use asc order scan
  char msg[] = "QInfo:%p scan order changed for %s query, old:%d, new:%d, qrange exchanged, old qrange:%" PRId64
               "-%" PRId64 ", new qrange:%" PRId64 "-%" PRId64;

  // todo handle the case the the order irrelevant query type mixed up with order critical query type
  // descending order query for last_row query
  if (isFirstLastRowQuery(pQuery)) {
    dTrace("QInfo:%p scan order changed for last_row query, old:%d, new:%d", GET_QINFO_ADDR(pQuery),
           pQuery->order.order, TSQL_SO_DESC);

    pQuery->order.order = TSQL_SO_DESC;

    int64_t skey = MIN(pQuery->skey, pQuery->ekey);
    int64_t ekey = MAX(pQuery->skey, pQuery->ekey);

    pQuery->skey = ekey;
    pQuery->ekey = skey;

    return;
  }

  if (isPointInterpoQuery(pQuery) && pQuery->intervalTime == 0) {
    if (!QUERY_IS_ASC_QUERY(pQuery)) {
      dTrace(msg, GET_QINFO_ADDR(pQuery), "interp", pQuery->order.order, TSQL_SO_ASC, pQuery->skey, pQuery->ekey,
             pQuery->ekey, pQuery->skey);
      SWAP(pQuery->skey, pQuery->ekey, TSKEY);
    }

    pQuery->order.order = TSQL_SO_ASC;
    return;
  }

  if (pQuery->intervalTime == 0) {
    if (onlyFirstQuery(pQuery)) {
      if (!QUERY_IS_ASC_QUERY(pQuery)) {
        dTrace(msg, GET_QINFO_ADDR(pQuery), "only-first", pQuery->order.order, TSQL_SO_ASC, pQuery->skey, pQuery->ekey,
               pQuery->ekey, pQuery->skey);

        SWAP(pQuery->skey, pQuery->ekey, TSKEY);
      }

      pQuery->order.order = TSQL_SO_ASC;
    } else if (onlyLastQuery(pQuery)) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        dTrace(msg, GET_QINFO_ADDR(pQuery), "only-last", pQuery->order.order, TSQL_SO_DESC, pQuery->skey, pQuery->ekey,
               pQuery->ekey, pQuery->skey);

        SWAP(pQuery->skey, pQuery->ekey, TSKEY);
      }

      pQuery->order.order = TSQL_SO_DESC;
    }

  } else {  // interval query
    if (metricQuery) {
      if (onlyFirstQuery(pQuery)) {
        if (!QUERY_IS_ASC_QUERY(pQuery)) {
          dTrace(msg, GET_QINFO_ADDR(pQuery), "only-first stable", pQuery->order.order, TSQL_SO_ASC, pQuery->skey,
                 pQuery->ekey, pQuery->ekey, pQuery->skey);

          SWAP(pQuery->skey, pQuery->ekey, TSKEY);
        }

        pQuery->order.order = TSQL_SO_ASC;
      } else if (onlyLastQuery(pQuery)) {
        if (QUERY_IS_ASC_QUERY(pQuery)) {
          dTrace(msg, GET_QINFO_ADDR(pQuery), "only-last stable", pQuery->order.order, TSQL_SO_DESC, pQuery->skey,
                 pQuery->ekey, pQuery->ekey, pQuery->skey);

          SWAP(pQuery->skey, pQuery->ekey, TSKEY);
        }

        pQuery->order.order = TSQL_SO_DESC;
      }
    }
  }
}

static int32_t doSkipDataBlock(SQueryRuntimeEnv *pRuntimeEnv) {
  SMeterObj *         pMeterObj = pRuntimeEnv->pMeterObj;
  SQuery *            pQuery = pRuntimeEnv->pQuery;
  int32_t             step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pMeterObj->searchAlgorithm];

  while (1) {
    moveToNextBlock(pRuntimeEnv, step, searchFn, false);
    if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
      break;
    }

    void *pBlock = getGenericDataBlock(pMeterObj, pRuntimeEnv, pQuery->slot);
    assert(pBlock != NULL);

    SBlockInfo blockInfo = getBlockInfo(pRuntimeEnv);

    int32_t maxReads = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.size - pQuery->pos : pQuery->pos + 1;
    assert(maxReads >= 0);

    if (pQuery->limit.offset < maxReads || (pQuery->ekey <= blockInfo.keyLast && QUERY_IS_ASC_QUERY(pQuery)) ||
        (pQuery->ekey >= blockInfo.keyFirst && !QUERY_IS_ASC_QUERY(pQuery))) {  // start position in current block
      updateOffsetVal(pRuntimeEnv, &blockInfo, pBlock);
      break;
    } else {
      pQuery->limit.offset -= maxReads;
      pQuery->lastKey = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.keyLast : blockInfo.keyFirst;
      pQuery->lastKey += step;

      qTrace("QInfo:%p skip rows:%d, offset:%" PRId64 "", GET_QINFO_ADDR(pQuery), maxReads, pQuery->limit.offset);
    }
  }

  return 0;
}

void forwardQueryStartPosition(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  SMeterObj *pMeterObj = pRuntimeEnv->pMeterObj;

  if (pQuery->limit.offset <= 0) {
    return;
  }

  void *pBlock = getGenericDataBlock(pMeterObj, pRuntimeEnv, pQuery->slot);
  assert(pBlock != NULL);

  SBlockInfo blockInfo = getBlockInfo(pRuntimeEnv);

  // get the qualified data that can be skipped
  int32_t maxReads = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.size - pQuery->pos : pQuery->pos + 1;

  if (pQuery->limit.offset < maxReads || (pQuery->ekey <= blockInfo.keyLast && QUERY_IS_ASC_QUERY(pQuery)) ||
      (pQuery->ekey >= blockInfo.keyFirst && !QUERY_IS_ASC_QUERY(pQuery))) {  // start position in current block
    updateOffsetVal(pRuntimeEnv, &blockInfo, pBlock);
  } else {
    pQuery->limit.offset -= maxReads;

    // update the lastkey, since the following skip operation may traverse to another media. update the lastkey first.
    pQuery->lastKey = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.keyLast + 1 : blockInfo.keyFirst - 1;
    doSkipDataBlock(pRuntimeEnv);
  }
}

static bool forwardQueryStartPosIfNeeded(SQInfo *pQInfo, STableQuerySupportObj *pSupporter, bool dataInDisk,
                                         bool dataInCache) {
  SQuery *          pQuery = &pQInfo->query;
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;

  /* if queried with value filter, do NOT forward query start position */
  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) {
    return true;
  }

  if (pQuery->limit.offset > 0 && (!isTopBottomQuery(pQuery)) && pQuery->interpoType == TSDB_INTERPO_NONE) {
    /*
     * 1. for top/bottom query, the offset applies to the final result, not here
     * 2. for interval without interpolation query we forward pQuery->intervalTime at a time for
     *    pQuery->limit.offset times. Since hole exists, pQuery->intervalTime*pQuery->limit.offset value is
     *    not valid. otherwise, we only forward pQuery->limit.offset number of points
     */
    if (isIntervalQuery(pQuery)) {
      int16_t             step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
      __block_search_fn_t searchFn = vnodeSearchKeyFunc[pRuntimeEnv->pMeterObj->searchAlgorithm];
      SWindowResInfo *    pWindowResInfo = &pRuntimeEnv->windowResInfo;

      TSKEY *     primaryKey = (TSKEY *)pRuntimeEnv->primaryColBuffer->data;
      STimeWindow win = getActiveTimeWindow(pWindowResInfo, pWindowResInfo->prevSKey, pQuery);

      while (pQuery->limit.offset > 0) {
        SBlockInfo blockInfo = getBlockInfo(pRuntimeEnv);

        STimeWindow tw = win;
        getNextTimeWindow(pQuery, &tw);

        // next time window starts from current data block
        if ((tw.skey <= blockInfo.keyLast && QUERY_IS_ASC_QUERY(pQuery)) ||
            (tw.ekey >= blockInfo.keyFirst && !QUERY_IS_ASC_QUERY(pQuery))) {
          // query completed
          if ((tw.skey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
              (tw.ekey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
            setQueryStatus(pQuery, QUERY_COMPLETED);
            break;
          }

          // check its position in this block to make sure this time window covers data.
          if (IS_DISK_DATA_BLOCK(pQuery)) {
            getTimestampInDiskBlock(pRuntimeEnv, 0);
          }

          tw = win;
          int32_t startPos = getNextQualifiedWindow(pRuntimeEnv, &tw, pWindowResInfo, &blockInfo, primaryKey, searchFn);
          assert(startPos >= 0);

          pQuery->limit.offset -= 1;

          // set the abort info
          pQuery->pos = startPos;
          pQuery->lastKey = primaryKey[startPos];
          pWindowResInfo->prevSKey = tw.skey;
          win = tw;
          continue;
        } else {
          moveToNextBlock(pRuntimeEnv, step, searchFn, false);
          if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
            break;
          }

          blockInfo = getBlockInfo(pRuntimeEnv);
          if ((blockInfo.keyFirst > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
              (blockInfo.keyLast < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
            setQueryStatus(pQuery, QUERY_COMPLETED);
            break;
          }

          // set the window that start from the next data block
          TSKEY       key = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.keyFirst : blockInfo.keyLast;
          STimeWindow n = getActiveTimeWindow(pWindowResInfo, key, pQuery);

          // next data block are still covered by current time window
          if (n.skey == win.skey && n.ekey == win.ekey) {
            // do nothing
          } else {
            pQuery->limit.offset -= 1;

            // query completed
            if ((n.skey > pQuery->ekey && QUERY_IS_ASC_QUERY(pQuery)) ||
                (n.ekey < pQuery->ekey && !QUERY_IS_ASC_QUERY(pQuery))) {
              setQueryStatus(pQuery, QUERY_COMPLETED);
              break;
            }

            // set the abort info
            pQuery->pos = QUERY_IS_ASC_QUERY(pQuery) ? 0 : blockInfo.size - 1;
            pQuery->lastKey = QUERY_IS_ASC_QUERY(pQuery) ? blockInfo.keyFirst : blockInfo.keyLast;
            pWindowResInfo->prevSKey = n.skey;

            win = n;

            if (pQuery->limit.offset == 0 && IS_DISK_DATA_BLOCK(pQuery)) {
              getTimestampInDiskBlock(pRuntimeEnv, 0);
            }
          }
        }
      }

      if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK | QUERY_COMPLETED) || pQuery->limit.offset > 0) {
        setQueryStatus(pQuery, QUERY_COMPLETED);

        sem_post(&pQInfo->dataReady);  // hack for next read for empty return;
        pQInfo->over = 1;
        return false;
      } else {
        if (IS_DISK_DATA_BLOCK(pQuery)) {
          getTimestampInDiskBlock(pRuntimeEnv, 0);
        }
      }
    } else {  // forward the start position for projection query
      forwardQueryStartPosition(&pSupporter->runtimeEnv);
      if (Q_STATUS_EQUAL(pQuery->over, QUERY_NO_DATA_TO_CHECK)) {
        setQueryStatus(pQuery, QUERY_COMPLETED);

        sem_post(&pQInfo->dataReady);  // hack for next read for empty return;
        pQInfo->over = 1;
        return false;
      }
    }
  }

  return true;
}

static void doSetInterpVal(SQLFunctionCtx *pCtx, TSKEY ts, int16_t type, int32_t index, char *data) {
  assert(pCtx->param[index].pz == NULL);

  int32_t len = 0;
  size_t  t = 0;

  if (type == TSDB_DATA_TYPE_BINARY) {
    t = strlen(data);

    len = t + 1 + TSDB_KEYSIZE;
    pCtx->param[index].pz = calloc(1, len);
  } else if (type == TSDB_DATA_TYPE_NCHAR) {
    t = wcslen((const wchar_t *)data);

    len = (t + 1) * TSDB_NCHAR_SIZE + TSDB_KEYSIZE;
    pCtx->param[index].pz = calloc(1, len);
  } else {
    len = TSDB_KEYSIZE * 2;
    pCtx->param[index].pz = malloc(len);
  }

  pCtx->param[index].nType = TSDB_DATA_TYPE_BINARY;

  char *z = pCtx->param[index].pz;
  *(TSKEY *)z = ts;
  z += TSDB_KEYSIZE;

  switch (type) {
    case TSDB_DATA_TYPE_FLOAT:
      *(double *)z = GET_FLOAT_VAL(data);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      *(double *)z = GET_DOUBLE_VAL(data);
      break;
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      *(int64_t *)z = GET_INT64_VAL(data);
      break;
    case TSDB_DATA_TYPE_BINARY:
      strncpy(z, data, t);
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      wcsncpy((wchar_t *)z, (const wchar_t *)data, t);
    } break;
    default:
      assert(0);
  }

  pCtx->param[index].nLen = len;
}

/**
 * param[1]: default value/previous value of specified timestamp
 * param[2]: next value of specified timestamp
 * param[3]: denotes if the result is a precious result or interpolation results
 *
 * @param pQInfo
 * @param pSupporter
 * @param pInterpoRaw
 */
void pointInterpSupporterSetData(SQInfo *pQInfo, SPointInterpoSupporter *pPointInterpSupport) {
  // not point interpolation query, abort
  if (!isPointInterpoQuery(&pQInfo->query)) {
    return;
  }

  SQuery *               pQuery = &pQInfo->query;
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  int32_t count = 1;
  TSKEY   key = *(TSKEY *)pPointInterpSupport->pNextPoint[0];

  if (key == pSupporter->rawSKey) {
    // the queried timestamp has value, return it directly without interpolation
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      tVariantCreateFromBinary(&pRuntimeEnv->pCtx[i].param[3], (char *)&count, sizeof(count), TSDB_DATA_TYPE_INT);

      pRuntimeEnv->pCtx[i].param[0].i64Key = key;
      pRuntimeEnv->pCtx[i].param[0].nType = TSDB_DATA_TYPE_BIGINT;
    }
  } else {
    // set the direct previous(next) point for process
    count = 2;

    if (pQuery->interpoType == TSDB_INTERPO_SET_VALUE) {
      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

        // only the function of interp needs the corresponding information
        if (pCtx->functionId != TSDB_FUNC_INTERP) {
          continue;
        }

        pCtx->numOfParams = 4;

        SInterpInfo *pInterpInfo = (SInterpInfo *)pRuntimeEnv->pCtx[i].aOutputBuf;
        pInterpInfo->pInterpDetail = calloc(1, sizeof(SInterpInfoDetail));

        SInterpInfoDetail *pInterpDetail = pInterpInfo->pInterpDetail;

        // for primary timestamp column, set the flag
        if (pQuery->pSelectExpr[i].pBase.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
          pInterpDetail->primaryCol = 1;
        }

        tVariantCreateFromBinary(&pCtx->param[3], (char *)&count, sizeof(count), TSDB_DATA_TYPE_INT);

        if (isNull((char *)&pQuery->defaultVal[i], pCtx->inputType)) {
          pCtx->param[1].nType = TSDB_DATA_TYPE_NULL;
        } else {
          tVariantCreateFromBinary(&pCtx->param[1], (char *)&pQuery->defaultVal[i], pCtx->inputBytes, pCtx->inputType);
        }

        pInterpDetail->ts = pSupporter->rawSKey;
        pInterpDetail->type = pQuery->interpoType;
      }
    } else {
      TSKEY prevKey = *(TSKEY *)pPointInterpSupport->pPrevPoint[0];
      TSKEY nextKey = *(TSKEY *)pPointInterpSupport->pNextPoint[0];

      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];

        // tag column does not need the interp environment
        if (pQuery->pSelectExpr[i].pBase.functionId == TSDB_FUNC_TAG) {
          continue;
        }

        int32_t colInBuf = pQuery->pSelectExpr[i].pBase.colInfo.colIdxInBuf;

        SInterpInfo *pInterpInfo = (SInterpInfo *)pRuntimeEnv->pCtx[i].aOutputBuf;

        pInterpInfo->pInterpDetail = calloc(1, sizeof(SInterpInfoDetail));
        SInterpInfoDetail *pInterpDetail = pInterpInfo->pInterpDetail;

        int32_t type = GET_COLUMN_TYPE(pQuery, i);

        // for primary timestamp column, set the flag
        if (pQuery->pSelectExpr[i].pBase.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
          pInterpDetail->primaryCol = 1;
        } else {
          doSetInterpVal(pCtx, prevKey, type, 1, pPointInterpSupport->pPrevPoint[colInBuf]);
          doSetInterpVal(pCtx, nextKey, type, 2, pPointInterpSupport->pNextPoint[colInBuf]);
        }

        tVariantCreateFromBinary(&pRuntimeEnv->pCtx[i].param[3], (char *)&count, sizeof(count), TSDB_DATA_TYPE_INT);

        pInterpDetail->ts = pSupporter->rawSKey;
        pInterpDetail->type = pQuery->interpoType;
      }
    }
  }
}

void pointInterpSupporterInit(SQuery *pQuery, SPointInterpoSupporter *pInterpoSupport) {
  if (isPointInterpoQuery(pQuery)) {
    pInterpoSupport->pPrevPoint = malloc(pQuery->numOfCols * POINTER_BYTES);
    pInterpoSupport->pNextPoint = malloc(pQuery->numOfCols * POINTER_BYTES);

    pInterpoSupport->numOfCols = pQuery->numOfCols;

    /* get appropriated size for one row data source*/
    int32_t len = 0;
    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      len += pQuery->colList[i].data.bytes;
    }

    assert(PRIMARY_TSCOL_LOADED(pQuery));

    void *prev = calloc(1, len);
    void *next = calloc(1, len);

    int32_t offset = 0;

    for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
      pInterpoSupport->pPrevPoint[i] = prev + offset;
      pInterpoSupport->pNextPoint[i] = next + offset;

      offset += pQuery->colList[i].data.bytes;
    }
  }
}

void pointInterpSupporterDestroy(SPointInterpoSupporter *pPointInterpSupport) {
  if (pPointInterpSupport->numOfCols <= 0 || pPointInterpSupport->pPrevPoint == NULL) {
    return;
  }

  tfree(pPointInterpSupport->pPrevPoint[0]);
  tfree(pPointInterpSupport->pNextPoint[0]);

  tfree(pPointInterpSupport->pPrevPoint);
  tfree(pPointInterpSupport->pNextPoint);

  pPointInterpSupport->numOfCols = 0;
}

static void allocMemForInterpo(STableQuerySupportObj *pSupporter, SQuery *pQuery, SMeterObj *pMeterObj) {
  if (pQuery->interpoType != TSDB_INTERPO_NONE) {
    assert(isIntervalQuery(pQuery) || (pQuery->intervalTime == 0 && isPointInterpoQuery(pQuery)));

    if (isIntervalQuery(pQuery)) {
      pSupporter->runtimeEnv.pInterpoBuf = malloc(POINTER_BYTES * pQuery->numOfOutputCols);

      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        pSupporter->runtimeEnv.pInterpoBuf[i] =
            calloc(1, sizeof(tFilePage) + pQuery->pSelectExpr[i].resBytes * pMeterObj->pointsPerFileBlock);
      }
    }
  }
}

static int32_t getInitialPageNum(STableQuerySupportObj *pSupporter) {
  SQuery *pQuery = pSupporter->runtimeEnv.pQuery;
  int32_t INITIAL_RESULT_ROWS_VALUE = 16;

  int32_t num = 0;

  if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
    num = 128;
  } else if (isIntervalQuery(pQuery)) {  // time window query, allocate one page for each table
    num = MAX(pSupporter->numOfMeters, INITIAL_RESULT_ROWS_VALUE);
  } else {  // for super table query, one page for each subset
    num = pSupporter->pSidSet->numOfSubSet;
  }

  assert(num > 0);
  return num;
}

static int32_t allocateRuntimeEnvBuf(SQueryRuntimeEnv *pRuntimeEnv, SMeterObj *pMeterObj) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // To make sure the start position of each buffer is aligned to 4bytes in 32-bit ARM system.
  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    int32_t bytes = pQuery->colList[i].data.bytes;
    pRuntimeEnv->colDataBuffer[i] = calloc(1, sizeof(SData) + EXTRA_BYTES + pMeterObj->pointsPerFileBlock * bytes);
    if (pRuntimeEnv->colDataBuffer[i] == NULL) {
      goto _error_clean;
    }
  }

  // record the maximum column width among columns of this meter/metric
  int32_t maxColWidth = pQuery->colList[0].data.bytes;
  for (int32_t i = 1; i < pQuery->numOfCols; ++i) {
    int32_t bytes = pQuery->colList[i].data.bytes;
    if (bytes > maxColWidth) {
      maxColWidth = bytes;
    }
  }

  pRuntimeEnv->primaryColBuffer = NULL;
  if (PRIMARY_TSCOL_LOADED(pQuery)) {
    pRuntimeEnv->primaryColBuffer = pRuntimeEnv->colDataBuffer[0];
  } else {
    pRuntimeEnv->primaryColBuffer =
        (SData *)malloc(pMeterObj->pointsPerFileBlock * TSDB_KEYSIZE + sizeof(SData) + EXTRA_BYTES);
  }

  pRuntimeEnv->unzipBufSize = (size_t)(maxColWidth * pMeterObj->pointsPerFileBlock + EXTRA_BYTES);  // plus extra_bytes

  pRuntimeEnv->unzipBuffer = (char *)calloc(1, pRuntimeEnv->unzipBufSize);
  pRuntimeEnv->secondaryUnzipBuffer = (char *)calloc(1, pRuntimeEnv->unzipBufSize);

  if (pRuntimeEnv->unzipBuffer == NULL || pRuntimeEnv->secondaryUnzipBuffer == NULL ||
      pRuntimeEnv->primaryColBuffer == NULL) {
    goto _error_clean;
  }

  return TSDB_CODE_SUCCESS;

_error_clean:
  for (int32_t i = 0; i < pRuntimeEnv->pQuery->numOfCols; ++i) {
    tfree(pRuntimeEnv->colDataBuffer[i]);
  }

  tfree(pRuntimeEnv->unzipBuffer);
  tfree(pRuntimeEnv->secondaryUnzipBuffer);

  if (!PRIMARY_TSCOL_LOADED(pQuery)) {
    tfree(pRuntimeEnv->primaryColBuffer);
  }

  return TSDB_CODE_SERV_OUT_OF_MEMORY;
}

static int32_t getRowParamForMultiRowsOutput(SQuery *pQuery, bool isSTableQuery) {
  int32_t rowparam = 1;

  if (isTopBottomQuery(pQuery) && (!isSTableQuery)) {
    rowparam = pQuery->pSelectExpr[1].pBase.arg->argValue.i64;
  }

  return rowparam;
}

static int32_t getNumOfRowsInResultPage(SQuery *pQuery, bool isSTableQuery) {
  int32_t rowSize = pQuery->rowSize * getRowParamForMultiRowsOutput(pQuery, isSTableQuery);
  return (DEFAULT_INTERN_BUF_SIZE - sizeof(tFilePage)) / rowSize;
}

char *getPosInResultPage(SQueryRuntimeEnv *pRuntimeEnv, int32_t columnIndex, SWindowResult *pResult) {
  assert(pResult != NULL && pRuntimeEnv != NULL);

  SQuery *   pQuery = pRuntimeEnv->pQuery;
  tFilePage *page = getResultBufferPageById(pRuntimeEnv->pResultBuf, pResult->pos.pageId);

  int32_t numOfRows = getNumOfRowsInResultPage(pQuery, pRuntimeEnv->stableQuery);
  int32_t realRowId = pResult->pos.rowId * getRowParamForMultiRowsOutput(pQuery, pRuntimeEnv->stableQuery);

  return ((char *)page->data) + pRuntimeEnv->offset[columnIndex] * numOfRows +
         pQuery->pSelectExpr[columnIndex].resBytes * realRowId;
}

void vnodeQueryFreeQInfoEx(SQInfo *pQInfo) {
  if (pQInfo == NULL || pQInfo->pTableQuerySupporter == NULL) {
    return;
  }

  SQuery *               pQuery = &pQInfo->query;
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  teardownQueryRuntimeEnv(&pSupporter->runtimeEnv);
  tfree(pSupporter->pMeterSidExtInfo);

  if (pSupporter->pMetersHashTable != NULL) {
    taosHashCleanup(pSupporter->pMetersHashTable);
    pSupporter->pMetersHashTable = NULL;
  }

  tSidSetDestroy(&pSupporter->pSidSet);

  if (pSupporter->pMeterDataInfo != NULL) {
    for (int32_t j = 0; j < pSupporter->numOfMeters; ++j) {
      destroyMeterQueryInfo(pSupporter->pMeterDataInfo[j].pMeterQInfo, pQuery->numOfOutputCols);
      free(pSupporter->pMeterDataInfo[j].pBlock);
    }
  }

  tfree(pSupporter->pMeterDataInfo);

  tfree(pQInfo->pTableQuerySupporter);
}

int32_t vnodeSTableQueryPrepare(SQInfo *pQInfo, SQuery *pQuery, void *param) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->skey > pQuery->ekey)) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->ekey > pQuery->skey))) {
    dTrace("QInfo:%p no result in time range %" PRId64 "-%" PRId64 ", order %d", pQInfo, pQuery->skey, pQuery->ekey,
           pQuery->order.order);

    sem_post(&pQInfo->dataReady);
    pQInfo->over = 1;

    return TSDB_CODE_SUCCESS;
  }

  pQInfo->over = 0;
  pQInfo->pointsRead = 0;
  pQuery->pointsRead = 0;

  changeExecuteScanOrder(pQuery, true);
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;

  initQueryFileInfoFD(&pRuntimeEnv->vnodeFileInfo);
  vnodeInitDataBlockLoadInfo(&pRuntimeEnv->dataBlockLoadInfo);
  vnodeInitCompBlockLoadInfo(&pRuntimeEnv->compBlockLoadInfo);

  /*
   * since we employ the output control mechanism in main loop.
   * so, disable it during data block scan procedure.
   */
  setScanLimitationByResultBuffer(pQuery);

  // save raw query range for applying to each subgroup
  pSupporter->rawEKey = pQuery->ekey;
  pSupporter->rawSKey = pQuery->skey;
  pQuery->lastKey = pQuery->skey;

  // create runtime environment
  SColumnModel *pTagSchemaInfo = pSupporter->pSidSet->pColumnModel;

  // get one queried meter
  SMeterObj *pMeter = getMeterObj(pSupporter->pMetersHashTable, pSupporter->pSidSet->pSids[0]->sid);

  pRuntimeEnv->pTSBuf = param;
  pRuntimeEnv->cur.vnodeIndex = -1;

  // set the ts-comp file traverse order
  if (param != NULL) {
    int16_t order = (pQuery->order.order == pRuntimeEnv->pTSBuf->tsOrder) ? TSQL_SO_ASC : TSQL_SO_DESC;
    tsBufSetTraverseOrder(pRuntimeEnv->pTSBuf, order);
  }

  int32_t ret = setupQueryRuntimeEnv(pMeter, pQuery, &pSupporter->runtimeEnv, pTagSchemaInfo, TSQL_SO_ASC, true);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  ret = allocateRuntimeEnvBuf(pRuntimeEnv, pMeter);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  tSidSetSort(pSupporter->pSidSet);
  vnodeRecordAllFiles(pQInfo, pMeter->vnode);

  int32_t size = getInitialPageNum(pSupporter);
  ret = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, size, pQuery->rowSize);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  if (pQuery->intervalTime == 0) {
    int16_t type = TSDB_DATA_TYPE_NULL;

    if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {  // group by columns not tags;
      type = getGroupbyColumnType(pQuery, pQuery->pGroupbyExpr);
    } else {
      type = TSDB_DATA_TYPE_INT;  // group id
    }

    initWindowResInfo(&pRuntimeEnv->windowResInfo, pRuntimeEnv, 512, 4096, type);
  }

  pRuntimeEnv->numOfRowsPerPage = getNumOfRowsInResultPage(pQuery, true);

  // metric query do not invoke interpolation, it will be done at the second-stage merge
  if (!isPointInterpoQuery(pQuery)) {
    pQuery->interpoType = TSDB_INTERPO_NONE;
  }

  TSKEY revisedStime = taosGetIntervalStartTimestamp(pSupporter->rawSKey, pQuery->intervalTime,
                                                     pQuery->intervalTimeUnit, pQuery->precision);
  taosInitInterpoInfo(&pRuntimeEnv->interpoInfo, pQuery->order.order, revisedStime, 0, 0);
  pRuntimeEnv->stableQuery = true;

  return TSDB_CODE_SUCCESS;
}

/**
 * decrease the refcount for each table involved in this query
 * @param pQInfo
 */
void vnodeDecMeterRefcnt(SQInfo *pQInfo) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  if (pSupporter != NULL) {
    assert(pSupporter->numOfMeters >= 1);
  }

  if (pSupporter == NULL || pSupporter->numOfMeters == 1) {
    atomic_fetch_sub_32(&pQInfo->pObj->numOfQueries, 1);
    dTrace("QInfo:%p vid:%d sid:%d meterId:%s, query is over, numOfQueries:%d", pQInfo, pQInfo->pObj->vnode,
           pQInfo->pObj->sid, pQInfo->pObj->meterId, pQInfo->pObj->numOfQueries);
  } else {
    int32_t num = 0;
    for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
      SMeterObj *pMeter = getMeterObj(pSupporter->pMetersHashTable, pSupporter->pSidSet->pSids[i]->sid);
      atomic_fetch_sub_32(&(pMeter->numOfQueries), 1);

      if (pMeter->numOfQueries > 0) {
        dTrace("QInfo:%p vid:%d sid:%d meterId:%s, query is over, numOfQueries:%d", pQInfo, pMeter->vnode, pMeter->sid,
               pMeter->meterId, pMeter->numOfQueries);
        num++;
      }
    }

    /*
     * in order to reduce log output, for all meters of which numOfQueries count are 0,
     * we do not output corresponding information
     */
    num = pSupporter->numOfMeters - num;
    dTrace("QInfo:%p metric query is over, dec query ref for %d meters, numOfQueries on %d meters are 0", pQInfo,
           pSupporter->numOfMeters, num);
  }
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

static bool needToLoadDataBlock(SQuery *pQuery, SDataStatis *pField, SQLFunctionCtx *pCtx, int32_t numOfTotalPoints) {
  if (pField == NULL) {
    return true;
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


static int32_t doHandleDataBlockImpl(SQueryRuntimeEnv *pRuntimeEnv, SDataBlockInfo *pDataBlockInfo, int32_t *forwardStep) {
  SQuery *           pQuery = pRuntimeEnv->pQuery;
  int32_t            numOfRes = 0;
  
  __block_search_fn_t searchFn = vnodeSearchKeyFunc[pRuntimeEnv->pMeterObj->searchAlgorithm];

  SArray* pDataBlock = NULL;
  SDataStatis* pStatis = NULL;
  
  STimeWindow *w = &pDataBlockInfo->window;
  int32_t blkStatus = 0;
  
  uint32_t req = 0;
  if (pQuery->numOfFilterCols > 0) {
    req = BLK_DATA_ALL_NEEDED;
  } else {
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
      int32_t colId = pQuery->pSelectExpr[i].pBase.colInfo.colId;
      
      req |= aAggs[functionId].dataReqFunc(&pRuntimeEnv->pCtx[i], w->skey, w->ekey, colId, blkStatus);
    }

    if (pRuntimeEnv->pTSBuf > 0 || isIntervalQuery(pQuery)) {
      req |= BLK_DATA_ALL_NEEDED;
    }
  }

  if (req == BLK_DATA_NO_NEEDED) {
    //      qTrace("QInfo:%p vid:%d sid:%d id:%s, slot:%d, data block ignored, brange:%" PRId64 "-%" PRId64 ", rows:%d",
    //             GET_QINFO_ADDR(pQuery), pMeterObj->vnode, pMeterObj->sid, pMeterObj->meterId, pQuery->slot,
    //             pBlock->keyFirst, pBlock->keyLast, pBlock->numOfPoints);
  } else if (req == BLK_DATA_FILEDS_NEEDED) {
    if (tsdbRetrieveDataBlockStatisInfo(pRuntimeEnv->pQueryHandle, &pStatis) != TSDB_CODE_SUCCESS) {
      return DISK_DATA_LOAD_FAILED;
    }
    
    if (pStatis == NULL) {
      pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
    }
  } else {
    assert(req == BLK_DATA_ALL_NEEDED);
    if (tsdbRetrieveDataBlockStatisInfo(pRuntimeEnv->pQueryHandle, &pStatis) != TSDB_CODE_SUCCESS) {
      return DISK_DATA_LOAD_FAILED;
    }

    /*
     * if this block is completed included in the query range, do more filter operation
     * filter the data block according to the value filter condition.
     * no need to load the data block, continue for next block
     */
    if (!needToLoadDataBlock(pQuery, pStatis, pRuntimeEnv->pCtx, pDataBlockInfo->size)) {
#if defined(_DEBUG_VIEW)
      dTrace("QInfo:%p fileId:%d, slot:%d, block discarded by per-filter", GET_QINFO_ADDR(pQuery), pQuery->fileId,
             pQuery->slot);
#endif
//      qTrace("QInfo:%p id:%s slot:%d, data block ignored by pre-filter, fields loaded, brange:%" PRId64 "-%" PRId64 ", rows:%d",
//             GET_QINFO_ADDR(pQuery), pMeterObj->meterId, pQuery->slot, pBlock->keyFirst, pBlock->keyLast,
//             pBlock->numOfPoints);
      return DISK_DATA_DISCARDED;
    }
  
    pDataBlock = tsdbRetrieveDataBlock(pRuntimeEnv->pQueryHandle, NULL);
  }
  
  *forwardStep = tableApplyFunctionsOnBlock(pRuntimeEnv, pDataBlockInfo, pStatis, searchFn, &numOfRes,
                                            &pRuntimeEnv->windowResInfo, pDataBlock);

  return numOfRes;
}

// previous time window may not be of the same size of pQuery->intervalTime
static void getNextTimeWindow(SQuery *pQuery, STimeWindow *pTimeWindow) {
  int32_t factor = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  pTimeWindow->skey += (pQuery->slidingTime * factor);
  pTimeWindow->ekey = pTimeWindow->skey + (pQuery->intervalTime - 1);
}

static int64_t doScanAllDataBlocks(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *   pQuery = pRuntimeEnv->pQuery;
  
  int64_t cnt = 0;
  dTrace("QInfo:%p query start, qrange:%" PRId64 "-%" PRId64 ", lastkey:%" PRId64 ", order:%d",
         GET_QINFO_ADDR(pQuery), pQuery->skey, pQuery->ekey, pQuery->lastKey, pQuery->order.order);

  STsdbQueryHandle* pQueryHandle = pRuntimeEnv->pQueryHandle;
  
  while (tsdbNextDataBlock(pQueryHandle)) {
    // check if query is killed or not set the status of query to pass the status check
    if (isQueryKilled(pQuery)) {
      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
      return cnt;
    }

    int32_t forwardStep = 0;
    SDataBlockInfo blockInfo = tsdbRetrieveDataBlockInfo(pQueryHandle);
    
    if (isIntervalQuery(pQuery) && pRuntimeEnv->windowResInfo.prevSKey == 0) {
      TSKEY skey1, ekey1;
      TSKEY windowSKey = 0, windowEKey = 0;
  
      doGetAlignedIntervalQueryRangeImpl(pQuery, blockInfo.window.skey, blockInfo.window.skey,
                                         pQueryHandle->window.ekey, &skey1, &ekey1, &windowSKey,
                                         &windowEKey);
      pRuntimeEnv->windowResInfo.startTime = windowSKey;
  
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        pRuntimeEnv->windowResInfo.prevSKey = windowSKey;
      } else {
        assert(false);
//        pRuntimeEnv->windowResInfo.prevSKey =
//            windowSKey + ((win.ekey - windowSKey) / pQuery->slidingTime) * pQuery->slidingTime;
      }
    }
    
    doHandleDataBlockImpl(pRuntimeEnv, &blockInfo, &forwardStep);

    dTrace("QInfo:%p check data block, brange:%" PRId64 "-%" PRId64 ", fileId:%d, slot:%d, pos:%d, rows:%d, checked:%d",
           GET_QINFO_ADDR(pQuery), blockInfo.window.skey, blockInfo.window.ekey, pQuery->fileId, pQuery->slot,
           pQuery->pos, blockInfo.size, forwardStep);

    // save last access position
    cnt += forwardStep;

    if (queryPausedInCurrentBlock(pQuery, &blockInfo, forwardStep)) {
      if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {
        break;
      }
    }
  }  // while(1)
  
  // if the result buffer is not full, set the query completed flag
  if (!Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {
    setQueryStatus(pQuery, QUERY_COMPLETED);
  }

  if (isIntervalQuery(pQuery) && IS_MASTER_SCAN(pRuntimeEnv)) {
    if (Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED | QUERY_NO_DATA_TO_CHECK)) {
      closeAllTimeWindow(&pRuntimeEnv->windowResInfo);
      pRuntimeEnv->windowResInfo.curIndex = pRuntimeEnv->windowResInfo.size - 1;
    } else if (Q_STATUS_EQUAL(pQuery->over, QUERY_RESBUF_FULL)) {  // check if window needs to be closed
      SBlockInfo blockInfo = getBlockInfo(pRuntimeEnv);

      // check if need to close window result or not
      TSKEY t = (QUERY_IS_ASC_QUERY(pQuery)) ? blockInfo.keyFirst : blockInfo.keyLast;
      doCheckQueryCompleted(pRuntimeEnv, t, &pRuntimeEnv->windowResInfo);
    }
  }

  return cnt;
}

static void updatelastkey(SQuery *pQuery, SMeterQueryInfo *pMeterQInfo) { pMeterQInfo->lastKey = pQuery->lastKey; }

/*
 * set tag value in SQLFunctionCtx
 * e.g.,tag information into input buffer
 */
static void doSetTagValueInParam(SColumnModel *pTagSchema, int32_t tagColIdx, SMeterSidExtInfo *pMeterSidInfo,
                                 tVariant *param) {
  assert(tagColIdx >= 0);

  int16_t offset = getColumnModelOffset(pTagSchema, tagColIdx);

  void *   pStr = (char *)pMeterSidInfo->tags + offset;
  SSchema *pCol = getColumnModelSchema(pTagSchema, tagColIdx);

  tVariantDestroy(param);

  if (isNull(pStr, pCol->type)) {
    param->nType = TSDB_DATA_TYPE_NULL;
  } else {
    tVariantCreateFromBinary(param, pStr, pCol->bytes, pCol->type);
  }
}

void vnodeSetTagValueInParam(tSidSet *pSidSet, SQueryRuntimeEnv *pRuntimeEnv, SMeterSidExtInfo *pMeterSidInfo) {
  SQuery *      pQuery = pRuntimeEnv->pQuery;
  SColumnModel *pTagSchema = pSidSet->pColumnModel;

  SSqlFuncExprMsg *pFuncMsg = &pQuery->pSelectExpr[0].pBase;
  if (pQuery->numOfOutputCols == 1 && pFuncMsg->functionId == TSDB_FUNC_TS_COMP) {
    assert(pFuncMsg->numOfParams == 1);
    doSetTagValueInParam(pTagSchema, pFuncMsg->arg->argValue.i64, pMeterSidInfo, &pRuntimeEnv->pCtx[0].tag);
  } else {
    // set tag value, by which the results are aggregated.
    for (int32_t idx = 0; idx < pQuery->numOfOutputCols; ++idx) {
      SColIndexEx *pColEx = &pQuery->pSelectExpr[idx].pBase.colInfo;

      // ts_comp column required the tag value for join filter
      if (!TSDB_COL_IS_TAG(pColEx->flag)) {
        continue;
      }

      doSetTagValueInParam(pTagSchema, pColEx->colIdx, pMeterSidInfo, &pRuntimeEnv->pCtx[idx].tag);
    }

    // set the join tag for first column
    SSqlFuncExprMsg *pFuncMsg = &pQuery->pSelectExpr[0].pBase;
    if (pFuncMsg->functionId == TSDB_FUNC_TS && pFuncMsg->colInfo.colIdx == PRIMARYKEY_TIMESTAMP_COL_INDEX &&
        pRuntimeEnv->pTSBuf != NULL) {
      assert(pFuncMsg->numOfParams == 1);
      doSetTagValueInParam(pTagSchema, pFuncMsg->arg->argValue.i64, pMeterSidInfo, &pRuntimeEnv->pCtx[0].tag);
    }
  }
}

static void doMerge(SQueryRuntimeEnv *pRuntimeEnv, int64_t timestamp, SWindowResult *pWindowRes, bool mergeFlag) {
  SQuery *        pQuery = pRuntimeEnv->pQuery;
  SQLFunctionCtx *pCtx = pRuntimeEnv->pCtx;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (!mergeFlag) {
      pCtx[i].aOutputBuf = pCtx[i].aOutputBuf + pCtx[i].outputBytes;
      pCtx[i].currentStage = FIRST_STAGE_MERGE;

      resetResultInfo(pCtx[i].resultInfo);
      aAggs[functionId].init(&pCtx[i]);
    }

    pCtx[i].hasNull = true;
    pCtx[i].nStartQueryTimestamp = timestamp;
    pCtx[i].aInputElemBuf = getPosInResultPage(pRuntimeEnv, i, pWindowRes);
    //    pCtx[i].aInputElemBuf = ((char *)inputSrc->data) +
    //                            ((int32_t)pRuntimeEnv->offset[i] * pRuntimeEnv->numOfRowsPerPage) +
    //                            pCtx[i].outputBytes * inputIdx;

    // in case of tag column, the tag information should be extracted from input buffer
    if (functionId == TSDB_FUNC_TAG_DUMMY || functionId == TSDB_FUNC_TAG) {
      tVariantDestroy(&pCtx[i].tag);
      tVariantCreateFromBinary(&pCtx[i].tag, pCtx[i].aInputElemBuf, pCtx[i].inputBytes, pCtx[i].inputType);
    }
  }

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TAG_DUMMY) {
      continue;
    }

    aAggs[functionId].distMergeFunc(&pCtx[i]);
  }
}

static void printBinaryData(int32_t functionId, char *data, int32_t srcDataType) {
  if (functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_LAST_DST) {
    switch (srcDataType) {
      case TSDB_DATA_TYPE_BINARY:
        printf("%" PRId64 ",%s\t", *(TSKEY *)data, (data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_BOOL:
        printf("%" PRId64 ",%d\t", *(TSKEY *)data, *(int8_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        printf("%" PRId64 ",%d\t", *(TSKEY *)data, *(int16_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_TIMESTAMP:
        printf("%" PRId64 ",%" PRId64 "\t", *(TSKEY *)data, *(TSKEY *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_INT:
        printf("%" PRId64 ",%d\t", *(TSKEY *)data, *(int32_t *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%" PRId64 ",%f\t", *(TSKEY *)data, *(float *)(data + TSDB_KEYSIZE + 1));
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%" PRId64 ",%lf\t", *(TSKEY *)data, *(double *)(data + TSDB_KEYSIZE + 1));
        break;
    }
  } else if (functionId == TSDB_FUNC_AVG) {
    printf("%lf,%d\t", *(double *)data, *(int32_t *)(data + sizeof(double)));
  } else if (functionId == TSDB_FUNC_SPREAD) {
    printf("%lf,%lf\t", *(double *)data, *(double *)(data + sizeof(double)));
  } else if (functionId == TSDB_FUNC_TWA) {
    data += 1;
    printf("%lf,%" PRId64 ",%" PRId64 ",%" PRId64 "\t", *(double *)data, *(int64_t *)(data + 8),
           *(int64_t *)(data + 16), *(int64_t *)(data + 24));
  } else if (functionId == TSDB_FUNC_MIN || functionId == TSDB_FUNC_MAX) {
    switch (srcDataType) {
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_BOOL:
        printf("%d\t", *(int8_t *)data);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        printf("%d\t", *(int16_t *)data);
        break;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_TIMESTAMP:
        printf("%" PRId64 "\t", *(int64_t *)data);
        break;
      case TSDB_DATA_TYPE_INT:
        printf("%d\t", *(int *)data);
        break;
      case TSDB_DATA_TYPE_FLOAT:
        printf("%f\t", *(float *)data);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        printf("%f\t", *(float *)data);
        break;
    }
  } else if (functionId == TSDB_FUNC_SUM) {
    if (srcDataType == TSDB_DATA_TYPE_FLOAT || srcDataType == TSDB_DATA_TYPE_DOUBLE) {
      printf("%lf\t", *(float *)data);
    } else {
      printf("%" PRId64 "\t", *(int64_t *)data);
    }
  } else {
    printf("%s\t", data);
  }
}

void UNUSED_FUNC displayInterResult(SData **pdata, SQuery *pQuery, int32_t numOfRows) {
  int32_t numOfCols = pQuery->numOfOutputCols;
  printf("super table query intermediate result, total:%d\n", numOfRows);

  SQInfo *   pQInfo = (SQInfo *)(GET_QINFO_ADDR(pQuery));
  SMeterObj *pMeterObj = pQInfo->pObj;

  for (int32_t j = 0; j < numOfRows; ++j) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      switch (pQuery->pSelectExpr[i].resType) {
        case TSDB_DATA_TYPE_BINARY: {
          int32_t colIdx = pQuery->pSelectExpr[i].pBase.colInfo.colIdx;
          int32_t type = 0;

          if (TSDB_COL_IS_TAG(pQuery->pSelectExpr[i].pBase.colInfo.flag)) {
            type = pQuery->pSelectExpr[i].resType;
          } else {
            type = pMeterObj->schema[colIdx].type;
          }
          printBinaryData(pQuery->pSelectExpr[i].pBase.functionId, pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j,
                          type);
          break;
        }
        case TSDB_DATA_TYPE_TIMESTAMP:
        case TSDB_DATA_TYPE_BIGINT:
          printf("%" PRId64 "\t", *(int64_t *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
        case TSDB_DATA_TYPE_INT:
          printf("%d\t", *(int32_t *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
        case TSDB_DATA_TYPE_FLOAT:
          printf("%f\t", *(float *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          printf("%lf\t", *(double *)(pdata[i]->data + pQuery->pSelectExpr[i].resBytes * j));
          break;
      }
    }
    printf("\n");
  }
}

typedef struct SCompSupporter {
  SMeterDataInfo **      pMeterDataInfo;
  int32_t *              position;
  STableQuerySupportObj *pSupporter;
} SCompSupporter;

int32_t tableResultComparFn(const void *pLeft, const void *pRight, void *param) {
  int32_t left = *(int32_t *)pLeft;
  int32_t right = *(int32_t *)pRight;

  SCompSupporter *  supporter = (SCompSupporter *)param;
  SQueryRuntimeEnv *pRuntimeEnv = &supporter->pSupporter->runtimeEnv;

  int32_t leftPos = supporter->position[left];
  int32_t rightPos = supporter->position[right];

  /* left source is exhausted */
  if (leftPos == -1) {
    return 1;
  }

  /* right source is exhausted*/
  if (rightPos == -1) {
    return -1;
  }

  SWindowResInfo *pWindowResInfo1 = &supporter->pMeterDataInfo[left]->pMeterQInfo->windowResInfo;
  SWindowResult * pWindowRes1 = getWindowResult(pWindowResInfo1, leftPos);

  char *b1 = getPosInResultPage(pRuntimeEnv, PRIMARYKEY_TIMESTAMP_COL_INDEX, pWindowRes1);
  TSKEY leftTimestamp = GET_INT64_VAL(b1);

  SWindowResInfo *pWindowResInfo2 = &supporter->pMeterDataInfo[right]->pMeterQInfo->windowResInfo;
  SWindowResult * pWindowRes2 = getWindowResult(pWindowResInfo2, rightPos);

  char *b2 = getPosInResultPage(pRuntimeEnv, PRIMARYKEY_TIMESTAMP_COL_INDEX, pWindowRes2);
  TSKEY rightTimestamp = GET_INT64_VAL(b2);

  if (leftTimestamp == rightTimestamp) {
    return 0;
  }

  return leftTimestamp > rightTimestamp ? 1 : -1;
}

int32_t mergeMetersResultToOneGroups(STableQuerySupportObj *pSupporter) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int64_t st = taosGetTimestampMs();
  int32_t ret = TSDB_CODE_SUCCESS;

  while (pSupporter->subgroupIdx < pSupporter->pSidSet->numOfSubSet) {
    int32_t start = pSupporter->pSidSet->starterPos[pSupporter->subgroupIdx];
    int32_t end = pSupporter->pSidSet->starterPos[pSupporter->subgroupIdx + 1];

    ret = doMergeMetersResultsToGroupRes(pSupporter, pQuery, pRuntimeEnv, pSupporter->pMeterDataInfo, start, end);
    if (ret < 0) {  // not enough disk space to save the data into disk
      return -1;
    }

    pSupporter->subgroupIdx += 1;

    // this group generates at least one result, return results
    if (ret > 0) {
      break;
    }

    assert(pSupporter->numOfGroupResultPages == 0);
    dTrace("QInfo:%p no result in group %d, continue", GET_QINFO_ADDR(pQuery), pSupporter->subgroupIdx - 1);
  }

  dTrace("QInfo:%p merge res data into group, index:%d, total group:%d, elapsed time:%lldms", GET_QINFO_ADDR(pQuery),
         pSupporter->subgroupIdx - 1, pSupporter->pSidSet->numOfSubSet, taosGetTimestampMs() - st);

  return TSDB_CODE_SUCCESS;
}

void copyResToQueryResultBuf(STableQuerySupportObj *pSupporter, SQuery *pQuery) {
  if (pSupporter->offset == pSupporter->numOfGroupResultPages) {
    pSupporter->numOfGroupResultPages = 0;

    // current results of group has been sent to client, try next group
    if (mergeMetersResultToOneGroups(pSupporter) != TSDB_CODE_SUCCESS) {
      return;  // failed to save data in the disk
    }

    // set current query completed
    if (pSupporter->numOfGroupResultPages == 0 && pSupporter->subgroupIdx == pSupporter->pSidSet->numOfSubSet) {
      pSupporter->meterIdx = pSupporter->pSidSet->numOfSids;
      return;
    }
  }

  SQueryRuntimeEnv *        pRuntimeEnv = &pSupporter->runtimeEnv;
  SQueryDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;

  int32_t id = getGroupResultId(pSupporter->subgroupIdx - 1);
  SIDList list = getDataBufPagesIdList(pResultBuf, pSupporter->offset + id);

  int32_t total = 0;
  for (int32_t i = 0; i < list.size; ++i) {
    tFilePage *pData = getResultBufferPageById(pResultBuf, list.pData[i]);
    total += pData->numOfElems;
  }

  pQuery->sdata[0]->len = total;

  int32_t offset = 0;
  for (int32_t num = 0; num < list.size; ++num) {
    tFilePage *pData = getResultBufferPageById(pResultBuf, list.pData[num]);

    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;
      char *  pDest = pQuery->sdata[i]->data;

      memcpy(pDest + offset * bytes, pData->data + pRuntimeEnv->offset[i] * pData->numOfElems,
             bytes * pData->numOfElems);
    }

    offset += pData->numOfElems;
  }

  assert(pQuery->pointsRead == 0);

  pQuery->pointsRead += pQuery->sdata[0]->len;
  pSupporter->offset += 1;
}

int64_t getNumOfResultWindowRes(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pWindowRes) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  int64_t maxOutput = 0;
  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ) {
      continue;
    }

    SResultInfo *pResultInfo = &pWindowRes->resultInfo[j];
    if (pResultInfo != NULL && maxOutput < pResultInfo->numOfRes) {
      maxOutput = pResultInfo->numOfRes;
    }
  }

  return maxOutput;
}

int32_t doMergeMetersResultsToGroupRes(STableQuerySupportObj *pSupporter, SQuery *pQuery, SQueryRuntimeEnv *pRuntimeEnv,
                                       SMeterDataInfo *pMeterDataInfo, int32_t start, int32_t end) {
  tFilePage **     buffer = (tFilePage **)pQuery->sdata;
  int32_t *        posList = calloc((end - start), sizeof(int32_t));
  SMeterDataInfo **pTableList = malloc(POINTER_BYTES * (end - start));

  // todo opt for the case of one table per group
  int32_t numOfMeters = 0;
  for (int32_t i = start; i < end; ++i) {
    int32_t sid = pMeterDataInfo[i].pMeterQInfo->sid;

    SIDList list = getDataBufPagesIdList(pRuntimeEnv->pResultBuf, sid);
    if (list.size > 0 && pMeterDataInfo[i].pMeterQInfo->windowResInfo.size > 0) {
      pTableList[numOfMeters] = &pMeterDataInfo[i];
      numOfMeters += 1;
    }
  }

  if (numOfMeters == 0) {
    tfree(posList);
    tfree(pTableList);

    assert(pSupporter->numOfGroupResultPages == 0);
    return 0;
  }

  SCompSupporter cs = {pTableList, posList, pSupporter};

  SLoserTreeInfo *pTree = NULL;
  tLoserTreeCreate(&pTree, numOfMeters, &cs, tableResultComparFn);

  SResultInfo *pResultInfo = calloc(pQuery->numOfOutputCols, sizeof(SResultInfo));
  setWindowResultInfo(pResultInfo, pQuery, pRuntimeEnv->stableQuery);

  resetMergeResultBuf(pQuery, pRuntimeEnv->pCtx, pResultInfo);
  int64_t lastTimestamp = -1;

  int64_t startt = taosGetTimestampMs();

  while (1) {
    int32_t pos = pTree->pNode[0].index;

    SWindowResInfo *pWindowResInfo = &pTableList[pos]->pMeterQInfo->windowResInfo;
    SWindowResult * pWindowRes = getWindowResult(pWindowResInfo, cs.position[pos]);

    char *b = getPosInResultPage(pRuntimeEnv, PRIMARYKEY_TIMESTAMP_COL_INDEX, pWindowRes);
    TSKEY ts = GET_INT64_VAL(b);

    assert(ts == pWindowRes->window.skey);
    int64_t num = getNumOfResultWindowRes(pRuntimeEnv, pWindowRes);
    if (num <= 0) {
      cs.position[pos] += 1;

      if (cs.position[pos] >= pWindowResInfo->size) {
        cs.position[pos] = -1;

        // all input sources are exhausted
        if (--numOfMeters == 0) {
          break;
        }
      }
    } else {
      if (ts == lastTimestamp) {  // merge with the last one
        doMerge(pRuntimeEnv, ts, pWindowRes, true);
      } else {  // copy data to disk buffer
        if (buffer[0]->numOfElems == pQuery->pointsToRead) {
          if (flushFromResultBuf(pSupporter, pQuery, pRuntimeEnv) != TSDB_CODE_SUCCESS) {
            return -1;
          }

          resetMergeResultBuf(pQuery, pRuntimeEnv->pCtx, pResultInfo);
        }

        doMerge(pRuntimeEnv, ts, pWindowRes, false);
        buffer[0]->numOfElems += 1;
      }

      lastTimestamp = ts;

      cs.position[pos] += 1;
      if (cs.position[pos] >= pWindowResInfo->size) {
        cs.position[pos] = -1;

        // all input sources are exhausted
        if (--numOfMeters == 0) {
          break;
        }
      }
    }

    tLoserTreeAdjust(pTree, pos + pTree->numOfEntries);
  }

  if (buffer[0]->numOfElems != 0) {  // there are data in buffer
    if (flushFromResultBuf(pSupporter, pQuery, pRuntimeEnv) != TSDB_CODE_SUCCESS) {
      //      dError("QInfo:%p failed to flush data into temp file, abort query", GET_QINFO_ADDR(pQuery),
      //             pSupporter->extBufFile);
      tfree(pTree);
      tfree(pTableList);
      tfree(posList);
      tfree(pResultInfo);

      return -1;
    }
  }

  int64_t endt = taosGetTimestampMs();

#ifdef _DEBUG_VIEW
  displayInterResult(pQuery->sdata, pQuery, pQuery->sdata[0]->len);
#endif

  dTrace("QInfo:%p result merge completed, elapsed time:%" PRId64 " ms", GET_QINFO_ADDR(pQuery), endt - startt);
  tfree(pTree);
  tfree(pTableList);
  tfree(posList);

  pSupporter->offset = 0;
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    tfree(pResultInfo[i].interResultBuf);
  }

  tfree(pResultInfo);
  return pSupporter->numOfGroupResultPages;
}

int32_t flushFromResultBuf(STableQuerySupportObj *pSupporter, const SQuery *pQuery,
                           const SQueryRuntimeEnv *pRuntimeEnv) {
  SQueryDiskbasedResultBuf *pResultBuf = pRuntimeEnv->pResultBuf;
  int32_t                   capacity = (DEFAULT_INTERN_BUF_SIZE - sizeof(tFilePage)) / pQuery->rowSize;

  // the base value for group result, since the maximum number of table for each vnode will not exceed 100,000.
  int32_t pageId = -1;

  int32_t remain = pQuery->sdata[0]->len;
  int32_t offset = 0;

  while (remain > 0) {
    int32_t r = remain;
    if (r > capacity) {
      r = capacity;
    }

    int32_t    id = getGroupResultId(pSupporter->subgroupIdx) + pSupporter->numOfGroupResultPages;
    tFilePage *buf = getNewDataBuf(pResultBuf, id, &pageId);

    // pagewise copy to dest buffer
    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;
      buf->numOfElems = r;

      memcpy(buf->data + pRuntimeEnv->offset[i] * buf->numOfElems, ((char *)pQuery->sdata[i]->data) + offset * bytes,
             buf->numOfElems * bytes);
    }

    offset += r;
    remain -= r;
  }

  pSupporter->numOfGroupResultPages += 1;
  return TSDB_CODE_SUCCESS;
}

void resetMergeResultBuf(SQuery *pQuery, SQLFunctionCtx *pCtx, SResultInfo *pResultInfo) {
  for (int32_t k = 0; k < pQuery->numOfOutputCols; ++k) {
    pCtx[k].aOutputBuf = pQuery->sdata[k]->data - pCtx[k].outputBytes;
    pCtx[k].size = 1;
    pCtx[k].startOffset = 0;
    pCtx[k].resultInfo = &pResultInfo[k];

    pQuery->sdata[k]->len = 0;
  }
}

void setMeterDataInfo(SMeterDataInfo *pMeterDataInfo, SMeterObj *pMeterObj, int32_t meterIdx, int32_t groupId) {
  pMeterDataInfo->pMeterObj = pMeterObj;
  pMeterDataInfo->groupIdx = groupId;
  pMeterDataInfo->meterOrderIdx = meterIdx;
}

static void doDisableFunctsForSupplementaryScan(SQuery *pQuery, SWindowResInfo *pWindowResInfo, int32_t order) {
  for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
    SWindowStatus *pStatus = getTimeWindowResStatus(pWindowResInfo, i);
    if (!pStatus->closed) {
      continue;
    }

    SWindowResult *buf = getWindowResult(pWindowResInfo, i);

    // open/close the specified query for each group result
    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      int32_t functId = pQuery->pSelectExpr[j].pBase.functionId;

      if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSQL_SO_DESC) ||
          ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSQL_SO_ASC)) {
        buf->resultInfo[j].complete = false;
      } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
        buf->resultInfo[j].complete = true;
      }
    }
  }
}

void disableFunctForTableSuppleScan(SQueryRuntimeEnv *pRuntimeEnv, int32_t order) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // group by normal columns and interval query on normal table
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pRuntimeEnv->pCtx[i].order = (pRuntimeEnv->pCtx[i].order) ^ 1u;
  }

  SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || isIntervalQuery(pQuery)) {
    doDisableFunctsForSupplementaryScan(pQuery, pWindowResInfo, order);
  } else {  // for simple result of table query,
    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      int32_t         functId = pQuery->pSelectExpr[j].pBase.functionId;
      SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[j];

      if (((functId == TSDB_FUNC_FIRST || functId == TSDB_FUNC_FIRST_DST) && order == TSQL_SO_DESC) ||
          ((functId == TSDB_FUNC_LAST || functId == TSDB_FUNC_LAST_DST) && order == TSQL_SO_ASC)) {
        pCtx->resultInfo->complete = false;
      } else if (functId != TSDB_FUNC_TS && functId != TSDB_FUNC_TAG) {
        pCtx->resultInfo->complete = true;
      }
    }
  }

  pQuery->order.order = pQuery->order.order ^ 1u;
}

void disableFunctForSuppleScan(STableQuerySupportObj *pSupporter, int32_t order) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pRuntimeEnv->pCtx[i].order = (pRuntimeEnv->pCtx[i].order) ^ 1u;
  }

  if (isIntervalQuery(pQuery)) {
    for (int32_t i = 0; i < pSupporter->numOfMeters; ++i) {
      SMeterQueryInfo *pMeterQueryInfo = pSupporter->pMeterDataInfo[i].pMeterQInfo;
      SWindowResInfo * pWindowResInfo = &pMeterQueryInfo->windowResInfo;

      doDisableFunctsForSupplementaryScan(pQuery, pWindowResInfo, order);
    }
  } else {
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
    doDisableFunctsForSupplementaryScan(pQuery, pWindowResInfo, order);
  }

  pQuery->order.order = (pQuery->order.order) ^ 1u;
}

void enableFunctForMasterScan(SQueryRuntimeEnv *pRuntimeEnv, int32_t order) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pRuntimeEnv->pCtx[i].order = (pRuntimeEnv->pCtx[i].order) ^ 1u;
  }

  pQuery->order.order = (pQuery->order.order) ^ 1u;
}

void createQueryResultInfo(SQuery *pQuery, SWindowResult *pResultRow, bool isSTableQuery, SPosInfo *posInfo) {
  int32_t numOfCols = pQuery->numOfOutputCols;

  pResultRow->resultInfo = calloc((size_t)numOfCols, sizeof(SResultInfo));
  pResultRow->pos = *posInfo;

  // set the intermediate result output buffer
  setWindowResultInfo(pResultRow->resultInfo, pQuery, isSTableQuery);
}

void resetCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  int32_t rows = pRuntimeEnv->pMeterObj->pointsPerFileBlock;

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->aOutputBuf = pQuery->sdata[i]->data;

    /*
     * set the output buffer information and intermediate buffer
     * not all queries require the interResultBuf, such as COUNT/TAGPRJ/PRJ/TAG etc.
     */
    resetResultInfo(&pRuntimeEnv->resultInfo[i]);
    pCtx->resultInfo = &pRuntimeEnv->resultInfo[i];

    // set the timestamp output buffer for top/bottom/diff query
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
    }

    memset(pQuery->sdata[i]->data, 0, (size_t)pQuery->pSelectExpr[i].resBytes * rows);
  }

  initCtxOutputBuf(pRuntimeEnv);
}

void forwardCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, int64_t output) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // reset the execution contexts
  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;
    assert(functionId != TSDB_FUNC_DIFF);

    // set next output position
    if (IS_OUTER_FORWARD(aAggs[functionId].nStatus)) {
      pRuntimeEnv->pCtx[j].aOutputBuf += pRuntimeEnv->pCtx[j].outputBytes * output;
    }

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      /*
       * NOTE: for top/bottom query, the value of first column of output (timestamp) are assigned
       * in the procedure of top/bottom routine
       * the output buffer in top/bottom routine is ptsOutputBuf, so we need to forward the output buffer
       *
       * diff function is handled in multi-output function
       */
      pRuntimeEnv->pCtx[j].ptsOutputBuf += TSDB_KEYSIZE * output;
    }

    resetResultInfo(pRuntimeEnv->pCtx[j].resultInfo);
  }
}

void initCtxOutputBuf(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;
    pRuntimeEnv->pCtx[j].currentStage = 0;

    aAggs[functionId].init(&pRuntimeEnv->pCtx[j]);
  }
}

void doSkipResults(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  if (pQuery->pointsRead == 0 || pQuery->limit.offset == 0) {
    return;
  }

  if (pQuery->pointsRead <= pQuery->limit.offset) {
    pQuery->limit.offset -= pQuery->pointsRead;

    pQuery->pointsRead = 0;
    pQuery->pointsOffset = pQuery->pointsToRead;  // clear all data in result buffer

    resetCtxOutputBuf(pRuntimeEnv);

    // clear the buffer is full flag if exists
    pQuery->over &= (~QUERY_RESBUF_FULL);
  } else {
    int32_t numOfSkip = (int32_t)pQuery->limit.offset;
    pQuery->pointsRead -= numOfSkip;

    for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
      int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
      int32_t bytes = pRuntimeEnv->pCtx[i].outputBytes;

      memmove(pQuery->sdata[i]->data, pQuery->sdata[i]->data + bytes * numOfSkip, pQuery->pointsRead * bytes);
      pRuntimeEnv->pCtx[i].aOutputBuf += bytes * numOfSkip;

      if (functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
        pRuntimeEnv->pCtx[i].ptsOutputBuf += TSDB_KEYSIZE * numOfSkip;
      }
    }

    pQuery->limit.offset = 0;
  }
}

typedef struct SQueryStatus {
  SPositionInfo start;
  SPositionInfo next;
  SPositionInfo end;
  int8_t        overStatus;
  TSKEY         lastKey;
  STSCursor     cur;
} SQueryStatus;
// todo refactor
static void queryStatusSave(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatus *pStatus) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pStatus->overStatus = pQuery->over;
  pStatus->lastKey = pQuery->lastKey;

  pStatus->start = pRuntimeEnv->startPos;
  pStatus->next = pRuntimeEnv->nextPos;
  pStatus->end = pRuntimeEnv->endPos;

  pStatus->cur = tsBufGetCursor(pRuntimeEnv->pTSBuf);  // save the cursor

  if (pRuntimeEnv->pTSBuf) {
    pRuntimeEnv->pTSBuf->cur.order ^= 1u;
    tsBufNextPos(pRuntimeEnv->pTSBuf);
  }

  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  SWAP(pQuery->skey, pQuery->ekey, TSKEY);
  pQuery->lastKey = pQuery->skey;
  pRuntimeEnv->startPos = pRuntimeEnv->endPos;
}

static void queryStatusRestore(SQueryRuntimeEnv *pRuntimeEnv, SQueryStatus *pStatus) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  SWAP(pQuery->skey, pQuery->ekey, TSKEY);

  pQuery->lastKey = pStatus->lastKey;
  pQuery->over = pStatus->overStatus;

  pRuntimeEnv->startPos = pStatus->start;
  pRuntimeEnv->nextPos = pStatus->next;
  pRuntimeEnv->endPos = pStatus->end;

  tsBufSetCursor(pRuntimeEnv->pTSBuf, &pStatus->cur);
}

static void doSingleMeterSupplementScan(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *     pQuery = pRuntimeEnv->pQuery;
  SQueryStatus qStatus = {0};

  if (!needSupplementaryScan(pQuery)) {
    return;
  }

  dTrace("QInfo:%p start to supp scan", GET_QINFO_ADDR(pQuery));

  SET_SUPPLEMENT_SCAN_FLAG(pRuntimeEnv);

  // usually this load operation will incur load disk block operation
  TSKEY endKey = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->endPos);

  assert((QUERY_IS_ASC_QUERY(pQuery) && endKey <= pQuery->ekey) ||
         (!QUERY_IS_ASC_QUERY(pQuery) && endKey >= pQuery->ekey));

  // close necessary function execution during supplementary scan
  disableFunctForTableSuppleScan(pRuntimeEnv, pQuery->order.order);
  queryStatusSave(pRuntimeEnv, &qStatus);

  doScanAllDataBlocks(pRuntimeEnv);

  // set the correct start position, and load the corresponding block in buffer if required.
  TSKEY actKey = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->startPos);
  assert((QUERY_IS_ASC_QUERY(pQuery) && actKey >= pQuery->skey) ||
         (!QUERY_IS_ASC_QUERY(pQuery) && actKey <= pQuery->skey));

  queryStatusRestore(pRuntimeEnv, &qStatus);
  enableFunctForMasterScan(pRuntimeEnv, pQuery->order.order);
  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
}

void setQueryStatus(SQuery *pQuery, int8_t status) {
  if (status == QUERY_NOT_COMPLETED) {
    pQuery->over = status;
  } else {
    // QUERY_NOT_COMPLETED is not compatible with any other status, so clear its position first
    pQuery->over &= (~QUERY_NOT_COMPLETED);
    pQuery->over |= status;
  }
}

bool needScanDataBlocksAgain(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  bool    toContinue = false;

  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || isIntervalQuery(pQuery)) {
    // for each group result, call the finalize function for each column
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;

    for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
      SWindowResult *pResult = getWindowResult(pWindowResInfo, i);
      if (!pResult->status.closed) {
        continue;
      }

      setWindowResOutputBuf(pRuntimeEnv, pResult);

      for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
        int16_t functId = pQuery->pSelectExpr[j].pBase.functionId;
        if (functId == TSDB_FUNC_TS) {
          continue;
        }

        aAggs[functId].xNextStep(&pRuntimeEnv->pCtx[j]);
        SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);

        toContinue |= (!pResInfo->complete);
      }
    }
  } else {
    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      int16_t functId = pQuery->pSelectExpr[j].pBase.functionId;
      if (functId == TSDB_FUNC_TS) {
        continue;
      }

      aAggs[functId].xNextStep(&pRuntimeEnv->pCtx[j]);
      SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);

      toContinue |= (!pResInfo->complete);
    }
  }

  return toContinue;
}

void vnodeScanAllData(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  /* store the start query position */
  void* pos = tsdbDataBlockTell(pRuntimeEnv->pQueryHandle);
  TSKEY lastKey = -1;

  int64_t skey = pQuery->lastKey;
  int32_t status = pQuery->over;
  int32_t activeSlot = pRuntimeEnv->windowResInfo.curIndex;

  SET_MASTER_SCAN_FLAG(pRuntimeEnv);
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);

  while (1) {
    doScanAllDataBlocks(pRuntimeEnv);

    if (!needScanDataBlocksAgain(pRuntimeEnv)) {
      
      // restore the status
      if (pRuntimeEnv->scanFlag == REPEAT_SCAN) {
        pQuery->over = status;
      }
      
      break;
    }

    /*
     * set the correct start position, and load the corresponding block in buffer for next
     * round scan all data blocks.
     */
    int32_t ret = tsdbDataBlockSeek(pRuntimeEnv->pQueryHandle, pos);
    
//    TSKEY key = loadRequiredBlockIntoMem(pRuntimeEnv, &pRuntimeEnv->startPos);
//    assert((QUERY_IS_ASC_QUERY(pQuery) && key >= pQuery->skey) || (!QUERY_IS_ASC_QUERY(pQuery) && key <= pQuery->skey));

    status = pQuery->over;
//    pQuery->ekey = pQuery->lastKey - step;
//    pQuery->lastKey = pQuery->skey;
    pRuntimeEnv->windowResInfo.curIndex = activeSlot;

    setQueryStatus(pQuery, QUERY_NOT_COMPLETED);
    pRuntimeEnv->scanFlag = REPEAT_SCAN;

    /* check if query is killed or not */
    if (isQueryKilled(pQuery)) {
      setQueryStatus(pQuery, QUERY_NO_DATA_TO_CHECK);
      return;
    }
  }

  // no need to set the end key
  int64_t curLastKey = pQuery->lastKey;
  pQuery->skey = skey;
  pQuery->ekey = pQuery->lastKey - step;

  doSingleMeterSupplementScan(pRuntimeEnv);

  //   update the pQuery->skey/pQuery->ekey to limit the scan scope of sliding query during supplementary scan
//  pQuery->skey = oldSkey;
//  pQuery->ekey = oldEkey;
  pQuery->lastKey = curLastKey;
}

void doFinalizeResult(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || isIntervalQuery(pQuery)) {
    // for each group result, call the finalize function for each column
    SWindowResInfo *pWindowResInfo = &pRuntimeEnv->windowResInfo;
    if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
      closeAllTimeWindow(pWindowResInfo);
    }

    for (int32_t i = 0; i < pWindowResInfo->size; ++i) {
      SWindowResult *buf = &pWindowResInfo->pResult[i];
      if (!isWindowResClosed(pWindowResInfo, i)) {
        continue;
      }

      setWindowResOutputBuf(pRuntimeEnv, buf);

      for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
        aAggs[pQuery->pSelectExpr[j].pBase.functionId].xFinalize(&pRuntimeEnv->pCtx[j]);
      }

      /*
       * set the number of output results for group by normal columns, the number of output rows usually is 1 except
       * the top and bottom query
       */
      buf->numOfRows = getNumOfResult(pRuntimeEnv);
    }

  } else {
    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      aAggs[pQuery->pSelectExpr[j].pBase.functionId].xFinalize(&pRuntimeEnv->pCtx[j]);
    }
  }
}

static bool hasMainOutput(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;

    if (functionId != TSDB_FUNC_TS && functionId != TSDB_FUNC_TAG && functionId != TSDB_FUNC_TAGPRJ) {
      return true;
    }
  }

  return false;
}

int64_t getNumOfResult(SQueryRuntimeEnv *pRuntimeEnv) {
  SQuery *pQuery = pRuntimeEnv->pQuery;
  bool    hasMainFunction = hasMainOutput(pQuery);

  int64_t maxOutput = 0;
  for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
    int32_t functionId = pQuery->pSelectExpr[j].pBase.functionId;

    /*
     * ts, tag, tagprj function can not decide the output number of current query
     * the number of output result is decided by main output
     */
    if (hasMainFunction &&
        (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TAGPRJ)) {
      continue;
    }

    SResultInfo *pResInfo = GET_RES_INFO(&pRuntimeEnv->pCtx[j]);
    if (pResInfo != NULL && maxOutput < pResInfo->numOfRes) {
      maxOutput = pResInfo->numOfRes;
    }
  }

  return maxOutput;
}

static int32_t offsetComparator(const void *pLeft, const void *pRight) {
  SMeterDataInfo **pLeft1 = (SMeterDataInfo **)pLeft;
  SMeterDataInfo **pRight1 = (SMeterDataInfo **)pRight;

  if ((*pLeft1)->offsetInHeaderFile == (*pRight1)->offsetInHeaderFile) {
    return 0;
  }

  return ((*pLeft1)->offsetInHeaderFile > (*pRight1)->offsetInHeaderFile) ? 1 : -1;
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
int32_t vnodeFilterQualifiedMeters(SQInfo *pQInfo, int32_t vid, tSidSet *pSidSet, SMeterDataInfo *pMeterDataInfo,
                                   int32_t *numOfMeters, SMeterDataInfo ***pReqMeterDataInfo) {
  SQuery *pQuery = &pQInfo->query;

  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SMeterSidExtInfo **    pMeterSidExtInfo = pSupporter->pMeterSidExtInfo;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;

  SVnodeObj *pVnode = &vnodeList[vid];

  char *buf = calloc(1, getCompHeaderSegSize(&pVnode->cfg));
  if (buf == NULL) {
    *numOfMeters = 0;
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  SQueryFilesInfo *pVnodeFileInfo = &pRuntimeEnv->vnodeFileInfo;

  int32_t headerSize = getCompHeaderSegSize(&pVnode->cfg);
  lseek(pVnodeFileInfo->headerFd, TSDB_FILE_HEADER_LEN, SEEK_SET);
  read(pVnodeFileInfo->headerFd, buf, headerSize);

  // check the offset value integrity
  if (validateHeaderOffsetSegment(pQInfo, pRuntimeEnv->vnodeFileInfo.headerFilePath, vid, buf - TSDB_FILE_HEADER_LEN,
                                  headerSize) < 0) {
    free(buf);
    *numOfMeters = 0;

    return TSDB_CODE_FILE_CORRUPTED;
  }

  int64_t oldestKey = getOldestKey(pVnode->numOfFiles, pVnode->fileId, &pVnode->cfg);
  (*pReqMeterDataInfo) = malloc(POINTER_BYTES * pSidSet->numOfSids);
  if (*pReqMeterDataInfo == NULL) {
    free(buf);
    *numOfMeters = 0;

    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  int32_t groupId = 0;
  TSKEY   skey, ekey;

  for (int32_t i = 0; i < pSidSet->numOfSids; ++i) {  // load all meter meta info
    SMeterObj *pMeterObj = getMeterObj(pSupporter->pMetersHashTable, pMeterSidExtInfo[i]->sid);
    if (pMeterObj == NULL) {
      dError("QInfo:%p failed to find required sid:%d", pQInfo, pMeterSidExtInfo[i]->sid);
      continue;
    }

    if (i >= pSidSet->starterPos[groupId + 1]) {
      groupId += 1;
    }

    SMeterDataInfo *pOneMeterDataInfo = &pMeterDataInfo[i];
    if (pOneMeterDataInfo->pMeterObj == NULL) {
      setMeterDataInfo(pOneMeterDataInfo, pMeterObj, i, groupId);
    }

    /* restore possible exists new query range for this meter, which starts from cache */
    if (pOneMeterDataInfo->pMeterQInfo != NULL) {
      skey = pOneMeterDataInfo->pMeterQInfo->lastKey;
    } else {
      skey = pSupporter->rawSKey;
    }

    // query on disk data files, which actually starts from the lastkey
    ekey = pSupporter->rawEKey;

    if (QUERY_IS_ASC_QUERY(pQuery)) {
      assert(skey >= pSupporter->rawSKey);
      if (ekey < oldestKey || skey > pMeterObj->lastKeyOnFile) {
        continue;
      }
    } else {
      assert(skey <= pSupporter->rawSKey);
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
    if (validateCompBlockOffset(pQInfo, pMeterObj, compHeader, &pRuntimeEnv->vnodeFileInfo, compHeaderOffset) !=
        TSDB_CODE_SUCCESS) {
      free(buf);
      *numOfMeters = 0;

      return TSDB_CODE_FILE_CORRUPTED;
    }

    pOneMeterDataInfo->offsetInHeaderFile = (uint64_t)compHeader->compInfoOffset;

    if (pOneMeterDataInfo->pMeterQInfo == NULL) {
      pOneMeterDataInfo->pMeterQInfo =
          createMeterQueryInfo(pSupporter, pMeterObj->sid, pSupporter->rawSKey, pSupporter->rawEKey);
    }

    (*pReqMeterDataInfo)[*numOfMeters] = pOneMeterDataInfo;
    (*numOfMeters) += 1;
  }

  assert(*numOfMeters <= pSidSet->numOfSids);

  /* enable sequentially access*/
  if (*numOfMeters > 1) {
    qsort((*pReqMeterDataInfo), *numOfMeters, POINTER_BYTES, offsetComparator);
  }

  free(buf);

  return TSDB_CODE_SUCCESS;
}

SMeterQueryInfo *createMeterQueryInfo(STableQuerySupportObj *pSupporter, int32_t sid, TSKEY skey, TSKEY ekey) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;

  SMeterQueryInfo *pMeterQueryInfo = calloc(1, sizeof(SMeterQueryInfo));

  pMeterQueryInfo->skey = skey;
  pMeterQueryInfo->ekey = ekey;
  pMeterQueryInfo->lastKey = skey;

  pMeterQueryInfo->sid = sid;
  pMeterQueryInfo->cur.vnodeIndex = -1;

  initWindowResInfo(&pMeterQueryInfo->windowResInfo, pRuntimeEnv, 100, 100, TSDB_DATA_TYPE_INT);
  return pMeterQueryInfo;
}

void destroyMeterQueryInfo(SMeterQueryInfo *pMeterQueryInfo, int32_t numOfCols) {
  if (pMeterQueryInfo == NULL) {
    return;
  }

  cleanupTimeWindowInfo(&pMeterQueryInfo->windowResInfo, numOfCols);
  free(pMeterQueryInfo);
}

void changeMeterQueryInfoForSuppleQuery(SQuery *pQuery, SMeterQueryInfo *pMeterQueryInfo, TSKEY skey, TSKEY ekey) {
  if (pMeterQueryInfo == NULL) {
    return;
  }

  // order has change already!
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pQuery->order.order);
  if (!QUERY_IS_ASC_QUERY(pQuery)) {
    assert(pMeterQueryInfo->ekey >= pMeterQueryInfo->lastKey + step);
  } else {
    assert(pMeterQueryInfo->ekey <= pMeterQueryInfo->lastKey + step);
  }

  pMeterQueryInfo->ekey = pMeterQueryInfo->lastKey + step;

  SWAP(pMeterQueryInfo->skey, pMeterQueryInfo->ekey, TSKEY);
  pMeterQueryInfo->lastKey = pMeterQueryInfo->skey;

  pMeterQueryInfo->cur.order = pMeterQueryInfo->cur.order ^ 1u;
  pMeterQueryInfo->cur.vnodeIndex = -1;
}

void restoreIntervalQueryRange(SQueryRuntimeEnv *pRuntimeEnv, SMeterQueryInfo *pMeterQueryInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  pQuery->skey = pMeterQueryInfo->skey;
  pQuery->ekey = pMeterQueryInfo->ekey;
  pQuery->lastKey = pMeterQueryInfo->lastKey;

  assert(((pQuery->lastKey >= pQuery->skey) && QUERY_IS_ASC_QUERY(pQuery)) ||
         ((pQuery->lastKey <= pQuery->skey) && !QUERY_IS_ASC_QUERY(pQuery)));
}

/**
 * set output buffer for different group
 * @param pRuntimeEnv
 * @param pDataBlockInfoEx
 */
void setExecutionContext(STableQuerySupportObj *pSupporter, SMeterQueryInfo *pMeterQueryInfo, int32_t meterIdx,
                         int32_t groupIdx, TSKEY nextKey) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SWindowResInfo *  pWindowResInfo = &pRuntimeEnv->windowResInfo;
  int32_t           GROUPRESULTID = 1;

  SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, pWindowResInfo, (char *)&groupIdx, sizeof(groupIdx));
  if (pWindowRes == NULL) {
    return;
  }

  /*
   * not assign result buffer yet, add new result buffer
   * all group belong to one result set, and each group result has different group id so set the id to be one
   */
  if (pWindowRes->pos.pageId == -1) {
    if (addNewWindowResultBuf(pWindowRes, pRuntimeEnv->pResultBuf, GROUPRESULTID, pRuntimeEnv->numOfRowsPerPage) !=
        TSDB_CODE_SUCCESS) {
      return;
    }
  }

  setWindowResOutputBuf(pRuntimeEnv, pWindowRes);
  initCtxOutputBuf(pRuntimeEnv);

  pMeterQueryInfo->lastKey = nextKey;
  setAdditionalInfo(pSupporter, meterIdx, pMeterQueryInfo);
}

static void setWindowResOutputBuf(SQueryRuntimeEnv *pRuntimeEnv, SWindowResult *pResult) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // Note: pResult->pos[i]->numOfElems == 0, there is only fixed number of results for each group
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    SQLFunctionCtx *pCtx = &pRuntimeEnv->pCtx[i];
    pCtx->aOutputBuf = getPosInResultPage(pRuntimeEnv, i, pResult);

    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF) {
      pCtx->ptsOutputBuf = pRuntimeEnv->pCtx[0].aOutputBuf;
    }

    /*
     * set the output buffer information and intermediate buffer
     * not all queries require the interResultBuf, such as COUNT
     */
    pCtx->resultInfo = &pResult->resultInfo[i];

    // set super table query flag
    SResultInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->superTableQ = pRuntimeEnv->stableQuery;
  }
}

int32_t setAdditionalInfo(STableQuerySupportObj *pSupporter, int32_t meterIdx, SMeterQueryInfo *pMeterQueryInfo) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  assert(pMeterQueryInfo->lastKey > 0);

  vnodeSetTagValueInParam(pSupporter->pSidSet, pRuntimeEnv, pSupporter->pMeterSidExtInfo[meterIdx]);

  // both the master and supplement scan needs to set the correct ts comp start position
  if (pRuntimeEnv->pTSBuf != NULL) {
    if (pMeterQueryInfo->cur.vnodeIndex == -1) {
      pMeterQueryInfo->tag = pRuntimeEnv->pCtx[0].tag.i64Key;

      tsBufGetElemStartPos(pRuntimeEnv->pTSBuf, 0, pMeterQueryInfo->tag);

      // keep the cursor info of current meter
      pMeterQueryInfo->cur = pRuntimeEnv->pTSBuf->cur;
    } else {
      tsBufSetCursor(pRuntimeEnv->pTSBuf, &pMeterQueryInfo->cur);
    }
  }

  return 0;
}

/*
 * There are two cases to handle:
 *
 * 1. Query range is not set yet (queryRangeSet = 0). we need to set the query range info, including pQuery->lastKey,
 *    pQuery->skey, and pQuery->eKey.
 * 2. Query range is set and query is in progress. There may be another result with the same query ranges to be
 *    merged during merge stage. In this case, we need the pMeterQueryInfo->lastResRows to decide if there
 *    is a previous result generated or not.
 */
void setIntervalQueryRange(SMeterQueryInfo *pMeterQueryInfo, STableQuerySupportObj *pSupporter, TSKEY key) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  if (pMeterQueryInfo->queryRangeSet) {
    pQuery->lastKey = key;
    pMeterQueryInfo->lastKey = key;
  } else {
    pQuery->skey = key;
    STimeWindow win = {.skey = key, pSupporter->rawEKey};

    // for too small query range, no data in this interval.
    if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->ekey < pQuery->skey)) ||
        (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->skey < pQuery->ekey))) {
      return;
    }

    /**
     * In handling the both ascending and descending order super table query, we need to find the first qualified
     * timestamp of this table, and then set the first qualified start timestamp.
     * In ascending query, key is the first qualified timestamp. However, in the descending order query, additional
     * operations involve.
     */
    if (!QUERY_IS_ASC_QUERY(pQuery)) {
      TSKEY k = getGreaterEqualTimestamp(pRuntimeEnv);
      win.skey = k;
      win.ekey = key;  // current key is the last timestamp value that are contained in query time window

      SPositionInfo p = {.fileId = pQuery->fileId, .slot = pQuery->slot, .pos = pQuery->pos};
      loadRequiredBlockIntoMem(pRuntimeEnv, &p);
    }

    TSKEY skey1, ekey1;
    TSKEY windowSKey = 0, windowEKey = 0;

    SWindowResInfo *pWindowResInfo = &pMeterQueryInfo->windowResInfo;

    doGetAlignedIntervalQueryRangeImpl(pQuery, win.skey, win.skey, win.ekey, &skey1, &ekey1, &windowSKey, &windowEKey);
    pWindowResInfo->startTime = windowSKey;  // windowSKey may be 0 in case of 1970 timestamp
    //    assert(pWindowResInfo->startTime > 0);

    if (pWindowResInfo->prevSKey == 0) {
      if (QUERY_IS_ASC_QUERY(pQuery)) {
        pWindowResInfo->prevSKey = windowSKey;
      } else {
        assert(win.ekey == pQuery->skey);
        pWindowResInfo->prevSKey = windowSKey + ((win.ekey - windowSKey) / pQuery->slidingTime) * pQuery->slidingTime;
      }
    }

    pMeterQueryInfo->queryRangeSet = 1;
    pMeterQueryInfo->lastKey = pQuery->skey;
    pMeterQueryInfo->skey = pQuery->skey;

    pQuery->lastKey = pQuery->skey;
  }
}

bool requireTimestamp(SQuery *pQuery) {
  for (int32_t i = 0; i < pQuery->numOfOutputCols; i++) {
    int32_t functionId = pQuery->pSelectExpr[i].pBase.functionId;
    if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_NEED_TS) != 0) {
      return true;
    }
  }
  return false;
}

bool needPrimaryTimestampCol(SQuery *pQuery, SDataBlockInfo *pDataBlockInfo) {
  /*
   * 1. if skey or ekey locates in this block, we need to load the timestamp column to decide the precise position
   * 2. if there are top/bottom, first_dst/last_dst functions, we need to load timestamp column in any cases;
   */
  STimeWindow* w = &pDataBlockInfo->window;
  bool loadPrimaryTS = (pQuery->lastKey >= w->skey && pQuery->lastKey <= w->ekey) ||
                       (pQuery->ekey >= w->skey && pQuery->ekey <= w->ekey) || requireTimestamp(pQuery);

  return loadPrimaryTS;
}

bool onDemandLoadDatablock(SQuery *pQuery, int16_t queryRangeSet) {
  return (pQuery->intervalTime == 0) || ((queryRangeSet == 1) && (isIntervalQuery(pQuery)));
}

static int32_t getNumOfSubset(STableQuerySupportObj *pSupporter) {
  SQuery *pQuery = pSupporter->runtimeEnv.pQuery;

  int32_t totalSubset = 0;
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || (isIntervalQuery(pQuery))) {
    totalSubset = numOfClosedTimeWindow(&pSupporter->runtimeEnv.windowResInfo);
  } else {
    totalSubset = pSupporter->pSidSet->numOfSubSet;
  }

  return totalSubset;
}

static int32_t doCopyToSData(STableQuerySupportObj *pSupporter, SWindowResult *result, int32_t orderType) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  int32_t numOfResult = 0;
  int32_t startIdx = 0;
  int32_t step = -1;

  dTrace("QInfo:%p start to copy data from windowResInfo to pQuery buf", GET_QINFO_ADDR(pQuery));
  int32_t totalSubset = getNumOfSubset(pSupporter);

  if (orderType == TSQL_SO_ASC) {
    startIdx = pSupporter->subgroupIdx;
    step = 1;
  } else {  // desc order copy all data
    startIdx = totalSubset - pSupporter->subgroupIdx - 1;
    step = -1;
  }

  for (int32_t i = startIdx; (i < totalSubset) && (i >= 0); i += step) {
    if (result[i].numOfRows == 0) {
      pSupporter->offset = 0;
      pSupporter->subgroupIdx += 1;
      continue;
    }

    assert(result[i].numOfRows >= 0 && pSupporter->offset <= 1);

    int32_t numOfRowsToCopy = result[i].numOfRows - pSupporter->offset;
    int32_t oldOffset = pSupporter->offset;

    /*
     * current output space is not enough to keep all the result data of this group, only copy partial results
     * to SQuery object's result buffer
     */
    if (numOfRowsToCopy > pQuery->pointsToRead - numOfResult) {
      numOfRowsToCopy = pQuery->pointsToRead - numOfResult;
      pSupporter->offset += numOfRowsToCopy;
    } else {
      pSupporter->offset = 0;
      pSupporter->subgroupIdx += 1;
    }

    for (int32_t j = 0; j < pQuery->numOfOutputCols; ++j) {
      int32_t size = pRuntimeEnv->pCtx[j].outputBytes;

      char *out = pQuery->sdata[j]->data + numOfResult * size;
      char *in = getPosInResultPage(pRuntimeEnv, j, &result[i]);
      memcpy(out, in + oldOffset * size, size * numOfRowsToCopy);
    }

    numOfResult += numOfRowsToCopy;
    if (numOfResult == pQuery->pointsToRead) {
      break;
    }
  }

  dTrace("QInfo:%p copy data to SQuery buf completed", GET_QINFO_ADDR(pQuery));

#ifdef _DEBUG_VIEW
  displayInterResult(pQuery->sdata, pQuery, numOfResult);
#endif
  return numOfResult;
}

/**
 * copyFromWindowResToSData support copy data in ascending/descending order
 * For interval query of both super table and table, copy the data in ascending order, since the output results are
 * ordered in SWindowResutl already. While handling the group by query for both table and super table,
 * all group result are completed already.
 *
 * @param pQInfo
 * @param result
 */
void copyFromWindowResToSData(SQInfo *pQInfo, SWindowResult *result) {
  SQuery *               pQuery = &pQInfo->query;
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  int32_t orderType = (pQuery->pGroupbyExpr != NULL) ? pQuery->pGroupbyExpr->orderType : TSQL_SO_ASC;
  int32_t numOfResult = doCopyToSData(pSupporter, result, orderType);

  pQuery->pointsRead += numOfResult;
  assert(pQuery->pointsRead <= pQuery->pointsToRead);
}

static void updateWindowResNumOfRes(SQueryRuntimeEnv *pRuntimeEnv, SMeterDataInfo *pMeterDataInfo) {
  SQuery *pQuery = pRuntimeEnv->pQuery;

  // update the number of result for each, only update the number of rows for the corresponding window result.
  if (pQuery->intervalTime == 0) {
    int32_t g = pMeterDataInfo->groupIdx;
    assert(pRuntimeEnv->windowResInfo.size > 0);

    SWindowResult *pWindowRes = doSetTimeWindowFromKey(pRuntimeEnv, &pRuntimeEnv->windowResInfo, (char *)&g, sizeof(g));
    if (pWindowRes->numOfRows == 0) {
      pWindowRes->numOfRows = getNumOfResult(pRuntimeEnv);
    }
  }
}

void stableApplyFunctionsOnBlock(STableQuerySupportObj *pSupporter, SMeterDataInfo *pMeterDataInfo,
                                 SBlockInfo *pDataBlockInfo, SField *pFields, __block_search_fn_t searchFn) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;
  SMeterQueryInfo * pMeterQueryInfo = pMeterDataInfo->pMeterQInfo;
  SWindowResInfo *  pWindowResInfo = &pMeterQueryInfo->windowResInfo;

  int64_t *pPrimaryKey = (int64_t *)pRuntimeEnv->primaryColBuffer->data;

  int32_t forwardStep =
      getNumOfRowsInTimeWindow(pQuery, pDataBlockInfo, pPrimaryKey, pQuery->pos, pQuery->ekey, searchFn, true);

  int32_t numOfRes = 0;
  if (pQuery->numOfFilterCols > 0 || pRuntimeEnv->pTSBuf != NULL) {
    numOfRes = rowwiseApplyAllFunctions(pRuntimeEnv, &forwardStep, pFields, pDataBlockInfo, pWindowResInfo);
  } else {
//    numOfRes = blockwiseApplyAllFunctions(pRuntimeEnv, forwardStep, pFields, pDataBlockInfo, pWindowResInfo, searchFn);
  }

  assert(numOfRes >= 0);

  updateWindowResNumOfRes(pRuntimeEnv, pMeterDataInfo);
  updatelastkey(pQuery, pMeterQueryInfo);
}

// we need to split the refstatsult into different packages.
int32_t vnodeGetResultSize(void *thandle, int32_t *numOfRows) {
  SQInfo *pQInfo = (SQInfo *)thandle;
  SQuery *pQuery = &pQInfo->query;

  /*
   * get the file size and set the numOfRows to be the file size, since for tsComp query,
   * the returned row size is equalled to 1
   *
   * TODO handle the case that the file is too large to send back one time
   */
  if (pQInfo->pTableQuerySupporter != NULL && isTSCompQuery(pQuery) && (*numOfRows) > 0) {
    struct stat fstat;
    if (stat(pQuery->sdata[0]->data, &fstat) == 0) {
      *numOfRows = fstat.st_size;
      return fstat.st_size;
    } else {
      dError("QInfo:%p failed to get file info, path:%s, reason:%s", pQInfo, pQuery->sdata[0]->data, strerror(errno));
      return 0;
    }
  } else {
    return pQInfo->query.rowSize * (*numOfRows);
  }
}

int64_t vnodeGetOffsetVal(void *thandle) {
  SQInfo *pQInfo = (SQInfo *)thandle;
  return pQInfo->query.limit.offset;
}

bool vnodeHasRemainResults(void *handle) {
  SQInfo *               pQInfo = (SQInfo *)handle;
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;

  if (pSupporter == NULL || pQInfo->query.interpoType == TSDB_INTERPO_NONE) {
    return false;
  }

  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *          pQuery = pRuntimeEnv->pQuery;

  SInterpolationInfo *pInterpoInfo = &pRuntimeEnv->interpoInfo;
  if (pQuery->limit.limit > 0 && pQInfo->pointsRead >= pQuery->limit.limit) {
    return false;
  }

  int32_t remain = taosNumOfRemainPoints(pInterpoInfo);
  if (remain > 0) {
    return true;
  } else {
    if (pRuntimeEnv->pInterpoBuf == NULL) {
      return false;
    }

    // query has completed
    if (Q_STATUS_EQUAL(pQuery->over, QUERY_COMPLETED | QUERY_NO_DATA_TO_CHECK)) {
      TSKEY   ekey = taosGetRevisedEndKey(pSupporter->rawEKey, pQuery->order.order, pQuery->intervalTime,
                                        pQuery->intervalTimeUnit, pQuery->precision);
      int32_t numOfTotal = taosGetNumOfResultWithInterpo(pInterpoInfo, (TSKEY *)pRuntimeEnv->pInterpoBuf[0]->data,
                                                         remain, pQuery->intervalTime, ekey, pQuery->pointsToRead);
      return numOfTotal > 0;
    }

    return false;
  }
}

static int32_t resultInterpolate(SQInfo *pQInfo, tFilePage **data, tFilePage **pDataSrc, int32_t numOfRows,
                                 int32_t outputRows) {
  SQuery *          pQuery = &pQInfo->query;
  SQueryRuntimeEnv *pRuntimeEnv = &pQInfo->pTableQuerySupporter->runtimeEnv;

  assert(pRuntimeEnv->pCtx[0].outputBytes == TSDB_KEYSIZE);

  // build support structure for performing interpolation
  SSchema *pSchema = calloc(1, sizeof(SSchema) * pQuery->numOfOutputCols);
  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    pSchema[i].bytes = pRuntimeEnv->pCtx[i].outputBytes;
    pSchema[i].type = pQuery->pSelectExpr[i].resType;
  }

  SColumnModel *pModel = createColumnModel(pSchema, pQuery->numOfOutputCols, pQuery->pointsToRead);

  char *  srcData[TSDB_MAX_COLUMNS] = {0};
  int32_t functions[TSDB_MAX_COLUMNS] = {0};

  for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
    srcData[i] = pDataSrc[i]->data;
    functions[i] = pQuery->pSelectExpr[i].pBase.functionId;
  }

  int32_t numOfRes = taosDoInterpoResult(&pRuntimeEnv->interpoInfo, pQuery->interpoType, data, numOfRows, outputRows,
                                         pQuery->intervalTime, (int64_t *)pDataSrc[0]->data, pModel, srcData,
                                         pQuery->defaultVal, functions, pRuntimeEnv->pMeterObj->pointsPerFileBlock);

  destroyColumnModel(pModel);
  free(pSchema);

  return numOfRes;
}

static void doCopyQueryResultToMsg(SQInfo *pQInfo, int32_t numOfRows, char *data) {
  SMeterObj *pObj = pQInfo->pObj;
  SQuery *   pQuery = &pQInfo->query;

  int tnumOfRows = vnodeList[pObj->vnode].cfg.rowsInFileBlock;

  // for metric query, bufIndex always be 0.
  for (int32_t col = 0; col < pQuery->numOfOutputCols; ++col) {  // pQInfo->bufIndex == 0
    int32_t bytes = pQuery->pSelectExpr[col].resBytes;

    memmove(data, pQuery->sdata[col]->data + bytes * tnumOfRows * pQInfo->bufIndex, bytes * numOfRows);
    data += bytes * numOfRows;
  }
}

/**
 * Copy the result data/file to output message buffer.
 * If the result is in file format, read file from disk and copy to output buffer, compression is not involved since
 * data in file is already compressed.
 * In case of other result in buffer, compress the result before copy once the tsComressMsg is set.
 *
 * @param handle
 * @param data
 * @param numOfRows the number of rows that are not returned in current retrieve
 * @return
 */
int32_t vnodeCopyQueryResultToMsg(void *handle, char *data, int32_t numOfRows) {
  SQInfo *pQInfo = (SQInfo *)handle;
  SQuery *pQuery = &pQInfo->query;

  assert(pQuery->pSelectExpr != NULL && pQuery->numOfOutputCols > 0);

  // load data from file to msg buffer
  if (isTSCompQuery(pQuery)) {
    int32_t fd = open(pQuery->sdata[0]->data, O_RDONLY, 0666);

    // make sure file exist
    if (FD_VALID(fd)) {
      size_t s = lseek(fd, 0, SEEK_END);
      dTrace("QInfo:%p ts comp data return, file:%s, size:%zu", pQInfo, pQuery->sdata[0]->data, s);

      lseek(fd, 0, SEEK_SET);
      read(fd, data, s);
      close(fd);

      unlink(pQuery->sdata[0]->data);
    } else {
      dError("QInfo:%p failed to open tmp file to send ts-comp data to client, path:%s, reason:%s", pQInfo,
             pQuery->sdata[0]->data, strerror(errno));
    }
  } else {
    doCopyQueryResultToMsg(pQInfo, numOfRows, data);
  }

  return numOfRows;
}

int32_t vnodeQueryResultInterpolate(SQInfo *pQInfo, tFilePage **pDst, tFilePage **pDataSrc, int32_t numOfRows,
                                    int32_t *numOfInterpo) {
  STableQuerySupportObj *pSupporter = pQInfo->pTableQuerySupporter;
  SQueryRuntimeEnv *     pRuntimeEnv = &pSupporter->runtimeEnv;
  SQuery *               pQuery = pRuntimeEnv->pQuery;

  while (1) {
    numOfRows = taosNumOfRemainPoints(&pRuntimeEnv->interpoInfo);

    TSKEY   ekey = taosGetRevisedEndKey(pSupporter->rawEKey, pQuery->order.order, pQuery->intervalTime,
                                      pQuery->intervalTimeUnit, pQuery->precision);
    int32_t numOfFinalRows = taosGetNumOfResultWithInterpo(&pRuntimeEnv->interpoInfo, (TSKEY *)pDataSrc[0]->data,
                                                           numOfRows, pQuery->intervalTime, ekey, pQuery->pointsToRead);

    int32_t ret = resultInterpolate(pQInfo, pDst, pDataSrc, numOfRows, numOfFinalRows);
    assert(ret == numOfFinalRows);

    /* reached the start position of according to offset value, return immediately */
    if (pQuery->limit.offset == 0) {
      return ret;
    }

    if (pQuery->limit.offset < ret) {
      ret -= pQuery->limit.offset;
      // todo !!!!there exactly number of interpo is not valid.
      // todo refactor move to the beginning of buffer
      for (int32_t i = 0; i < pQuery->numOfOutputCols; ++i) {
        memmove(pDst[i]->data, pDst[i]->data + pQuery->pSelectExpr[i].resBytes * pQuery->limit.offset,
                ret * pQuery->pSelectExpr[i].resBytes);
      }
      pQuery->limit.offset = 0;
      return ret;
    } else {
      pQuery->limit.offset -= ret;
      ret = 0;
    }

    if (!vnodeHasRemainResults(pQInfo)) {
      return ret;
    }
  }
}

void vnodePrintQueryStatistics(STableQuerySupportObj *pSupporter) {
  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;

  SQuery *pQuery = pRuntimeEnv->pQuery;
  SQInfo *pQInfo = (SQInfo *)GET_QINFO_ADDR(pQuery);

  SQueryCostSummary *pSummary = &pRuntimeEnv->summary;
  if (pRuntimeEnv->pResultBuf == NULL) {
    pSummary->tmpBufferInDisk = 0;
  } else {
    pSummary->tmpBufferInDisk = getResBufSize(pRuntimeEnv->pResultBuf);
  }

  dTrace("QInfo:%p statis: comp blocks:%d, size:%d Bytes, elapsed time:%.2f ms", pQInfo, pSummary->readCompInfo,
         pSummary->totalCompInfoSize, pSummary->loadCompInfoUs / 1000.0);

  dTrace("QInfo:%p statis: field info: %d, size:%d Bytes, avg size:%.2f Bytes, elapsed time:%.2f ms", pQInfo,
         pSummary->readField, pSummary->totalFieldSize, (double)pSummary->totalFieldSize / pSummary->readField,
         pSummary->loadFieldUs / 1000.0);

  dTrace(
      "QInfo:%p statis: file blocks:%d, size:%d Bytes, elapsed time:%.2f ms, skipped:%d, in-memory gen null:%d Bytes",
      pQInfo, pSummary->readDiskBlocks, pSummary->totalBlockSize, pSummary->loadBlocksUs / 1000.0,
      pSummary->skippedFileBlocks, pSummary->totalGenData);

  dTrace("QInfo:%p statis: cache blocks:%d", pQInfo, pSummary->blocksInCache, 0);
  dTrace("QInfo:%p statis: temp file:%d Bytes", pQInfo, pSummary->tmpBufferInDisk);

  dTrace("QInfo:%p statis: file:%d, table:%d", pQInfo, pSummary->numOfFiles, pSummary->numOfTables);
  dTrace("QInfo:%p statis: seek ops:%d", pQInfo, pSummary->numOfSeek);

  double total = pSummary->fileTimeUs + pSummary->cacheTimeUs;
  double io = pSummary->loadCompInfoUs + pSummary->loadBlocksUs + pSummary->loadFieldUs;
  //    assert(io <= pSummary->fileTimeUs);

  // todo add the intermediate result save cost!!
  double computing = total - io;

  dTrace(
      "QInfo:%p statis: total elapsed time:%.2f ms, file:%.2f ms(%.2f%), cache:%.2f ms(%.2f%). io:%.2f ms(%.2f%),"
      "comput:%.2fms(%.2f%)",
      pQInfo, total / 1000.0, pSummary->fileTimeUs / 1000.0, pSummary->fileTimeUs * 100 / total,
      pSummary->cacheTimeUs / 1000.0, pSummary->cacheTimeUs * 100 / total, io / 1000.0, io * 100 / total,
      computing / 1000.0, computing * 100 / total);
}

int32_t vnodeQueryTablePrepare(SQInfo *pQInfo, SMeterObj *pMeterObj, STableQuerySupportObj *pSupporter, void *param) {
  SQuery *pQuery = &pQInfo->query;
  int32_t code = TSDB_CODE_SUCCESS;

  /*
   * only the successful complete requries the sem_post/over = 1 operations.
   */
  if ((QUERY_IS_ASC_QUERY(pQuery) && (pQuery->skey > pQuery->ekey)) ||
      (!QUERY_IS_ASC_QUERY(pQuery) && (pQuery->ekey > pQuery->skey))) {
    dTrace("QInfo:%p no result in time range %" PRId64 "-%" PRId64 ", order %d", pQInfo, pQuery->skey, pQuery->ekey,
           pQuery->order.order);

    sem_post(&pQInfo->dataReady);
    pQInfo->over = 1;
    return TSDB_CODE_SUCCESS;
  }

  setScanLimitationByResultBuffer(pQuery);
  changeExecuteScanOrder(pQuery, false);

  pQInfo->over = 0;
  pQInfo->pointsRead = 0;
  pQuery->pointsRead = 0;

  // dataInCache requires lastKey value
  pQuery->lastKey = pQuery->skey;

  STsdbQueryCond cond = {0};
  cond.window = (STimeWindow){.skey = pQuery->skey, .ekey = pQuery->ekey};
  cond.order = pQuery->order.order;

  cond.colList = *pQuery->colList;
  SArray *sa = taosArrayInit(1, POINTER_BYTES);
  taosArrayPush(sa, &pMeterObj);

  SArray *cols = taosArrayInit(pQuery->numOfCols, sizeof(pQuery->colList[0]));
  for (int32_t i = 0; i < pQuery->numOfCols; ++i) {
    taosArrayPush(cols, &pQuery->colList[i]);
  }

  pSupporter->runtimeEnv.pQueryHandle = tsdbQueryByTableId(&cond, sa, cols);

  SQueryRuntimeEnv *pRuntimeEnv = &pSupporter->runtimeEnv;
  pRuntimeEnv->pQuery = pQuery;
  pRuntimeEnv->pMeterObj = pMeterObj;

  pRuntimeEnv->pTSBuf = param;
  pRuntimeEnv->cur.vnodeIndex = -1;
  if (param != NULL) {
    int16_t order = (pQuery->order.order == pRuntimeEnv->pTSBuf->tsOrder) ? TSQL_SO_ASC : TSQL_SO_DESC;
    tsBufSetTraverseOrder(pRuntimeEnv->pTSBuf, order);
  }

  // create runtime environment
  code = setupQueryRuntimeEnv(pMeterObj, pQuery, &pSupporter->runtimeEnv, NULL, pQuery->order.order, false);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pRuntimeEnv->numOfRowsPerPage = getNumOfRowsInResultPage(pQuery, false);
  if (isGroupbyNormalCol(pQuery->pGroupbyExpr) || isIntervalQuery(pQuery)) {
    int32_t rows = getInitialPageNum(pSupporter);

    code = createDiskbasedResultBuffer(&pRuntimeEnv->pResultBuf, rows, pQuery->rowSize);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    int16_t type = TSDB_DATA_TYPE_NULL;
    if (isGroupbyNormalCol(pQuery->pGroupbyExpr)) {
      type = getGroupbyColumnType(pQuery, pQuery->pGroupbyExpr);
    } else {
      type = TSDB_DATA_TYPE_TIMESTAMP;
    }

    initWindowResInfo(&pRuntimeEnv->windowResInfo, pRuntimeEnv, rows, 4096, type);
  }

  pSupporter->rawSKey = pQuery->skey;
  pSupporter->rawEKey = pQuery->ekey;

  /* query on single table */
  pSupporter->numOfMeters = 1;
  setQueryStatus(pQuery, QUERY_NOT_COMPLETED);

  SPointInterpoSupporter interpInfo = {0};
  pointInterpSupporterInit(pQuery, &interpInfo);

  /*
   * in case of last_row query without query range, we set the query timestamp to
   * pMeterObj->lastKey. Otherwise, keep the initial query time range unchanged.
   */
  if (isFirstLastRowQuery(pQuery) && notHasQueryTimeRange(pQuery)) {
    if (!normalizeUnBoundLastRowQuery(pSupporter, &interpInfo)) {
      sem_post(&pQInfo->dataReady);
      pQInfo->over = 1;

      pointInterpSupporterDestroy(&interpInfo);
      return TSDB_CODE_SUCCESS;
    }
  } else {  // find the skey and ekey in case of sliding query
            //    if (isIntervalQuery(pQuery)) {
            //      STimeWindow win = {0};
            //
            //      // find the minimum value for descending order query
            //      TSKEY minKey = -1;
            //      if (!QUERY_IS_ASC_QUERY(pQuery)) {
            //        minKey = getGreaterEqualTimestamp(pRuntimeEnv);
            //      }
            //
            //      int64_t skey = 0;
            //      if ((normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &interpInfo, &skey) == false) ||
            //          (isFixedOutputQuery(pQuery) && !isTopBottomQuery(pQuery) && (pQuery->limit.offset > 0)) ||
    //          (isTopBottomQuery(pQuery) && pQuery->limit.offset >= pQuery->pSelectExpr[1].pBase.arg[0].argValue.i64))
    //          {
    //        sem_post(&pQInfo->dataReady);
    //        pQInfo->over = 1;
    //
    //        pointInterpSupporterDestroy(&interpInfo);
    //        return TSDB_CODE_SUCCESS;
    //      }
    //
    //      if (!QUERY_IS_ASC_QUERY(pQuery)) {
    //        win.skey = minKey;
    //        win.ekey = skey;
    //      } else {
    //        win.skey = skey;
    //        win.ekey = pQuery->ekey;
    //      }
    //
    //      TSKEY skey1, ekey1;
    //      TSKEY windowSKey = 0, windowEKey = 0;
    //
    //      doGetAlignedIntervalQueryRangeImpl(pQuery, win.skey, win.skey, win.ekey, &skey1, &ekey1, &windowSKey,
    //                                         &windowEKey);
    //      pRuntimeEnv->windowResInfo.startTime = windowSKey;
    //
    //      if (QUERY_IS_ASC_QUERY(pQuery)) {
    //        pRuntimeEnv->windowResInfo.prevSKey = windowSKey;
    //      } else {
    //        pRuntimeEnv->windowResInfo.prevSKey =
    //            windowSKey + ((win.ekey - windowSKey) / pQuery->slidingTime) * pQuery->slidingTime;
    //      }
    //
    //      pQuery->over = QUERY_NOT_COMPLETED;
    //    } else {
    //      int64_t ekey = 0;
    //      if ((normalizedFirstQueryRange(dataInDisk, dataInCache, pSupporter, &interpInfo, &ekey) == false) ||
    //          (isFixedOutputQuery(pQuery) && !isTopBottomQuery(pQuery) && (pQuery->limit.offset > 0)) ||
    //          (isTopBottomQuery(pQuery) && pQuery->limit.offset >= pQuery->pSelectExpr[1].pBase.arg[0].argValue.i64))
    //          {
    //        sem_post(&pQInfo->dataReady);
    //        pQInfo->over = 1;
    //
    //        pointInterpSupporterDestroy(&interpInfo);
    //        return TSDB_CODE_SUCCESS;
    //      }
    //    }
  }

  /*
   * here we set the value for before and after the specified time into the
   * parameter for interpolation query
   */
  pointInterpSupporterSetData(pQInfo, &interpInfo);
  pointInterpSupporterDestroy(&interpInfo);

  //  if (!forwardQueryStartPosIfNeeded(pQInfo, pSupporter, dataInDisk, dataInCache)) {
  //    return TSDB_CODE_SUCCESS;
  //  }

  int64_t rs = taosGetIntervalStartTimestamp(pSupporter->rawSKey, pQuery->intervalTime, pQuery->intervalTimeUnit,
                                             pQuery->precision);
  taosInitInterpoInfo(&pRuntimeEnv->interpoInfo, pQuery->order.order, rs, 0, 0);
  allocMemForInterpo(pSupporter, pQuery, pMeterObj);

  if (!isPointInterpoQuery(pQuery)) {
    //    assert(pQuery->pos >= 0 && pQuery->slot >= 0);
  }

  // the pQuery->skey is changed during normalizedFirstQueryRange, so set the newest lastkey value
  pQuery->lastKey = pQuery->skey;
  pRuntimeEnv->stableQuery = false;

  return TSDB_CODE_SUCCESS;
}
