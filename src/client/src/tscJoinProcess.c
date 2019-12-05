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
#include "tcache.h"
#include "tscJoinProcess.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tscompression.h"
#include "ttime.h"
#include "tutil.h"

static UNUSED_FUNC bool isSubqueryCompleted(SSqlObj* pSql) {
  bool hasData = true;
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlRes* pRes = &pSql->pSubs[i]->res;

    // in case inner join, if any subquery exhausted, query completed
    if (pRes->numOfRows == 0) {
      hasData = false;
      break;
    }
  }

  return hasData;
}

static bool doCompare(int32_t order, int64_t left, int64_t right) {
  if (order == TSQL_SO_ASC) {
    return left < right;
  } else {
    return left > right;
  }
}

static int64_t doTSBlockIntersect(SSqlObj* pSql, SJoinSubquerySupporter* pSupporter1, SJoinSubquerySupporter* pSupporter2,
                               TSKEY* st, TSKEY* et) {
  STSBuf* output1 = tsBufCreate(true);
  STSBuf* output2 = tsBufCreate(true);

  *st = INT64_MAX;
  *et = INT64_MIN;

  SLimitVal* pLimit = &pSql->cmd.limit;
  int32_t    order = pSql->cmd.order.order;

  pSql->pSubs[0]->cmd.tsBuf = output1;
  pSql->pSubs[1]->cmd.tsBuf = output2;

  tsBufResetPos(pSupporter1->pTSBuf);
  tsBufResetPos(pSupporter2->pTSBuf);

  // TODO add more details information
  if (!tsBufNextPos(pSupporter1->pTSBuf)) {
    tsBufFlush(output1);
    tsBufFlush(output2);

    tscTrace("%p input1 is empty, 0 for secondary query after ts blocks intersecting", pSql);
    return 0;
  }

  if (!tsBufNextPos(pSupporter2->pTSBuf)) {
    tsBufFlush(output1);
    tsBufFlush(output2);

    tscTrace("%p input2 is empty, 0 for secondary query after ts blocks intersecting", pSql);
    return 0;
  }

  int64_t numOfInput1 = 1;
  int64_t numOfInput2 = 1;

  while (1) {
    STSElem elem1 = tsBufGetElem(pSupporter1->pTSBuf);
    STSElem elem2 = tsBufGetElem(pSupporter2->pTSBuf);

#ifdef _DEBUG_VIEW
    // for debug purpose
    tscPrint("%lld, tags:%d \t %lld, tags:%d", elem1.ts, elem1.tag, elem2.ts, elem2.tag);
#endif

    if (elem1.tag < elem2.tag || (elem1.tag == elem2.tag && doCompare(order, elem1.ts, elem2.ts))) {
      if (!tsBufNextPos(pSupporter1->pTSBuf)) {
        break;
      }

      numOfInput1++;
    } else if (elem1.tag > elem2.tag || (elem1.tag == elem2.tag && doCompare(order, elem2.ts, elem1.ts))) {
      if (!tsBufNextPos(pSupporter2->pTSBuf)) {
        break;
      }

      numOfInput2++;
    } else {
      if (*st > elem1.ts) {
        *st = elem1.ts;
      }

      if (*et < elem1.ts) {
        *et = elem1.ts;
      }

      // in case of stable query, limit/offset is not applied here
      if (pLimit->offset == 0 || pSql->cmd.nAggTimeInterval > 0 || QUERY_IS_STABLE_QUERY(pSql->cmd.type)) {
        tsBufAppend(output1, elem1.vnode, elem1.tag, (const char*)&elem1.ts, sizeof(elem1.ts));
        tsBufAppend(output2, elem2.vnode, elem2.tag, (const char*)&elem2.ts, sizeof(elem2.ts));
      } else {
        pLimit->offset -= 1;
      }

      if (!tsBufNextPos(pSupporter1->pTSBuf)) {
        break;
      }

      numOfInput1++;

      if (!tsBufNextPos(pSupporter2->pTSBuf)) {
        break;
      }

      numOfInput2++;
    }
  }

  /*
   * failed to set the correct ts order yet in two cases:
   * 1. only one element
   * 2. only one element for each tag.
   */
  if (output1->tsOrder == -1) {
    output1->tsOrder = TSQL_SO_ASC;
    output2->tsOrder = TSQL_SO_ASC;
  }

  tsBufFlush(output1);
  tsBufFlush(output2);

  tsBufDestory(pSupporter1->pTSBuf);
  tsBufDestory(pSupporter2->pTSBuf);

  tscTrace("%p input1:%lld, input2:%lld, %lld for secondary query after ts blocks intersecting",
           pSql, numOfInput1, numOfInput2, output1->numOfTotal);

  return output1->numOfTotal;
}

//todo handle failed to create sub query
SJoinSubquerySupporter* tscCreateJoinSupporter(SSqlObj* pSql, SSubqueryState* pState, /*int32_t* numOfComplete, int32_t* gc,*/ int32_t index) {
  SJoinSubquerySupporter* pSupporter = calloc(1, sizeof(SJoinSubquerySupporter));
  if (pSupporter == NULL) {
    return NULL;
  }

  pSupporter->pObj = pSql;
  pSupporter->hasMore = true;

  pSupporter->pState = pState;

  pSupporter->subqueryIndex = index;
  pSupporter->interval = pSql->cmd.nAggTimeInterval;
  pSupporter->limit = pSql->cmd.limit;

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, index);
  pSupporter->uid = pMeterMetaInfo->pMeterMeta->uid;

  getTmpfilePath("join-", pSupporter->path);
  pSupporter->f = fopen(pSupporter->path, "w");

  if (pSupporter->f == NULL) {
    tscError("%p failed to create tmp file:%s, reason:%s", pSql, pSupporter->path, strerror(errno));
  }

  return pSupporter;
}

void tscDestroyJoinSupporter(SJoinSubquerySupporter* pSupporter) {
  if (pSupporter == NULL) {
    return;
  }

  tfree(pSupporter->exprsInfo.pExprs);
  tscColumnBaseInfoDestroy(&pSupporter->colList);

  tscClearFieldInfo(&pSupporter->fieldsInfo);

  if (pSupporter->f != NULL) {
    fclose(pSupporter->f);
    unlink(pSupporter->path);
  }

  tscTagCondRelease(&pSupporter->tagCond);
  free(pSupporter);
}

/*
 * need the secondary query process
 * In case of count(ts)/count(*)/spread(ts) query, that are only applied to
 * primary timestamp column , the secondary query is not necessary
 *
 */
bool needSecondaryQuery(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  for (int32_t i = 0; i < pCmd->colList.numOfCols; ++i) {
    SColumnBase* pBase = tscColumnBaseInfoGet(&pCmd->colList, i);
    if (pBase->colIndex.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return true;
    }
  }

  return false;
}

/*
 * launch secondary stage query to fetch the result that contains timestamp in set
 */
int32_t tscLaunchSecondSubquery(SSqlObj* pSql) {
  // TODO not launch secondary stage query
  //  if (!needSecondaryQuery(pSql)) {
  //    return;
  //  }

  // sub query may not be necessary
  int32_t                 numOfSub = 0;
  SJoinSubquerySupporter* pSupporter = NULL;

  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    pSupporter = pSql->pSubs[i]->param;
    pSupporter->pState->numOfCompleted = 0;

    if (pSupporter->exprsInfo.numOfExprs > 0) {
      ++numOfSub;
    }
  }

  // scan all subquery, if one sub query has only ts, ignore it
  int32_t j = 0;
  tscTrace("%p start to launch secondary subqueries: %d", pSql, pSql->numOfSubs);

  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    pSupporter = pSub->param;
    pSupporter->pState->numOfTotal = numOfSub;

    if (pSupporter->exprsInfo.numOfExprs == 0) {
      tscDestroyJoinSupporter(pSupporter);
      taos_free_result(pSub);
      continue;
    }

    SSqlObj* pNew = createSubqueryObj(pSql, 0, (int16_t)i, tscJoinQueryCallback, pSupporter, NULL);
    if (pNew == NULL) {
      pSql->numOfSubs = i; //revise the number of subquery
      pSupporter->pState->numOfTotal = i;

      pSupporter->pState->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      tscDestroyJoinSupporter(pSupporter);

      return NULL;
    }

    tscFreeSqlCmdData(&pNew->cmd);

    pSql->pSubs[j++] = pNew;
    pNew->cmd.tsBuf = pSub->cmd.tsBuf;
    pSub->cmd.tsBuf = NULL;

    taos_free_result(pSub);

    // set the second stage sub query for join process
    pNew->cmd.type |= TSDB_QUERY_TYPE_JOIN_SEC_STAGE;

    pNew->cmd.nAggTimeInterval = pSupporter->interval;
    pNew->cmd.limit = pSupporter->limit;
    pNew->cmd.groupbyExpr = pSupporter->groupbyExpr;

    tscColumnBaseInfoCopy(&pNew->cmd.colList, &pSupporter->colList, 0);
    tscTagCondCopy(&pNew->cmd.tagCond, &pSupporter->tagCond);

    tscSqlExprCopy(&pNew->cmd.exprsInfo, &pSupporter->exprsInfo, pSupporter->uid);
    tscFieldInfoCopyAll(&pSupporter->fieldsInfo, &pNew->cmd.fieldsInfo);

    // add the ts function for interval query if it is missing
    if (pSupporter->exprsInfo.pExprs[0].functionId != TSDB_FUNC_TS && pNew->cmd.nAggTimeInterval > 0) {
      tscAddTimestampColumn(&pNew->cmd, TSDB_FUNC_TS, 0);
    }

    // todo refactor function name
    tscAddTimestampColumn(&pNew->cmd, TSDB_FUNC_TS, 0);
    tscFieldInfoCalOffset(&pNew->cmd);

    SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(&pNew->cmd, 0);

    // fetch the join tag column
    if (UTIL_METER_IS_METRIC(pMeterMetaInfo)) {
      SSqlExpr* pExpr = tscSqlExprGet(&pNew->cmd, 0);
      assert(pNew->cmd.tagCond.joinInfo.hasJoin);

      int16_t tagColIndex = tscGetJoinTagColIndexByUid(&pNew->cmd, pMeterMetaInfo->pMeterMeta->uid);
      pExpr->param[0].i64Key = tagColIndex;
      pExpr->numOfParams = 1;

      addRequiredTagColumn(&pNew->cmd, tagColIndex, 0);
    }

    tscProcessSql(pNew);
  }

  // revise the number of subs
  pSql->numOfSubs = j;

  return 0;
}

static void freeSubqueryObj(SSqlObj* pSql) {
  SSubqueryState* pState = NULL;

  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    if (pSql->pSubs[i] != NULL) {
      SJoinSubquerySupporter* p = pSql->pSubs[i]->param;
      pState = p->pState;

      tscDestroyJoinSupporter(p);

      if (pSql->pSubs[i]->res.code == TSDB_CODE_SUCCESS) {
        taos_free_result(pSql->pSubs[i]);
      }
    }
  }

  tfree(pState);
  pSql->numOfSubs = 0;
}

static void doQuitSubquery(SSqlObj* pParentSql) {
  freeSubqueryObj(pParentSql);

  tsem_wait(&pParentSql->emptyRspSem);
  tsem_wait(&pParentSql->emptyRspSem);

  tsem_post(&pParentSql->rspSem);
}

static void quitAllSubquery(SSqlObj* pSqlObj, SJoinSubquerySupporter* pSupporter) {
  if (atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1) >= pSupporter->pState->numOfTotal) {
    pSqlObj->res.code = abs(pSupporter->pState->code);
    tscError("%p all subquery return and query failed, global code:%d", pSqlObj, pSqlObj->res.code);

    doQuitSubquery(pSqlObj);
  }
}

// update the query time range according to the join results on timestamp
static void updateQueryTimeRange(SSqlObj* pSql, int64_t st, int64_t et) {
  assert(pSql->cmd.stime <= st && pSql->cmd.etime >= et);

  pSql->cmd.stime = st;
  pSql->cmd.etime = et;
}

static void joinRetrieveCallback(void* param, TAOS_RES* tres, int numOfRows) {
  SJoinSubquerySupporter* pSupporter = (SJoinSubquerySupporter*)param;
  SSqlObj*                pParentSql = pSupporter->pObj;

  SSqlObj* pSql = (SSqlObj*)tres;

  if ((pSql->cmd.type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) == 0) {
    if (pSupporter->pState->code != TSDB_CODE_SUCCESS) {
      tscError("%p abort query due to other subquery failure. code:%d, global code:%d", pSql, numOfRows,
               pSupporter->pState->code);

      quitAllSubquery(pParentSql, pSupporter);
      return;
    }

    if (numOfRows > 0) { // write the data into disk
      fwrite(pSql->res.data, pSql->res.numOfRows, 1, pSupporter->f);
      fflush(pSupporter->f);

      STSBuf* pBuf = tsBufCreateFromFile(pSupporter->path, true);
      if (pBuf == NULL) {
        tscError("%p invalid ts comp file from vnode, abort sub query, file size:%d", pSql, numOfRows);

        pSupporter->pState->code = TSDB_CODE_APP_ERROR;  // todo set the informative code
        quitAllSubquery(pParentSql, pSupporter);
        return;
      }

      if (pSupporter->pTSBuf == NULL) {
        tscTrace("%p create tmp file for ts block:%s", pSql, pBuf->path);
        pSupporter->pTSBuf = pBuf;
      } else {
        tsBufMerge(pSupporter->pTSBuf, pBuf, pSql->cmd.vnodeIdx);
        tsBufDestory(pBuf);
      }

      // open new file to save the result
      getTmpfilePath("ts-join", pSupporter->path);
      pSupporter->f = fopen(pSupporter->path, "w");
      pSql->res.row = pSql->res.numOfRows;

      taos_fetch_rows_a(tres, joinRetrieveCallback, param);
    } else if (numOfRows == 0) { // no data from this vnode anymore
      if (atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1) >= pSupporter->pState->numOfTotal) {

        if (pSupporter->pState->code != TSDB_CODE_SUCCESS) {
          tscTrace("%p sub:%p, numOfSub:%d, quit from further procedure due to other queries failure", pParentSql, tres,
          pSupporter->subqueryIndex);
          doQuitSubquery(pParentSql);
          return;
        }

        tscTrace("%p all subqueries retrieve ts complete, do ts block intersect", pParentSql);

        SJoinSubquerySupporter* p1 = pParentSql->pSubs[0]->param;
        SJoinSubquerySupporter* p2 = pParentSql->pSubs[1]->param;

        TSKEY st, et;

        int64_t num = doTSBlockIntersect(pParentSql, p1, p2, &st, &et);
        if (num <= 0) {  // no result during ts intersect
          tscTrace("%p free all sub SqlObj and quit", pParentSql);
          doQuitSubquery(pParentSql);
        } else {
          updateQueryTimeRange(pParentSql, st, et);
          tscLaunchSecondSubquery(pParentSql);
        }
      }
    } else {  // failure of sub query
      tscError("%p sub query failed, code:%d, index:%d", pSql, numOfRows, pSupporter->subqueryIndex);
      pSupporter->pState->code = numOfRows;

      quitAllSubquery(pParentSql, pSupporter);
      return;
    }

  } else {  // secondary stage retrieve, driven by taos_fetch_row or other functions
    if (numOfRows < 0) {
      pSupporter->pState->code = numOfRows;
      tscError("%p retrieve failed, code:%d, index:%d", pSql, numOfRows, pSupporter->subqueryIndex);
    }

    if (atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1) >= pSupporter->pState->numOfTotal) {
      tscTrace("%p secondary retrieve completed, global code:%d", tres, pParentSql->res.code);
      if (pSupporter->pState->code != TSDB_CODE_SUCCESS) {
        pParentSql->res.code = abs(pSupporter->pState->code);
        freeSubqueryObj(pParentSql);
      }

      tsem_post(&pParentSql->rspSem);
    }
  }
}

void tscFetchDatablockFromSubquery(SSqlObj* pSql) {
  int32_t numOfFetch = 0;

  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SJoinSubquerySupporter* pSupporter = (SJoinSubquerySupporter*)pSql->pSubs[i]->param;

    SSqlRes* pRes = &pSql->pSubs[i]->res;
    if (pRes->row >= pRes->numOfRows && pSupporter->hasMore) {
      numOfFetch++;
    }
  }

  if (numOfFetch > 0) {
    tscTrace("%p retrieve data from %d subqueries", pSql, numOfFetch);

    SJoinSubquerySupporter* pSupporter = (SJoinSubquerySupporter*)pSql->pSubs[0]->param;
    pSupporter->pState->numOfTotal = numOfFetch;  // wait for all subqueries completed
    pSupporter->pState->numOfCompleted = 0;

    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      SSqlObj* pSql1 = pSql->pSubs[i];

      SSqlRes* pRes1 = &pSql1->res;
      SSqlCmd* pCmd1 = &pSql1->cmd;

      pSupporter = (SJoinSubquerySupporter*)pSql1->param;

      // wait for all subqueries completed
      pSupporter->pState->numOfTotal = numOfFetch;
      if (pRes1->row >= pRes1->numOfRows && pSupporter->hasMore) {
        tscTrace("%p subquery:%p retrieve data from vnode, index:%d", pSql, pSql1, pSupporter->subqueryIndex);

        tscResetForNextRetrieve(pRes1);

        pSql1->fp = joinRetrieveCallback;

        if (pCmd1->command < TSDB_SQL_LOCAL) {
          pCmd1->command = (pCmd1->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
        }

        tscProcessSql(pSql1);
      }
    }

    // wait for all subquery completed
    tsem_wait(&pSql->rspSem);
  }
}

// all subqueries return, set the result output index
void tscSetupOutputColumnIndex(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  tscTrace("%p all subquery response, retrieve data", pSql);

  pRes->pColumnIndex = calloc(1, sizeof(SColumnIndex) * pCmd->fieldsInfo.numOfOutputCols);

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);

    int32_t tableIndexOfSub = -1;
    for (int32_t j = 0; j < pCmd->numOfTables; ++j) {
      SSqlObj* pSub = pSql->pSubs[j];

      SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(&pSub->cmd, 0);
      if (pMeterMetaInfo->pMeterMeta->uid == pExpr->uid) {
        tableIndexOfSub = j;
        break;
      }
    }

    SSqlCmd* pSubCmd = &pSql->pSubs[tableIndexOfSub]->cmd;

    for (int32_t k = 0; k < pSubCmd->exprsInfo.numOfExprs; ++k) {
      SSqlExpr* pSubExpr = tscSqlExprGet(pSubCmd, k);
      if (pExpr->functionId == pSubExpr->functionId && pExpr->colInfo.colId == pSubExpr->colInfo.colId) {
        pRes->pColumnIndex[i] = (SColumnIndex){.tableIndex = tableIndexOfSub, .columnIndex = k};
        break;
      }
    }
  }
}

void tscJoinQueryCallback(void* param, TAOS_RES* tres, int code) {
  SSqlObj* pSql = (SSqlObj*)tres;
  //  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);

  //  int32_t idx = pSql->cmd.vnodeIdx;

  //  SVnodeSidList *vnodeInfo = NULL;
  //  if (pMeterMetaInfo->pMetricMeta != NULL) {
  //    vnodeInfo = tscGetVnodeSidList(pMeterMetaInfo->pMetricMeta, idx - 1);
  //  }

  SJoinSubquerySupporter* pSupporter = (SJoinSubquerySupporter*)param;

  //  if (atomic_add_fetch_32(pSupporter->numOfComplete, 1) >=
  //      pSupporter->numOfTotal) {
  //    SSqlObj *pParentObj = pSupporter->pObj;
  //
  //    if ((pSql->cmd.type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) != 1) {
  //      int32_t num = 0;
  //      tscFetchDatablockFromSubquery(pParentObj);
  //      TSKEY* ts = tscGetQualifiedTSList(pParentObj, &num);
  //
  //      if (num <= 0) {
  //        // no qualified result
  //      }
  //
  //      tscLaunchSecondSubquery(pSql, ts, num);
  //    } else {

  //    }
  //  } else {
  if ((pSql->cmd.type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) != TSDB_QUERY_TYPE_JOIN_SEC_STAGE) {
    if (code != TSDB_CODE_SUCCESS) {  // direct call joinRetrieveCallback and set the error code
      joinRetrieveCallback(param, pSql, code);
    } else {  // first stage query, continue to retrieve data
      pSql->fp = joinRetrieveCallback;
      pSql->cmd.command = TSDB_SQL_FETCH;
      tscProcessSql(pSql);
    }

  } else {  // second stage join subquery
    SSqlObj* pParentSql = pSupporter->pObj;

    if (pSupporter->pState->code != TSDB_CODE_SUCCESS) {
      tscError("%p abort query due to other subquery failure. code:%d, global code:%d", pSql, code,
               pSupporter->pState->code);
      quitAllSubquery(pParentSql, pSupporter);

      return;
    }

    if (code != TSDB_CODE_SUCCESS) {
      tscError("%p sub query failed, code:%d, set global code:%d, index:%d", pSql, code, code,
               pSupporter->subqueryIndex);
      pSupporter->pState->code = code;  // todo set the informative code

      quitAllSubquery(pParentSql, pSupporter);
    } else {
      if (atomic_add_fetch_32(&pSupporter->pState->numOfCompleted, 1) >= pSupporter->pState->numOfTotal) {
        tscSetupOutputColumnIndex(pParentSql);

        if (pParentSql->fp == NULL) {
          tsem_wait(&pParentSql->emptyRspSem);
          tsem_wait(&pParentSql->emptyRspSem);

          tsem_post(&pParentSql->rspSem);
        } else {
          // set the command flag must be after the semaphore been correctly set.
          //    pPObj->cmd.command = TSDB_SQL_RETRIEVE_METRIC;
          //    if (pPObj->res.code == TSDB_CODE_SUCCESS) {
          //      (*pPObj->fp)(pPObj->param, pPObj, 0);
          //    } else {
          //      tscQueueAsyncRes(pPObj);
          //    }
          assert(0);
        }
      }
    }
  }
}

static int32_t getDataStartOffset() {
  return sizeof(STSBufFileHeader) + TS_COMP_FILE_VNODE_MAX * sizeof(STSVnodeBlockInfo);
}

static int32_t doUpdateVnodeInfo(STSBuf* pTSBuf, int64_t offset, STSVnodeBlockInfo* pVInfo) {
  if (offset < 0 || offset >= getDataStartOffset()) {
    return -1;
  }

  if (fseek(pTSBuf->f, offset, SEEK_SET) != 0) {
    return -1;
  }

  fwrite(pVInfo, sizeof(STSVnodeBlockInfo), 1, pTSBuf->f);
  return 0;
}

// update prev vnode length info in file
static void TSBufUpdateVnodeInfo(STSBuf* pTSBuf, int32_t index, STSVnodeBlockInfo* pBlockInfo) {
  int32_t offset = sizeof(STSBufFileHeader) + index * sizeof(STSVnodeBlockInfo);
  doUpdateVnodeInfo(pTSBuf, offset, pBlockInfo);
}

static STSBuf* allocResForTSBuf(STSBuf* pTSBuf) {
  const int32_t INITIAL_VNODEINFO_SIZE = 4;

  pTSBuf->numOfAlloc = INITIAL_VNODEINFO_SIZE;
  pTSBuf->pData = calloc(pTSBuf->numOfAlloc, sizeof(STSVnodeBlockInfoEx));
  if (pTSBuf->pData == NULL) {
    tsBufDestory(pTSBuf);
    return NULL;
  }

  pTSBuf->tsData.rawBuf = malloc(MEM_BUF_SIZE);
  if (pTSBuf->tsData.rawBuf == NULL) {
    tsBufDestory(pTSBuf);
    return NULL;
  }

  pTSBuf->bufSize = MEM_BUF_SIZE;
  pTSBuf->tsData.threshold = MEM_BUF_SIZE;
  pTSBuf->tsData.allocSize = MEM_BUF_SIZE;

  pTSBuf->assistBuf = malloc(MEM_BUF_SIZE);
  if (pTSBuf->assistBuf == NULL) {
    tsBufDestory(pTSBuf);
    return NULL;
  }

  pTSBuf->block.payload = malloc(MEM_BUF_SIZE);
  if (pTSBuf->block.payload == NULL) {
    tsBufDestory(pTSBuf);
    return NULL;
  }

  pTSBuf->fileSize += getDataStartOffset();
  return pTSBuf;
}

static int32_t STSBufUpdateHeader(STSBuf* pTSBuf, STSBufFileHeader* pHeader);

/**
 * todo error handling
 * support auto closeable tmp file
 * @param path
 * @return
 */
STSBuf* tsBufCreate(bool autoDelete) {
  STSBuf* pTSBuf = calloc(1, sizeof(STSBuf));
  if (pTSBuf == NULL) {
    return NULL;
  }

  getTmpfilePath("join", pTSBuf->path);
  pTSBuf->f = fopen(pTSBuf->path, "w+");
  if (pTSBuf->f == NULL) {
    free(pTSBuf);
    return NULL;
  }

  allocResForTSBuf(pTSBuf);

  // update the header info
  STSBufFileHeader header = {.magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = TSQL_SO_ASC};
  STSBufUpdateHeader(pTSBuf, &header);

  tsBufResetPos(pTSBuf);
  pTSBuf->cur.order = TSQL_SO_ASC;

  pTSBuf->autoDelete = autoDelete;
  pTSBuf->tsOrder = -1;

  return pTSBuf;
}

STSBuf* tsBufCreateFromFile(const char* path, bool autoDelete) {
  STSBuf* pTSBuf = calloc(1, sizeof(STSBuf));
  if (pTSBuf == NULL) {
    return NULL;
  }

  strncpy(pTSBuf->path, path, PATH_MAX);

  pTSBuf->f = fopen(pTSBuf->path, "r");
  if (pTSBuf->f == NULL) {
    return NULL;
  }

  if (allocResForTSBuf(pTSBuf) == NULL) {
    return NULL;
  }

  // validate the file magic number
  STSBufFileHeader header = {0};
  fseek(pTSBuf->f, 0, SEEK_SET);
  fread(&header, 1, sizeof(header), pTSBuf->f);

  // invalid file
  if (header.magic != TS_COMP_FILE_MAGIC) {
    return NULL;
  }

  if (header.numOfVnode > pTSBuf->numOfAlloc) {
    pTSBuf->numOfAlloc = header.numOfVnode;
    STSVnodeBlockInfoEx* tmp = realloc(pTSBuf->pData, sizeof(STSVnodeBlockInfoEx) * pTSBuf->numOfAlloc);
    if (tmp == NULL) {
      tsBufDestory(pTSBuf);
      return NULL;
    }

    pTSBuf->pData = tmp;
  }

  pTSBuf->numOfVnodes = header.numOfVnode;

  // check the ts order
  pTSBuf->tsOrder = header.tsOrder;
  if (pTSBuf->tsOrder != TSQL_SO_ASC && pTSBuf->tsOrder != TSQL_SO_DESC) {
    tscError("invalid order info in buf:%d", pTSBuf->tsOrder);
    tsBufDestory(pTSBuf);
    return NULL;
  }

  size_t infoSize = sizeof(STSVnodeBlockInfo) * pTSBuf->numOfVnodes;

  STSVnodeBlockInfo* buf = (STSVnodeBlockInfo*)calloc(1, infoSize);
  int64_t pos = ftell(pTSBuf->f);
  fread(buf, infoSize, 1, pTSBuf->f);

  // the length value for each vnode is not kept in file, so does not set the length value
  for (int32_t i = 0; i < pTSBuf->numOfVnodes; ++i) {
    STSVnodeBlockInfoEx* pBlockList = &pTSBuf->pData[i];
    memcpy(&pBlockList->info, &buf[i], sizeof(STSVnodeBlockInfo));
  }

  free(buf);

  fseek(pTSBuf->f, 0, SEEK_END);

  struct stat fileStat;
  fstat(fileno(pTSBuf->f), &fileStat);

  pTSBuf->fileSize = (uint32_t) fileStat.st_size;
  tsBufResetPos(pTSBuf);

  // ascending by default
  pTSBuf->cur.order = TSQL_SO_ASC;

  pTSBuf->autoDelete = autoDelete;
  return pTSBuf;
}

void tsBufDestory(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return;
  }

  tfree(pTSBuf->assistBuf);
  tfree(pTSBuf->tsData.rawBuf);

  tfree(pTSBuf->pData);
  tfree(pTSBuf->block.payload);

  fclose(pTSBuf->f);

  if (pTSBuf->autoDelete) {
    unlink(pTSBuf->path);
  }

  free(pTSBuf);
}

static STSVnodeBlockInfoEx* addOneVnodeInfo(STSBuf* pTSBuf, int32_t vnodeId) {
  if (pTSBuf->numOfAlloc <= pTSBuf->numOfVnodes) {
    uint32_t newSize = (uint32_t)(pTSBuf->numOfAlloc * 1.5);
    assert(newSize > pTSBuf->numOfAlloc);

    STSVnodeBlockInfoEx* tmp = (STSVnodeBlockInfoEx*)realloc(pTSBuf->pData, sizeof(STSVnodeBlockInfoEx) * newSize);
    if (tmp == NULL) {
      return NULL;
    }

    pTSBuf->pData = tmp;
    pTSBuf->numOfAlloc = newSize;
    memset(&pTSBuf->pData[pTSBuf->numOfVnodes], 0, sizeof(STSVnodeBlockInfoEx) * (newSize - pTSBuf->numOfVnodes));
  }

  if (pTSBuf->numOfVnodes > 0) {
    STSVnodeBlockInfo* pPrevBlockInfo = &pTSBuf->pData[pTSBuf->numOfVnodes - 1].info;

    // update prev vnode length info in file
    TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes - 1, pPrevBlockInfo);
  }

  // set initial value for vnode block
  STSVnodeBlockInfo* pBlockInfo = &pTSBuf->pData[pTSBuf->numOfVnodes].info;
  pBlockInfo->vnode = vnodeId;
  pBlockInfo->offset = pTSBuf->fileSize;
  assert(pBlockInfo->offset >= getDataStartOffset());

  // update vnode info in file
  TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes, pBlockInfo);

  // add one vnode info
  pTSBuf->numOfVnodes += 1;

  // update the header info
  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = pTSBuf->tsOrder};
  STSBufUpdateHeader(pTSBuf, &header);

  return &pTSBuf->pData[pTSBuf->numOfVnodes - 1];
}

static void shrinkBuffer(STSList* ptsData) {
  // shrink tmp buffer size if it consumes too many memory compared to the pre-defined size
  if (ptsData->allocSize >= ptsData->threshold * 2) {
    ptsData->rawBuf = realloc(ptsData->rawBuf, MEM_BUF_SIZE);
    ptsData->allocSize = MEM_BUF_SIZE;
  }
}

static void writeDataToDisk(STSBuf* pTSBuf) {
  if (pTSBuf->tsData.len == 0) {
    return;
  }

  STSBlock* pBlock = &pTSBuf->block;

  pBlock->numOfElem = pTSBuf->tsData.len / TSDB_KEYSIZE;
  pBlock->compLen =
      tsCompressTimestamp(pTSBuf->tsData.rawBuf, pTSBuf->tsData.len, pTSBuf->tsData.len / TSDB_KEYSIZE, pBlock->payload,
                          pTSBuf->tsData.allocSize, TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);

  int64_t r = fseek(pTSBuf->f, pTSBuf->fileSize, SEEK_SET);
  UNUSED(r);

  /*
   * format for output data:
   * 1. tags, number of ts, size after compressed, payload, size after compressed
   * 2. tags, number of ts, size after compressed, payload, size after compressed
   *
   * both side has the compressed length is used to support load data forwards/backwords.
   */
  fwrite(&pBlock->tag, sizeof(pBlock->tag), 1, pTSBuf->f);
  fwrite(&pBlock->numOfElem, sizeof(pBlock->numOfElem), 1, pTSBuf->f);

  fwrite(&pBlock->compLen, sizeof(pBlock->compLen), 1, pTSBuf->f);

  fwrite(pBlock->payload, (size_t)pBlock->compLen, 1, pTSBuf->f);

  fwrite(&pBlock->compLen, sizeof(pBlock->compLen), 1, pTSBuf->f);

  int32_t blockSize = sizeof(pBlock->tag) + sizeof(pBlock->numOfElem) + sizeof(pBlock->compLen) * 2 + pBlock->compLen;
  pTSBuf->fileSize += blockSize;

  pTSBuf->tsData.len = 0;

  pTSBuf->pData[pTSBuf->numOfVnodes - 1].info.compLen += blockSize;
  pTSBuf->pData[pTSBuf->numOfVnodes - 1].info.numOfBlocks += 1;

  shrinkBuffer(&pTSBuf->tsData);
}

static void expandBuffer(STSList* ptsData, int32_t inputSize) {
  if (ptsData->allocSize - ptsData->len < inputSize) {
    int32_t newSize = inputSize + ptsData->len;
    char*   tmp = realloc(ptsData->rawBuf, (size_t)newSize);
    if (tmp == NULL) {
      // todo
    }

    ptsData->rawBuf = tmp;
    ptsData->allocSize = newSize;
  }
}

STSBlock* readDataFromDisk(STSBuf* pTSBuf, int32_t order, bool decomp) {
  STSBlock* pBlock = &pTSBuf->block;

  // clear the memory buffer
  void* tmp = pBlock->payload;
  memset(pBlock, 0, sizeof(STSBlock));
  pBlock->payload = tmp;

  if (order == TSQL_SO_DESC) {
    /*
     * set the right position for the reversed traverse, the reversed traverse is started from
     * the end of each comp data block
     */
    fseek(pTSBuf->f, -sizeof(pBlock->padding), SEEK_CUR);
    fread(&pBlock->padding, sizeof(pBlock->padding), 1, pTSBuf->f);

    pBlock->compLen = pBlock->padding;
    int32_t offset = pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + sizeof(pBlock->tag);
    fseek(pTSBuf->f, -offset, SEEK_CUR);
  }

  fread(&pBlock->tag, sizeof(pBlock->tag), 1, pTSBuf->f);
  fread(&pBlock->numOfElem, sizeof(pBlock->numOfElem), 1, pTSBuf->f);

  fread(&pBlock->compLen, sizeof(pBlock->compLen), 1, pTSBuf->f);
  fread(pBlock->payload, (size_t)pBlock->compLen, 1, pTSBuf->f);

  if (decomp) {
    pTSBuf->tsData.len =
        tsDecompressTimestamp(pBlock->payload, pBlock->compLen, pBlock->numOfElem, pTSBuf->tsData.rawBuf,
                              pTSBuf->tsData.allocSize, TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);
  }

  // read the comp length at the length of comp block
  fread(&pBlock->padding, sizeof(pBlock->padding), 1, pTSBuf->f);

  // for backwards traverse, set the start position at the end of previous block
  if (order == TSQL_SO_DESC) {
    int32_t offset = pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + sizeof(pBlock->tag);
    int64_t r = fseek(pTSBuf->f, -offset, SEEK_CUR);
    UNUSED(r);
  }

  return pBlock;
}

// set the order of ts buffer if the ts order has not been set yet
static int32_t setCheckTSOrder(STSBuf* pTSBuf, const char* pData, int32_t len) {
  STSList* ptsData = &pTSBuf->tsData;

  if (pTSBuf->tsOrder == -1) {
    if (ptsData->len > 0) {
      TSKEY lastKey = *(TSKEY*)(ptsData->rawBuf + ptsData->len - TSDB_KEYSIZE);

      if (lastKey > *(TSKEY*)pData) {
        pTSBuf->tsOrder = TSQL_SO_DESC;
      } else {
        pTSBuf->tsOrder = TSQL_SO_ASC;
      }
    } else if (len > TSDB_KEYSIZE) {
      // no data in current vnode, more than one ts is added, check the orders
      TSKEY k1 = *(TSKEY*)(pData);
      TSKEY k2 = *(TSKEY*)(pData + TSDB_KEYSIZE);

      if (k1 < k2) {
        pTSBuf->tsOrder = TSQL_SO_ASC;
      } else if (k1 > k2) {
        pTSBuf->tsOrder = TSQL_SO_DESC;
      } else {
        // todo handle error
      }
    }
  } else {
    // todo the timestamp order is set, check the asc/desc order of appended data
  }

  return TSDB_CODE_SUCCESS;
}

void tsBufAppend(STSBuf* pTSBuf, int32_t vnodeId, int64_t tag, const char* pData, int32_t len) {
  STSVnodeBlockInfoEx* pBlockInfo = NULL;
  STSList*             ptsData = &pTSBuf->tsData;

  if (pTSBuf->numOfVnodes == 0 || pTSBuf->pData[pTSBuf->numOfVnodes - 1].info.vnode != vnodeId) {
    writeDataToDisk(pTSBuf);
    shrinkBuffer(ptsData);

    pBlockInfo = addOneVnodeInfo(pTSBuf, vnodeId);
  } else {
    pBlockInfo = &pTSBuf->pData[pTSBuf->numOfVnodes - 1];
  }

  assert(pBlockInfo->info.vnode == vnodeId);

  if (pTSBuf->block.tag != tag && ptsData->len > 0) {
    // new arrived data with different tags value, save current value into disk first
    writeDataToDisk(pTSBuf);
  } else {
    expandBuffer(ptsData, len);
  }

  pTSBuf->block.tag = tag;
  memcpy(ptsData->rawBuf + ptsData->len, pData, (size_t)len);

  // todo check return value
  setCheckTSOrder(pTSBuf, pData, len);

  ptsData->len += len;
  pBlockInfo->len += len;

  pTSBuf->numOfTotal += len / TSDB_KEYSIZE;

  if (ptsData->len >= ptsData->threshold) {
    writeDataToDisk(pTSBuf);
    shrinkBuffer(ptsData);
  }

  tsBufResetPos(pTSBuf);
}

void tsBufFlush(STSBuf* pTSBuf) {
  if (pTSBuf->tsData.len <= 0) {
    return;
  }

  writeDataToDisk(pTSBuf);
  shrinkBuffer(&pTSBuf->tsData);

  STSVnodeBlockInfo* pBlockInfo = &pTSBuf->pData[pTSBuf->numOfVnodes - 1].info;

  // update prev vnode length info in file
  TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes - 1, pBlockInfo);

  // save the ts order into header
  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = pTSBuf->tsOrder};
  STSBufUpdateHeader(pTSBuf, &header);

  fsync(fileno(pTSBuf->f));
}

static int32_t tsBufFindVnodeIndexFromId(STSVnodeBlockInfoEx* pVnodeInfoEx, int32_t numOfVnodes, int32_t vnodeId) {
  int32_t j = -1;
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    if (pVnodeInfoEx[i].info.vnode == vnodeId) {
      j = i;
      break;
    }
  }

  return j;
}

// todo opt performance by cache blocks info
static int32_t tsBufFindBlock(STSBuf* pTSBuf, STSVnodeBlockInfo* pBlockInfo, int32_t blockIndex) {
  if (fseek(pTSBuf->f, pBlockInfo->offset, SEEK_SET) != 0) {
    return -1;
  }

  // sequentially read the compressed data blocks, start from the beginning of the comp data block of this vnode
  int32_t i = 0;
  bool    decomp = false;

  while ((i++) <= blockIndex) {
    if (readDataFromDisk(pTSBuf, TSQL_SO_ASC, decomp) == NULL) {
      return -1;
    }
  }

  // set the file position to be the end of previous comp block
  if (pTSBuf->cur.order == TSQL_SO_DESC) {
    STSBlock* pBlock = &pTSBuf->block;
    int32_t   compBlockSize =
        pBlock->compLen + sizeof(pBlock->compLen) * 2 + sizeof(pBlock->numOfElem) + sizeof(pBlock->tag);
    fseek(pTSBuf->f, -compBlockSize, SEEK_CUR);
  }

  return 0;
}

static int32_t tsBufFindBlockByTag(STSBuf* pTSBuf, STSVnodeBlockInfo* pBlockInfo, int64_t tag) {
  bool decomp = false;

  int64_t offset = 0;
  if (pTSBuf->cur.order == TSQL_SO_ASC) {
    offset = pBlockInfo->offset;
  } else {  // reversed traverse starts from the end of block
    offset = pBlockInfo->offset + pBlockInfo->compLen;
  }

  if (fseek(pTSBuf->f, offset, SEEK_SET) != 0) {
    return -1;
  }

  for (int32_t i = 0; i < pBlockInfo->numOfBlocks; ++i) {
    if (readDataFromDisk(pTSBuf, pTSBuf->cur.order, decomp) == NULL) {
      return -1;
    }

    if (pTSBuf->block.tag == tag) {
      return i;
    }
  }

  return -1;
}

static void tsBufGetBlock(STSBuf* pTSBuf, int32_t vnodeIndex, int32_t blockIndex) {
  STSVnodeBlockInfo* pBlockInfo = &pTSBuf->pData[vnodeIndex].info;
  if (pBlockInfo->numOfBlocks <= blockIndex) {
    assert(false);
  }

  STSCursor* pCur = &pTSBuf->cur;
  if (pCur->vnodeIndex == vnodeIndex && ((pCur->blockIndex <= blockIndex && pCur->order == TSQL_SO_ASC) ||
                                         (pCur->blockIndex >= blockIndex && pCur->order == TSQL_SO_DESC))) {
    int32_t i = 0;
    bool    decomp = false;
    int32_t step = abs(blockIndex - pCur->blockIndex);

    while ((++i) <= step) {
      if (readDataFromDisk(pTSBuf, pCur->order, decomp) == NULL) {
        return;
      }
    }
  } else {
    if (tsBufFindBlock(pTSBuf, pBlockInfo, blockIndex) == -1) {
      assert(false);
    }
  }

  STSBlock* pBlock = &pTSBuf->block;
  pTSBuf->tsData.len =
      tsDecompressTimestamp(pBlock->payload, pBlock->compLen, pBlock->numOfElem, pTSBuf->tsData.rawBuf,
                            pTSBuf->tsData.allocSize, TWO_STAGE_COMP, pTSBuf->assistBuf, pTSBuf->bufSize);

  assert(pTSBuf->tsData.len / TSDB_KEYSIZE == pBlock->numOfElem);

  pCur->vnodeIndex = vnodeIndex;
  pCur->blockIndex = blockIndex;

  pCur->tsIndex = (pCur->order == TSQL_SO_ASC) ? 0 : pBlock->numOfElem - 1;
}

STSVnodeBlockInfo* tsBufGetVnodeBlockInfo(STSBuf* pTSBuf, int32_t vnodeId) {
  int32_t j = tsBufFindVnodeIndexFromId(pTSBuf->pData, pTSBuf->numOfVnodes, vnodeId);
  if (j == -1) {
    return NULL;
  }

  return &pTSBuf->pData[j].info;
}

int32_t STSBufUpdateHeader(STSBuf* pTSBuf, STSBufFileHeader* pHeader) {
  if ((pTSBuf->f == NULL) || pHeader == NULL || pHeader->numOfVnode < 0 || pHeader->magic != TS_COMP_FILE_MAGIC) {
    return -1;
  }

  int64_t r = fseek(pTSBuf->f, 0, SEEK_SET);
  if (r != 0) {
    return -1;
  }

  fwrite(pHeader, sizeof(STSBufFileHeader), 1, pTSBuf->f);
  return 0;
}

bool tsBufNextPos(STSBuf* pTSBuf) {
  if (pTSBuf == NULL || pTSBuf->numOfVnodes == 0) {
    return false;
  }

  STSCursor* pCur = &pTSBuf->cur;

  // get the first/last position according to traverse order
  if (pCur->vnodeIndex == -1) {
    if (pCur->order == TSQL_SO_ASC) {
      tsBufGetBlock(pTSBuf, 0, 0);
      
      if (pTSBuf->block.numOfElem == 0) { // the whole list is empty, return
        tsBufResetPos(pTSBuf);
        return false;
      } else {
        return true;
      }
      
    } else { // get the last timestamp record in the last block of the last vnode
      assert(pTSBuf->numOfVnodes > 0);
      
      int32_t vnodeIndex = pTSBuf->numOfVnodes - 1;
      pCur->vnodeIndex = vnodeIndex;
      
      int32_t            vnodeId = pTSBuf->pData[pCur->vnodeIndex].info.vnode;
      STSVnodeBlockInfo* pBlockInfo = tsBufGetVnodeBlockInfo(pTSBuf, vnodeId);
      int32_t            blockIndex = pBlockInfo->numOfBlocks - 1;

      tsBufGetBlock(pTSBuf, vnodeIndex, blockIndex);

      pCur->tsIndex = pTSBuf->block.numOfElem - 1;
      if (pTSBuf->block.numOfElem == 0) {
        tsBufResetPos(pTSBuf);
        return false;
      } else {
        return true;
      }
    }
  }

  int32_t step = pCur->order == TSQL_SO_ASC ? 1 : -1;

  while (1) {
    assert(pTSBuf->tsData.len == pTSBuf->block.numOfElem * TSDB_KEYSIZE);

    if ((pCur->order == TSQL_SO_ASC && pCur->tsIndex >= pTSBuf->block.numOfElem - 1) ||
        (pCur->order == TSQL_SO_DESC && pCur->tsIndex <= 0)) {
      int32_t vnodeId = pTSBuf->pData[pCur->vnodeIndex].info.vnode;

      STSVnodeBlockInfo* pBlockInfo = tsBufGetVnodeBlockInfo(pTSBuf, vnodeId);
      if (pBlockInfo == NULL || (pCur->blockIndex >= pBlockInfo->numOfBlocks - 1 && pCur->order == TSQL_SO_ASC) ||
          (pCur->blockIndex <= 0 && pCur->order == TSQL_SO_DESC)) {
        if ((pCur->vnodeIndex >= pTSBuf->numOfVnodes - 1 && pCur->order == TSQL_SO_ASC) ||
            (pCur->vnodeIndex <= 0 && pCur->order == TSQL_SO_DESC)) {
          pCur->vnodeIndex = -1;
          return false;
        }

        int32_t blockIndex = pCur->order == TSQL_SO_ASC ? 0 : pBlockInfo->numOfBlocks - 1;
        tsBufGetBlock(pTSBuf, pCur->vnodeIndex + step, blockIndex);
        break;

      } else {
        tsBufGetBlock(pTSBuf, pCur->vnodeIndex, pCur->blockIndex + step);
        break;
      }
    } else {
      pCur->tsIndex += step;
      break;
    }
  }

  return true;
}

void tsBufResetPos(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return;
  }

  pTSBuf->cur = (STSCursor){.tsIndex = -1, .blockIndex = -1, .vnodeIndex = -1, .order = pTSBuf->cur.order};
}

STSElem tsBufGetElem(STSBuf* pTSBuf) {
  STSElem    elem1 = {.vnode = -1};
  STSCursor* pCur = &pTSBuf->cur;

  if (pTSBuf == NULL || pCur->vnodeIndex < 0) {
    return elem1;
  }

  STSBlock* pBlock = &pTSBuf->block;

  elem1.vnode = pTSBuf->pData[pCur->vnodeIndex].info.vnode;
  elem1.ts = *(TSKEY*)(pTSBuf->tsData.rawBuf + pCur->tsIndex * TSDB_KEYSIZE);
  elem1.tag = pBlock->tag;

  return elem1;
}

/**
 * current only support ts comp data from two vnode merge
 * @param pDestBuf
 * @param pSrcBuf
 * @param vnodeId
 * @return
 */
int32_t tsBufMerge(STSBuf* pDestBuf, const STSBuf* pSrcBuf, int32_t vnodeId) {
  if (pDestBuf == NULL || pSrcBuf == NULL || pSrcBuf->numOfVnodes <= 0) {
    return 0;
  }

  if (pDestBuf->numOfVnodes + pSrcBuf->numOfVnodes > TS_COMP_FILE_VNODE_MAX) {
    return -1;
  }

  // src can only have one vnode index
  if (pSrcBuf->numOfVnodes > 1) {
    return -1;
  }

  // there are data in buffer, flush to disk first
  tsBufFlush(pDestBuf);

  // compared with the last vnode id
  if (vnodeId != pDestBuf->pData[pDestBuf->numOfVnodes - 1].info.vnode) {
    int32_t oldSize = pDestBuf->numOfVnodes;
    int32_t newSize = oldSize + pSrcBuf->numOfVnodes;

    if (pDestBuf->numOfAlloc < newSize) {
      pDestBuf->numOfAlloc = newSize;

      STSVnodeBlockInfoEx* tmp = realloc(pDestBuf->pData, sizeof(STSVnodeBlockInfoEx) * newSize);
      if (tmp == NULL) {
        return -1;
      }

      pDestBuf->pData = tmp;
    }

    // directly copy the vnode index information
    memcpy(&pDestBuf->pData[oldSize], pSrcBuf->pData, (size_t)pSrcBuf->numOfVnodes * sizeof(STSVnodeBlockInfoEx));

    // set the new offset value
    for (int32_t i = 0; i < pSrcBuf->numOfVnodes; ++i) {
      STSVnodeBlockInfoEx* pBlockInfoEx = &pDestBuf->pData[i + oldSize];
      pBlockInfoEx->info.offset = (pSrcBuf->pData[i].info.offset - getDataStartOffset()) + pDestBuf->fileSize;
      pBlockInfoEx->info.vnode = vnodeId;
    }

    pDestBuf->numOfVnodes = newSize;
  } else {
    STSVnodeBlockInfoEx* pBlockInfoEx = &pDestBuf->pData[pDestBuf->numOfVnodes - 1];
    pBlockInfoEx->len += pSrcBuf->pData[0].len;
    pBlockInfoEx->info.numOfBlocks += pSrcBuf->pData[0].info.numOfBlocks;
    pBlockInfoEx->info.compLen += pSrcBuf->pData[0].info.compLen;
    pBlockInfoEx->info.vnode = vnodeId;
  }

  int64_t r = fseek(pDestBuf->f, 0, SEEK_END);
  assert(r == 0);

  int64_t offset = getDataStartOffset();
  int32_t size = pSrcBuf->fileSize - offset;

#ifdef LINUX
  ssize_t rc = sendfile(fileno(pDestBuf->f), fileno(pSrcBuf->f), &offset, size);
#else
  ssize_t rc = fsendfile(pDestBuf->f, pSrcBuf->f, &offset, size);
#endif
  if (rc == -1) {
    printf("%s\n", strerror(errno));
    return -1;
  }

  if (rc != size) {
    printf("%s\n", strerror(errno));
    return -1;
  }

  pDestBuf->numOfTotal += pSrcBuf->numOfTotal;

  return 0;
}

STSBuf* tsBufCreateFromCompBlocks(const char* pData, int32_t numOfBlocks, int32_t len, int32_t order) {
  STSBuf* pTSBuf = tsBufCreate(true);

  STSVnodeBlockInfo* pBlockInfo = &(addOneVnodeInfo(pTSBuf, 0)->info);
  pBlockInfo->numOfBlocks = numOfBlocks;
  pBlockInfo->compLen = len;
  pBlockInfo->offset = getDataStartOffset();
  pBlockInfo->vnode = 0;

  // update prev vnode length info in file
  TSBufUpdateVnodeInfo(pTSBuf, pTSBuf->numOfVnodes - 1, pBlockInfo);

  fseek(pTSBuf->f, pBlockInfo->offset, SEEK_SET);
  fwrite((void*) pData, 1, len, pTSBuf->f);
  pTSBuf->fileSize += len;

  pTSBuf->tsOrder = order;
  assert(order == TSQL_SO_ASC || order == TSQL_SO_DESC);

  STSBufFileHeader header = {
      .magic = TS_COMP_FILE_MAGIC, .numOfVnode = pTSBuf->numOfVnodes, .tsOrder = pTSBuf->tsOrder};
  STSBufUpdateHeader(pTSBuf, &header);

  fsync(fileno(pTSBuf->f));

  return pTSBuf;
}

STSElem tsBufGetElemStartPos(STSBuf* pTSBuf, int32_t vnodeId, int64_t tag) {
  STSElem elem = {.vnode = -1};

  if (pTSBuf == NULL) {
    return elem;
  }

  int32_t j = tsBufFindVnodeIndexFromId(pTSBuf->pData, pTSBuf->numOfVnodes, vnodeId);
  if (j == -1) {
    return elem;
  }

  // for debug purpose
  //  tsBufDisplay(pTSBuf);

  STSCursor*         pCur = &pTSBuf->cur;
  STSVnodeBlockInfo* pBlockInfo = &pTSBuf->pData[j].info;

  int32_t blockIndex = tsBufFindBlockByTag(pTSBuf, pBlockInfo, tag);
  if (blockIndex < 0) {
    return elem;
  }

  pCur->vnodeIndex = j;
  pCur->blockIndex = blockIndex;
  tsBufGetBlock(pTSBuf, j, blockIndex);

  return tsBufGetElem(pTSBuf);
}

STSCursor tsBufGetCursor(STSBuf* pTSBuf) {
  STSCursor c = {.vnodeIndex = -1};
  if (pTSBuf == NULL) {
    return c;
  }

  return pTSBuf->cur;
}

void tsBufSetCursor(STSBuf* pTSBuf, STSCursor* pCur) {
  if (pTSBuf == NULL || pCur == NULL) {
    return;
  }

  //  assert(pCur->vnodeIndex != -1 && pCur->tsIndex >= 0 && pCur->blockIndex >= 0);
  if (pCur->vnodeIndex != -1) {
    tsBufGetBlock(pTSBuf, pCur->vnodeIndex, pCur->blockIndex);
  }

  pTSBuf->cur = *pCur;
}

void tsBufSetTraverseOrder(STSBuf* pTSBuf, int32_t order) {
  if (pTSBuf == NULL) {
    return;
  }

  pTSBuf->cur.order = order;
}

STSBuf* tsBufClone(STSBuf* pTSBuf) {
  if (pTSBuf == NULL) {
    return NULL;
  }

  return tsBufCreateFromFile(pTSBuf->path, false);
}

void tsBufDisplay(STSBuf* pTSBuf) {
  printf("-------start of ts comp file-------\n");
  printf("number of vnode:%d\n", pTSBuf->numOfVnodes);

  int32_t old = pTSBuf->cur.order;
  pTSBuf->cur.order = TSQL_SO_ASC;

  tsBufResetPos(pTSBuf);

  while (tsBufNextPos(pTSBuf)) {
    STSElem elem = tsBufGetElem(pTSBuf);
    printf("%d-%lld-%lld\n", elem.vnode, elem.tag, elem.ts);
  }

  pTSBuf->cur.order = old;
  printf("-------end of ts comp file-------\n");
}
