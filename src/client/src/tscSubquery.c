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

#include "tscSubquery.h"
#include "qast.h"
#include "tcompare.h"
#include "tschemautil.h"
#include "qtsbuf.h"
#include "tscLog.h"
#include "tsclient.h"

typedef struct SInsertSupporter {
  SSubqueryState* pState;
  SSqlObj*  pSql;
  int32_t   index;
} SInsertSupporter;

static void freeJoinSubqueryObj(SSqlObj* pSql);
static bool tscHasRemainDataInSubqueryResultSet(SSqlObj *pSql);

static bool tsCompare(int32_t order, int64_t left, int64_t right) {
  if (order == TSDB_ORDER_ASC) {
    return left < right;
  } else {
    return left > right;
  }
}

static int64_t doTSBlockIntersect(SSqlObj* pSql, SJoinSupporter* pSupporter1, SJoinSupporter* pSupporter2, STimeWindow * win) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);

  STSBuf* output1 = tsBufCreate(true, pQueryInfo->order.order);
  STSBuf* output2 = tsBufCreate(true, pQueryInfo->order.order);

  win->skey = INT64_MAX;
  win->ekey = INT64_MIN;

  SLimitVal* pLimit = &pQueryInfo->limit;
  int32_t    order = pQueryInfo->order.order;
  
  SQueryInfo* pSubQueryInfo1 = tscGetQueryInfoDetail(&pSql->pSubs[0]->cmd, 0);
  SQueryInfo* pSubQueryInfo2 = tscGetQueryInfoDetail(&pSql->pSubs[1]->cmd, 0);
  
  pSubQueryInfo1->tsBuf = output1;
  pSubQueryInfo2->tsBuf = output2;

  tsBufResetPos(pSupporter1->pTSBuf);
  tsBufResetPos(pSupporter2->pTSBuf);

  // TODO add more details information
  if (!tsBufNextPos(pSupporter1->pTSBuf)) {
    tsBufFlush(output1);
    tsBufFlush(output2);

    tscDebug("%p input1 is empty, 0 for secondary query after ts blocks intersecting", pSql);
    return 0;
  }

  if (!tsBufNextPos(pSupporter2->pTSBuf)) {
    tsBufFlush(output1);
    tsBufFlush(output2);

    tscDebug("%p input2 is empty, 0 for secondary query after ts blocks intersecting", pSql);
    return 0;
  }

  int64_t numOfInput1 = 1;
  int64_t numOfInput2 = 1;

  while (1) {
    STSElem elem1 = tsBufGetElem(pSupporter1->pTSBuf);
    STSElem elem2 = tsBufGetElem(pSupporter2->pTSBuf);

#ifdef _DEBUG_VIEW
    tscInfo("%" PRId64 ", tags:%d \t %" PRId64 ", tags:%d", elem1.ts, elem1.tag, elem2.ts, elem2.tag);
#endif

    if (elem1.tag < elem2.tag || (elem1.tag == elem2.tag && tsCompare(order, elem1.ts, elem2.ts))) {
      if (!tsBufNextPos(pSupporter1->pTSBuf)) {
        break;
      }

      numOfInput1++;
    } else if (elem1.tag > elem2.tag || (elem1.tag == elem2.tag && tsCompare(order, elem2.ts, elem1.ts))) {
      if (!tsBufNextPos(pSupporter2->pTSBuf)) {
        break;
      }

      numOfInput2++;
    } else {
      /*
       * in case of stable query, limit/offset is not applied here. the limit/offset is applied to the
       * final results which is acquired after the secondry merge of in the client.
       */
      if (pLimit->offset == 0 || pQueryInfo->intervalTime > 0 || QUERY_IS_STABLE_QUERY(pQueryInfo->type)) {
        if (win->skey > elem1.ts) {
          win->skey = elem1.ts;
        }
  
        if (win->ekey < elem1.ts) {
          win->ekey = elem1.ts;
        }
        
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
    output1->tsOrder = TSDB_ORDER_ASC;
    output2->tsOrder = TSDB_ORDER_ASC;
  }

  tsBufFlush(output1);
  tsBufFlush(output2);

  tsBufDestory(pSupporter1->pTSBuf);
  tsBufDestory(pSupporter2->pTSBuf);

  tscDebug("%p input1:%" PRId64 ", input2:%" PRId64 ", final:%" PRId64 " for secondary query after ts blocks "
           "intersecting, skey:%" PRId64 ", ekey:%" PRId64, pSql, numOfInput1, numOfInput2, output1->numOfTotal,
           win->skey, win->ekey);

  return output1->numOfTotal;
}

// todo handle failed to create sub query
SJoinSupporter* tscCreateJoinSupporter(SSqlObj* pSql, SSubqueryState* pState, int32_t index) {
  SJoinSupporter* pSupporter = calloc(1, sizeof(SJoinSupporter));
  if (pSupporter == NULL) {
    return NULL;
  }

  pSupporter->pObj = pSql;
  pSupporter->pState = pState;

  pSupporter->subqueryIndex = index;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);
  
  pSupporter->intervalTime = pQueryInfo->intervalTime;
  pSupporter->slidingTime = pQueryInfo->slidingTime;
  pSupporter->limit = pQueryInfo->limit;

  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, pSql->cmd.clauseIndex, index);
  pSupporter->uid = pTableMetaInfo->pTableMeta->uid;
  assert (pSupporter->uid != 0);

  getTmpfilePath("join-", pSupporter->path);
  pSupporter->f = fopen(pSupporter->path, "w");

  // todo handle error
  if (pSupporter->f == NULL) {
    tscError("%p failed to create tmp file:%s, reason:%s", pSql, pSupporter->path, strerror(errno));
  }

  return pSupporter;
}

static void tscDestroyJoinSupporter(SJoinSupporter* pSupporter) {
  if (pSupporter == NULL) {
    return;
  }

  if (pSupporter->exprList != NULL) {
    tscSqlExprInfoDestroy(pSupporter->exprList);
  }
  
  if (pSupporter->colList != NULL) {
    tscColumnListDestroy(pSupporter->colList);
  }

  tscFieldInfoClear(&pSupporter->fieldsInfo);

  if (pSupporter->f != NULL) {
    fclose(pSupporter->f);
    unlink(pSupporter->path);
    pSupporter->f = NULL;
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
static UNUSED_FUNC bool needSecondaryQuery(SQueryInfo* pQueryInfo) {
  size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
  
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumn* base = taosArrayGet(pQueryInfo->colList, i);
    if (base->colIndex.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return true;
    }
  }

  return false;
}

/*
 * launch secondary stage query to fetch the result that contains timestamp in set
 */
static int32_t tscLaunchRealSubqueries(SSqlObj* pSql) {
  int32_t         numOfSub = 0;
  SJoinSupporter* pSupporter = NULL;
  
  //If the columns are not involved in the final select clause, the corresponding query will not be issued.
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    pSupporter = pSql->pSubs[i]->param;
    if (taosArrayGetSize(pSupporter->exprList) > 0) {
      ++numOfSub;
    }
  }
  
  assert(numOfSub > 0);
  
  // scan all subquery, if one sub query has only ts, ignore it
  tscDebug("%p start to launch secondary subqueries, total:%d, only:%d needs to query", pSql, pSql->numOfSubs, numOfSub);

  //the subqueries that do not actually launch the secondary query to virtual node is set as completed.
  SSubqueryState* pState = pSupporter->pState;
  pState->numOfTotal = pSql->numOfSubs;
  pState->numOfRemain = numOfSub;
  
  bool success = true;
  
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj *pPrevSub = pSql->pSubs[i];
    pSql->pSubs[i] = NULL;
    
    pSupporter = pPrevSub->param;
  
    if (taosArrayGetSize(pSupporter->exprList) == 0) {
      tscDebug("%p subIndex: %d, no need to launch query, ignore it", pSql, i);
    
      tscDestroyJoinSupporter(pSupporter);
      tscFreeSqlObj(pPrevSub);
    
      pSql->pSubs[i] = NULL;
      continue;
    }
  
    SQueryInfo *pSubQueryInfo = tscGetQueryInfoDetail(&pPrevSub->cmd, 0);
    STSBuf *pTSBuf = pSubQueryInfo->tsBuf;
    pSubQueryInfo->tsBuf = NULL;
  
    // free result for async object will also free sqlObj
    assert(tscSqlExprNumOfExprs(pSubQueryInfo) == 1); // ts_comp query only requires one resutl columns
    taos_free_result(pPrevSub);
  
    SSqlObj *pNew = createSubqueryObj(pSql, (int16_t) i, tscJoinQueryCallback, pSupporter, TSDB_SQL_SELECT, NULL);
    if (pNew == NULL) {
      tscDestroyJoinSupporter(pSupporter);
      success = false;
      break;
    }
  
    tscClearSubqueryInfo(&pNew->cmd);
    pSql->pSubs[i] = pNew;
  
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pQueryInfo->tsBuf = pTSBuf;  // transfer the ownership of timestamp comp-z data to the new created object
  
    // set the second stage sub query for join process
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE);

    pQueryInfo->intervalTime = pSupporter->intervalTime;
    pQueryInfo->slidingTime = pSupporter->slidingTime;
    pQueryInfo->groupbyExpr = pSupporter->groupbyExpr;
    
    tscTagCondCopy(&pQueryInfo->tagCond, &pSupporter->tagCond);
  
    pQueryInfo->colList = pSupporter->colList;
    pQueryInfo->exprList = pSupporter->exprList;
    pQueryInfo->fieldsInfo = pSupporter->fieldsInfo;
    
    pSupporter->exprList = NULL;
    pSupporter->colList = NULL;
    memset(&pSupporter->fieldsInfo, 0, sizeof(SFieldInfo));
  
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    assert(pNew->numOfSubs == 0 && pNew->cmd.numOfClause == 1 && pNewQueryInfo->numOfTables == 1);
  
    tscFieldInfoUpdateOffset(pNewQueryInfo);
  
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pNewQueryInfo, 0);
  
    /*
     * When handling the projection query, the offset value will be modified for table-table join, which is changed
     * during the timestamp intersection.
     */
    pSupporter->limit = pQueryInfo->limit;
    pNewQueryInfo->limit = pSupporter->limit;

    SColumnIndex index = {.tableIndex = 0, .columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX};
    SSchema* s = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, 0);

    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, 0);
    int16_t funcId = pExpr->functionId;

    if ((pExpr->colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX) ||
        (funcId != TSDB_FUNC_TS && funcId != TSDB_FUNC_TS_DUMMY && funcId != TSDB_FUNC_PRJ)) {

      int16_t functionId = tscIsProjectionQuery(pQueryInfo)? TSDB_FUNC_PRJ : TSDB_FUNC_TS;

      tscAddSpecialColumnForSelect(pQueryInfo, 0, functionId, &index, s, TSDB_COL_NORMAL);
      tscPrintSelectClause(pNew, 0);
      tscFieldInfoUpdateOffset(pNewQueryInfo);

      pExpr = tscSqlExprGet(pQueryInfo, 0);
    }

    // set the join condition tag column info, to do extract method
    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      assert(pQueryInfo->tagCond.joinInfo.hasJoin);
      int16_t colId = tscGetJoinTagColIdByUid(&pQueryInfo->tagCond, pTableMetaInfo->pTableMeta->uid);

      pExpr->param[0].i64Key = colId;
      pExpr->numOfParams = 1;
    }

    size_t numOfCols = taosArrayGetSize(pNewQueryInfo->colList);
    tscDebug("%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, exprInfo:%zu, colList:%zu, fieldsInfo:%d, name:%s",
             pSql, pNew, 0, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, taosArrayGetSize(pNewQueryInfo->exprList),
             numOfCols, pNewQueryInfo->fieldsInfo.numOfOutput, pTableMetaInfo->name);
  }
  
  //prepare the subqueries object failed, abort
  if (!success) {
    pSql->res.code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscError("%p failed to prepare subqueries objs for secondary phase query, numOfSub:%d, code:%d", pSql,
        pSql->numOfSubs, pSql->res.code);
    freeJoinSubqueryObj(pSql);
    
    return pSql->res.code;
  }
  
  for(int32_t i = 0; i < pSql->numOfSubs; ++i) {
    if (pSql->pSubs[i] == NULL) {
      continue;
    }

    tscDoQuery(pSql->pSubs[i]);
  }

  return TSDB_CODE_SUCCESS;
}

void freeJoinSubqueryObj(SSqlObj* pSql) {
  SSubqueryState* pState = NULL;

  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    if (pSub == NULL) {
      continue;
    }
    
    SJoinSupporter* p = pSub->param;
    pState = p->pState;

    tscDestroyJoinSupporter(p);

    if (pSub->res.code == TSDB_CODE_SUCCESS) {
      taos_free_result(pSub);
    }
  }

  tfree(pState);
  pSql->numOfSubs = 0;
}

static void quitAllSubquery(SSqlObj* pSqlObj, SJoinSupporter* pSupporter) {
  assert(pSupporter->pState->numOfRemain > 0);

  if (atomic_sub_fetch_32(&pSupporter->pState->numOfRemain, 1) <= 0) {
    tscError("%p all subquery return and query failed, global code:%d", pSqlObj, pSqlObj->res.code);
    freeJoinSubqueryObj(pSqlObj);
  }
}

// update the query time range according to the join results on timestamp
static void updateQueryTimeRange(SQueryInfo* pQueryInfo, STimeWindow* win) {
  assert(pQueryInfo->window.skey <= win->skey && pQueryInfo->window.ekey >= win->ekey);
  pQueryInfo->window = *win;
}

static UNUSED_FUNC void tSIntersectionAndLaunchSecQuery(SJoinSupporter* pSupporter, SSqlObj* pSql) {
  SSqlObj* pParentSql = pSupporter->pObj;
  SQueryInfo* pParentQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, pParentSql->cmd.clauseIndex);
  
//  if (tscNonOrderedProjectionQueryOnSTable(pParentQueryInfo, 0)) {
//    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
//    assert(pQueryInfo->numOfTables == 1);
//
//    // for projection query, need to try next vnode
////        int32_t totalVnode = pTableMetaInfo->pMetricMeta->numOfVnodes;
//    int32_t totalVnode = 0;
//    if ((++pTableMetaInfo->vgroupIndex) < totalVnode) {
//      tscDebug("%p current vnode:%d exhausted, try next:%d. total vnode:%d. current numOfRes:%d", pSql,
//               pTableMetaInfo->vgroupIndex - 1, pTableMetaInfo->vgroupIndex, totalVnode, pRes->numOfTotal);
//
//      pSql->cmd.command = TSDB_SQL_SELECT;
//      pSql->fp = tscJoinQueryCallback;
//      tscProcessSql(pSql);
//
//      return;
//    }
//  }

    SJoinSupporter* p1 = pParentSql->pSubs[0]->param;
    SJoinSupporter* p2 = pParentSql->pSubs[1]->param;
    
    STimeWindow win = TSWINDOW_INITIALIZER;
    int64_t num = doTSBlockIntersect(pParentSql, p1, p2, &win);
    if (num <= 0) {  // no result during ts intersect
      tscDebug("%p free all sub SqlObj and quit", pParentSql);
      freeJoinSubqueryObj(pParentSql);
    } else {
      updateQueryTimeRange(pParentQueryInfo, &win);
      tscLaunchRealSubqueries(pParentSql);
    }
}

int32_t tscCompareTidTags(const void* p1, const void* p2) {
  const STidTags* t1 = (const STidTags*) varDataVal(p1);
  const STidTags* t2 = (const STidTags*) varDataVal(p2);
  
  if (t1->vgId != t2->vgId) {
    return (t1->vgId > t2->vgId) ? 1 : -1;
  }
  if (t1->tid != t2->tid) {
    return (t1->tid > t2->tid) ? 1 : -1;
  }
  return 0;
}

void tscBuildVgroupTableInfo(SSqlObj* pSql, STableMetaInfo* pTableMetaInfo, SArray* tables) {
  SArray*   result = taosArrayInit(4, sizeof(SVgroupTableInfo));
  SArray*   vgTables = NULL;
  STidTags* prev = NULL;

  size_t numOfTables = taosArrayGetSize(tables);
  for (size_t i = 0; i < numOfTables; i++) {
    STidTags* tt = taosArrayGet(tables, i);

    if (prev == NULL || tt->vgId != prev->vgId) {
      SVgroupsInfo* pvg = pTableMetaInfo->vgroupList;

      SVgroupTableInfo info = {{0}};
      for (int32_t m = 0; m < pvg->numOfVgroups; ++m) {
        if (tt->vgId == pvg->vgroups[m].vgId) {
          info.vgInfo = pvg->vgroups[m];
          break;
        }
      }
      assert(info.vgInfo.numOfIps != 0);

      vgTables = taosArrayInit(4, sizeof(STableIdInfo));
      info.itemList = vgTables;
      taosArrayPush(result, &info);
    }

    tscDebug("%p tid:%d, uid:%"PRIu64",vgId:%d added for vnode query", pSql, tt->tid, tt->uid, tt->vgId)
    STableIdInfo item = {.uid = tt->uid, .tid = tt->tid, .key = INT64_MIN};
    taosArrayPush(vgTables, &item);
    prev = tt;
  }

  pTableMetaInfo->pVgroupTables = result;
  pTableMetaInfo->vgroupIndex = 0;
}

static void issueTSCompQuery(SSqlObj* pSql, SJoinSupporter* pSupporter, SSqlObj* pParent) {
  SSqlCmd* pCmd = &pSql->cmd;
  tscClearSubqueryInfo(pCmd);
  tscFreeSqlResult(pSql);
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  tscInitQueryInfo(pQueryInfo);

  TSDB_QUERY_CLEAR_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY);
  TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_MULTITABLE_QUERY);
  
  pCmd->command = TSDB_SQL_SELECT;
  pSql->fp = tscJoinQueryCallback;
  
  SSchema colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = 1};
  
  SColumnIndex index = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscAddSpecialColumnForSelect(pQueryInfo, 0, TSDB_FUNC_TS_COMP, &index, &colSchema, TSDB_COL_NORMAL);
  
  // set the tags value for ts_comp function
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, 0);
    int16_t tagColId = tscGetJoinTagColIdByUid(&pSupporter->tagCond, pTableMetaInfo->pTableMeta->uid);
    pExpr->param->i64Key = tagColId;
    pExpr->numOfParams = 1;
  }

  // add the filter tag column
  if (pSupporter->colList != NULL) {
    size_t s = taosArrayGetSize(pSupporter->colList);
    
    for (int32_t i = 0; i < s; ++i) {
      SColumn *pCol = taosArrayGetP(pSupporter->colList, i);
      
      if (pCol->numOfFilters > 0) {  // copy to the pNew->cmd.colList if it is filtered.
        SColumn *p = tscColumnClone(pCol);
        taosArrayPush(pQueryInfo->colList, &p);
      }
    }
  }
  
  size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
  
  tscDebug(
      "%p subquery:%p tableIndex:%d, vgroupIndex:%d, numOfVgroups:%d, type:%d, ts_comp query to retrieve timestamps, "
      "numOfExpr:%zu, colList:%zu, numOfOutputFields:%d, name:%s",
      pParent, pSql, 0, pTableMetaInfo->vgroupIndex, pTableMetaInfo->vgroupList->numOfVgroups, pQueryInfo->type,
      tscSqlExprNumOfExprs(pQueryInfo), numOfCols, pQueryInfo->fieldsInfo.numOfOutput, pTableMetaInfo->name);
  
  tscProcessSql(pSql);
}

static bool checkForDuplicateTagVal(SQueryInfo* pQueryInfo, SJoinSupporter* p1, SSqlObj* pPSqlObj) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  SSchema* pSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);// todo: tags mismatch, tags not completed
  SColumn *pCol = taosArrayGetP(pTableMetaInfo->tagColList, 0);
  SSchema *pColSchema = &pSchema[pCol->colIndex.columnIndex];

  for(int32_t i = 1; i < p1->num; ++i) {
    STidTags* prev = (STidTags*) varDataVal(p1->pIdTagList + (i - 1) * p1->tagSize);
    STidTags* p = (STidTags*) varDataVal(p1->pIdTagList + i * p1->tagSize);

    if (doCompare(prev->tag, p->tag, pColSchema->type, pColSchema->bytes) == 0) {
      tscError("%p join tags have same value for different table, free all sub SqlObj and quit", pPSqlObj);
      pPSqlObj->res.code = TSDB_CODE_QRY_DUP_JOIN_KEY;
      return false;
    }
  }

  return true;
}

static void getIntersectionOfTableTuple(SQueryInfo* pQueryInfo, SSqlObj* pParentSql, SArray** s1, SArray** s2) {
  tscDebug("%p all subqueries retrieve <tid, tags> complete, do tags match", pParentSql);

  SJoinSupporter* p1 = pParentSql->pSubs[0]->param;
  SJoinSupporter* p2 = pParentSql->pSubs[1]->param;

  qsort(p1->pIdTagList, p1->num, p1->tagSize, tscCompareTidTags);
  qsort(p2->pIdTagList, p2->num, p2->tagSize, tscCompareTidTags);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  int16_t tagColId = tscGetJoinTagColIdByUid(&pQueryInfo->tagCond, pTableMetaInfo->pTableMeta->uid);

  SSchema* pColSchema = tscGetTableColumnSchemaById(pTableMetaInfo->pTableMeta, tagColId);

  *s1 = taosArrayInit(p1->num, p1->tagSize);
  *s2 = taosArrayInit(p2->num, p2->tagSize);

  if (!(checkForDuplicateTagVal(pQueryInfo, p1, pParentSql) && checkForDuplicateTagVal(pQueryInfo, p2, pParentSql))) {
    freeJoinSubqueryObj(pParentSql);
    pParentSql->res.code = TSDB_CODE_QRY_DUP_JOIN_KEY;
    tscQueueAsyncRes(pParentSql);
    return;
  }

  int32_t i = 0, j = 0;
  while(i < p1->num && j < p2->num) {
    STidTags* pp1 = (STidTags*) varDataVal(p1->pIdTagList + i * p1->tagSize);
    STidTags* pp2 = (STidTags*) varDataVal(p2->pIdTagList + j * p2->tagSize);

    int32_t ret = doCompare(pp1->tag, pp2->tag, pColSchema->type, pColSchema->bytes);
    if (ret == 0) {
      tscDebug("%p tag matched, vgId:%d, val:%d, tid:%d, uid:%"PRIu64", tid:%d, uid:%"PRIu64, pParentSql, pp1->vgId,
               *(int*) pp1->tag, pp1->tid, pp1->uid, pp2->tid, pp2->uid);

      taosArrayPush(*s1, pp1);
      taosArrayPush(*s2, pp2);
      j++;
      i++;
    } else if (ret > 0) {
      j++;
    } else {
      i++;
    }
  }
}

static void tidTagRetrieveCallback(void* param, TAOS_RES* tres, int32_t numOfRows) {
  SJoinSupporter* pSupporter = (SJoinSupporter*)param;

  SSqlObj* pParentSql = pSupporter->pObj;

  SSqlObj* pSql = (SSqlObj*)tres;
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  assert(TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY));

  // check for the error code firstly
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    // todo retry if other subqueries are not failed

    assert(numOfRows < 0 && numOfRows == taos_errno(pSql));
    tscError("%p sub query failed, code:%s, index:%d", pSql, tstrerror(numOfRows), pSupporter->subqueryIndex);

    pParentSql->res.code = numOfRows;
    quitAllSubquery(pParentSql, pSupporter);

    tscQueueAsyncRes(pParentSql);
    return;
  }

  // keep the results in memory
  if (numOfRows > 0) {
    size_t validLen = pSupporter->tagSize * pRes->numOfRows;
    size_t length = pSupporter->totalLen + validLen;

    // todo handle memory error
    char* tmp = realloc(pSupporter->pIdTagList, length);
    if (tmp == NULL) {
      tscError("%p failed to malloc memory", pSql);

      pParentSql->res.code = TAOS_SYSTEM_ERROR(errno);
      quitAllSubquery(pParentSql, pSupporter);

      tscQueueAsyncRes(pParentSql);
      return;
    }

    pSupporter->pIdTagList = tmp;

    memcpy(pSupporter->pIdTagList + pSupporter->totalLen, pRes->data, validLen);
    pSupporter->totalLen += validLen;
    pSupporter->num += pRes->numOfRows;

    // query not completed, continue to retrieve tid + tag tuples
    if (!pRes->completed) {
      taos_fetch_rows_a(tres, tidTagRetrieveCallback, param);
      return;
    }
  }

  // data in current vnode has all returned to client, try next vnode if exits
  // <tid + tag> tuples have been retrieved to client, try <tid + tag> tuples from the next vnode
  if (hasMoreVnodesToTry(pSql)) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

    int32_t totalVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
    pTableMetaInfo->vgroupIndex += 1;
    assert(pTableMetaInfo->vgroupIndex < totalVgroups);

    tscDebug("%p tid_tag from vgroup index:%d completed, try next vgroup:%d. total vgroups:%d. current numOfRes:%d",
             pSql, pTableMetaInfo->vgroupIndex - 1, pTableMetaInfo->vgroupIndex, totalVgroups, pSupporter->num);

    pCmd->command = TSDB_SQL_SELECT;
    tscResetForNextRetrieve(&pSql->res);

    // set the callback function
    pSql->fp = tscJoinQueryCallback;
    tscProcessSql(pSql);
    return;
  }

  // no data exists in next vnode, mark the <tid, tags> query completed
  // only when there is no subquery exits any more, proceeds to get the intersect of the <tid, tags> tuple sets.
  if (atomic_sub_fetch_32(&pSupporter->pState->numOfRemain, 1) > 0) {
    return;
  }

  SArray *s1 = NULL, *s2 = NULL;
  getIntersectionOfTableTuple(pQueryInfo, pParentSql, &s1, &s2);
  if (taosArrayGetSize(s1) == 0 || taosArrayGetSize(s2) == 0) {  // no results,return.
    tscDebug("%p free all sub SqlObj and quit", pParentSql);
    freeJoinSubqueryObj(pParentSql);

  } else {
    // proceed to for ts_comp query
    SSqlCmd* pSubCmd1 = &pParentSql->pSubs[0]->cmd;
    SSqlCmd* pSubCmd2 = &pParentSql->pSubs[1]->cmd;

    SQueryInfo*     pQueryInfo1 = tscGetQueryInfoDetail(pSubCmd1, 0);
    STableMetaInfo* pTableMetaInfo1 = tscGetMetaInfo(pQueryInfo1, 0);
    tscBuildVgroupTableInfo(pParentSql, pTableMetaInfo1, s1);

    SQueryInfo*     pQueryInfo2 = tscGetQueryInfoDetail(pSubCmd2, 0);
    STableMetaInfo* pTableMetaInfo2 = tscGetMetaInfo(pQueryInfo2, 0);
    tscBuildVgroupTableInfo(pParentSql, pTableMetaInfo2, s2);

    pSupporter->pState->numOfTotal = 2;
    pSupporter->pState->numOfRemain = pSupporter->pState->numOfTotal;

    for (int32_t m = 0; m < pParentSql->numOfSubs; ++m) {
      SSqlObj* sub = pParentSql->pSubs[m];
      issueTSCompQuery(sub, sub->param, pParentSql);
    }
  }

  taosArrayDestroy(s1);
  taosArrayDestroy(s2);
}

static void tsCompRetrieveCallback(void* param, TAOS_RES* tres, int32_t numOfRows) {
  SJoinSupporter* pSupporter = (SJoinSupporter*)param;

  SSqlObj* pParentSql = pSupporter->pObj;

  SSqlObj* pSql = (SSqlObj*)tres;
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  assert(!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE));

  // check for the error code firstly
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    // todo retry if other subqueries are not failed yet 
    assert(numOfRows < 0 && numOfRows == taos_errno(pSql));
    tscError("%p sub query failed, code:%s, index:%d", pSql, tstrerror(numOfRows), pSupporter->subqueryIndex);

    pParentSql->res.code = numOfRows;
    quitAllSubquery(pParentSql, pSupporter);

    tscQueueAsyncRes(pParentSql);
    return;
  }

  if (numOfRows > 0) {  // write the compressed timestamp to disk file
    fwrite(pRes->data, pRes->numOfRows, 1, pSupporter->f);
    fclose(pSupporter->f);
    pSupporter->f = NULL;

    STSBuf* pBuf = tsBufCreateFromFile(pSupporter->path, true);
    if (pBuf == NULL) {  // in error process, close the fd
      tscError("%p invalid ts comp file from vnode, abort subquery, file size:%d", pSql, numOfRows);

      pParentSql->res.code = TAOS_SYSTEM_ERROR(errno);
      tscQueueAsyncRes(pParentSql);

      return;
    }

    if (pSupporter->pTSBuf == NULL) {
      tscDebug("%p create tmp file for ts block:%s, size:%d bytes", pSql, pBuf->path, numOfRows);
      pSupporter->pTSBuf = pBuf;
    } else {
      assert(pQueryInfo->numOfTables == 1);  // for subquery, only one
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

      tsBufMerge(pSupporter->pTSBuf, pBuf, pTableMetaInfo->vgroupIndex);
      tsBufDestory(pBuf);
    }

    // continue to retrieve ts-comp data from vnode
    if (!pRes->completed) {
      getTmpfilePath("ts-join", pSupporter->path);
      pSupporter->f = fopen(pSupporter->path, "w");
      pRes->row = pRes->numOfRows;

      taos_fetch_rows_a(tres, tsCompRetrieveCallback, param);
      return;
    }
  }

  if (hasMoreVnodesToTry(pSql)) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

    int32_t totalVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
    pTableMetaInfo->vgroupIndex += 1;
    assert(pTableMetaInfo->vgroupIndex < totalVgroups);

    tscDebug("%p results from vgroup index:%d completed, try next vgroup:%d. total vgroups:%d. current numOfRes:%" PRId64,
             pSql, pTableMetaInfo->vgroupIndex - 1, pTableMetaInfo->vgroupIndex, totalVgroups,
             pRes->numOfClauseTotal);

    pCmd->command = TSDB_SQL_SELECT;
    tscResetForNextRetrieve(&pSql->res);

    assert(pSupporter->f == NULL);
    getTmpfilePath("ts-join", pSupporter->path);
    
    // TODO check for failure
    pSupporter->f = fopen(pSupporter->path, "w");
    pRes->row = pRes->numOfRows;

    // set the callback function
    pSql->fp = tscJoinQueryCallback;
    tscProcessSql(pSql);
    return;
  }

  if (atomic_sub_fetch_32(&pSupporter->pState->numOfRemain, 1) > 0) {
    return;
  }

  tscDebug("%p all subquery retrieve ts complete, do ts block intersect", pParentSql);

  // proceeds to launched secondary query to retrieve final data
  SJoinSupporter* p1 = pParentSql->pSubs[0]->param;
  SJoinSupporter* p2 = pParentSql->pSubs[1]->param;

  STimeWindow win = TSWINDOW_INITIALIZER;
  int64_t num = doTSBlockIntersect(pParentSql, p1, p2, &win);
  if (num <= 0) {  // no result during ts intersect
    tscDebug("%p no results generated in ts intersection, free all sub SqlObj and quit", pParentSql);
    freeJoinSubqueryObj(pParentSql);
    
    return;
  }

  // launch the query the retrieve actual results from vnode along with the filtered timestamp
  SQueryInfo* pPQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, pParentSql->cmd.clauseIndex);
  updateQueryTimeRange(pPQueryInfo, &win);
  tscLaunchRealSubqueries(pParentSql);
}

static void joinRetrieveFinalResCallback(void* param, TAOS_RES* tres, int numOfRows) {
  SJoinSupporter* pSupporter = (SJoinSupporter*)param;

  SSqlObj* pParentSql = pSupporter->pObj;
  SSubqueryState* pState = pSupporter->pState;

  SSqlObj* pSql = (SSqlObj*)tres;
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  // TODO put to async res?
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    assert(numOfRows == taos_errno(pSql));

    pParentSql->res.code = numOfRows;
    tscError("%p retrieve failed, index:%d, code:%s", pSql, pSupporter->subqueryIndex, tstrerror(numOfRows));
  }

  if (numOfRows >= 0) {
    pRes->numOfTotal += pRes->numOfRows;
  }

  if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) && numOfRows == 0) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
    assert(pQueryInfo->numOfTables == 1);

    // for projection query, need to try next vnode if current vnode is exhausted
    if ((++pTableMetaInfo->vgroupIndex) < pTableMetaInfo->vgroupList->numOfVgroups) {
      pState->numOfRemain = 1;
      pState->numOfTotal = 1;

      pSql->cmd.command = TSDB_SQL_SELECT;
      pSql->fp = tscJoinQueryCallback;
      tscProcessSql(pSql);

      return;
    }
  }

  if (atomic_sub_fetch_32(&pState->numOfRemain, 1) > 0) {
    tscDebug("%p sub:%p completed, remain:%d, total:%d", pParentSql, tres, pState->numOfRemain, pState->numOfTotal);
    return;
  }

  tscDebug("%p all %d secondary subqueries retrieval completed, code:%d", tres, pState->numOfTotal, pParentSql->res.code);

  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    freeJoinSubqueryObj(pParentSql);
    pParentSql->res.completed = true;
  }

  // update the records for each subquery in parent sql object.
  for (int32_t i = 0; i < pParentSql->numOfSubs; ++i) {
    if (pParentSql->pSubs[i] == NULL) {
      continue;
    }

    SSqlRes* pRes1 = &pParentSql->pSubs[i]->res;
    pRes1->numOfClauseTotal += pRes1->numOfRows;
  }

  // data has retrieved to client, build the join results
  tscBuildResFromSubqueries(pParentSql);
}

static SJoinSupporter* tscUpdateSubqueryStatus(SSqlObj* pSql, int32_t numOfFetch) {
  int32_t notInvolved = 0;
  SJoinSupporter* pSupporter = NULL;
  SSubqueryState* pState = NULL;
  
  for(int32_t i = 0; i < pSql->numOfSubs; ++i) {
    if (pSql->pSubs[i] == NULL) {
      notInvolved++;
    } else {
      pSupporter = (SJoinSupporter*)pSql->pSubs[i]->param;
      pState = pSupporter->pState;
    }
  }
  
  assert(pState != NULL);
  if (pState != NULL) {
    pState->numOfTotal = pSql->numOfSubs;
    pState->numOfRemain = numOfFetch;
  }
  
  return pSupporter;
}

void tscFetchDatablockFromSubquery(SSqlObj* pSql) {
  assert(pSql->numOfSubs >= 1);
  
  int32_t numOfFetch = 0;
  bool hasData = true;
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    // if the subquery is NULL, it does not involved in the final result generation
    SSqlObj* pSub = pSql->pSubs[i];
    if (pSub == NULL) {
      continue;
    }
    
    SSqlRes *pRes = &pSub->res;
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSub->cmd, 0);
//    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
//    if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
//      if (pRes->row >= pRes->numOfRows && pTableMetaInfo->vgroupIndex < pTableMetaInfo->vgroupList->numOfVgroups &&
//          (!tscHasReachLimitation(pQueryInfo, pRes)) && !pRes->completed) {
//        numOfFetch++;
//      }
//    } else {
      if (!tscHasReachLimitation(pQueryInfo, pRes)) {
        if (pRes->row >= pRes->numOfRows) {
          hasData = false;

          if (!pRes->completed) {
            numOfFetch++;
          }
        }
      } else {  // has reach the limitation, no data anymore
        if (pRes->row >= pRes->numOfRows) {
          hasData = false;
          break;
        }
      }
      
    }
//  }

  // has data remains in client side, and continue to return data to app
  if (hasData) {
    tscBuildResFromSubqueries(pSql);
    return;
  } else if (numOfFetch <= 0) {
    pSql->res.completed = true;
    freeJoinSubqueryObj(pSql);
    
    if (pSql->res.code == TSDB_CODE_SUCCESS) {
      (*pSql->fp)(pSql->param, pSql, 0);
    } else {
      tscQueueAsyncRes(pSql);
    }
    
    return;
  }

  // TODO multi-vnode retrieve for projection query with limitation has bugs, since the global limiation is not handled
  tscDebug("%p retrieve data from %d subqueries", pSql, numOfFetch);
  SJoinSupporter* pSupporter = tscUpdateSubqueryStatus(pSql, numOfFetch);
  
  for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
    SSqlObj* pSql1 = pSql->pSubs[i];
    if (pSql1 == NULL) {
      continue;
    }
    
    SSqlRes* pRes1 = &pSql1->res;
    SSqlCmd* pCmd1 = &pSql1->cmd;

    pSupporter = (SJoinSupporter*)pSql1->param;

    // wait for all subqueries completed
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd1, 0);
    assert(pRes1->numOfRows >= 0 && pQueryInfo->numOfTables == 1);

    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
    
    if (pRes1->row >= pRes1->numOfRows) {
      tscDebug("%p subquery:%p retrieve data from vnode, subquery:%d, vgroupIndex:%d", pSql, pSql1,
               pSupporter->subqueryIndex, pTableMetaInfo->vgroupIndex);

      tscResetForNextRetrieve(pRes1);
      pSql1->fp = joinRetrieveFinalResCallback;

      if (pCmd1->command < TSDB_SQL_LOCAL) {
        pCmd1->command = (pCmd1->command > TSDB_SQL_MGMT) ? TSDB_SQL_RETRIEVE : TSDB_SQL_FETCH;
      }

      tscProcessSql(pSql1);
    }
  }
}

// all subqueries return, set the result output index
void tscSetupOutputColumnIndex(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  tscDebug("%p all subquery response, retrieve data", pSql);

  // the column transfer support struct has been built
  if (pRes->pColumnIndex != NULL) {
    return;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  pRes->pColumnIndex = calloc(1, sizeof(SColumnIndex) * pQueryInfo->fieldsInfo.numOfOutput);

  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);

    int32_t tableIndexOfSub = -1;
    for (int32_t j = 0; j < pQueryInfo->numOfTables; ++j) {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, j);
      if (pTableMetaInfo->pTableMeta->uid == pExpr->uid) {
        tableIndexOfSub = j;
        break;
      }
    }

    assert(tableIndexOfSub >= 0 && tableIndexOfSub < pQueryInfo->numOfTables);
    
    SSqlCmd* pSubCmd = &pSql->pSubs[tableIndexOfSub]->cmd;
    SQueryInfo* pSubQueryInfo = tscGetQueryInfoDetail(pSubCmd, 0);
    
    size_t numOfExprs = taosArrayGetSize(pSubQueryInfo->exprList);
    for (int32_t k = 0; k < numOfExprs; ++k) {
      SSqlExpr* pSubExpr = tscSqlExprGet(pSubQueryInfo, k);
      if (pExpr->functionId == pSubExpr->functionId && pExpr->colInfo.colId == pSubExpr->colInfo.colId) {
        pRes->pColumnIndex[i] = (SColumnIndex){.tableIndex = tableIndexOfSub, .columnIndex = k};
        break;
      }
    }
  }
}

void tscJoinQueryCallback(void* param, TAOS_RES* tres, int code) {
  SSqlObj* pSql = (SSqlObj*)tres;

  SJoinSupporter* pSupporter = (SJoinSupporter*)param;
  SSqlObj* pParentSql = pSupporter->pObj;

  // There is only one subquery and table for each subquery.
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  assert(pQueryInfo->numOfTables == 1 && pSql->cmd.numOfClause == 1);

  // retrieve actual query results from vnode during the second stage join subquery
  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    tscError("%p abort query due to other subquery failure. code:%d, global code:%d", pSql, code, pParentSql->res.code);
    quitAllSubquery(pParentSql, pSupporter);
    tscQueueAsyncRes(pParentSql);

    return;
  }

  // TODO here retry is required, not directly returns to client
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    assert(taos_errno(pSql) == code);

    tscError("%p abort query, code:%d, global code:%d", pSql, code, pParentSql->res.code);
    pParentSql->res.code = code;

    quitAllSubquery(pParentSql, pSupporter);
    tscQueueAsyncRes(pParentSql);

    return;
  }

  // retrieve <tid, tag> tuples from vnode
  if (TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY)) {
    pSql->fp = tidTagRetrieveCallback;
    pSql->cmd.command = TSDB_SQL_FETCH;
    tscProcessSql(pSql);
    return;
  }

  // retrieve ts_comp info from vnode
  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE)) {
    pSql->fp = tsCompRetrieveCallback;
    pSql->cmd.command = TSDB_SQL_FETCH;
    tscProcessSql(pSql);
    return;
  }

  // wait for the other subqueries response from vnode
  if (atomic_sub_fetch_32(&pSupporter->pState->numOfRemain, 1) > 0) {
    return;
  }

  tscSetupOutputColumnIndex(pParentSql);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  /**
   * if the query is a continue query (vgroupIndex > 0 for projection query) for next vnode, do the retrieval of
   * data instead of returning to its invoker
   */
  if (pTableMetaInfo->vgroupIndex > 0 && tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    pSupporter->pState->numOfRemain = pSupporter->pState->numOfTotal;  // reset the record value

    pSql->fp = joinRetrieveFinalResCallback;  // continue retrieve data
    pSql->cmd.command = TSDB_SQL_FETCH;
    tscProcessSql(pSql);
  } else {  // first retrieve from vnode during the secondary stage sub-query
    // set the command flag must be after the semaphore been correctly set.
    if (pParentSql->res.code == TSDB_CODE_SUCCESS) {
      (*pParentSql->fp)(pParentSql->param, pParentSql, 0);
    } else {
      tscQueueAsyncRes(pParentSql);
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////
static void tscRetrieveDataRes(void *param, TAOS_RES *tres, int code);

static SSqlObj *tscCreateSqlObjForSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj *prevSqlObj);

int32_t tscLaunchJoinSubquery(SSqlObj *pSql, int16_t tableIndex, SJoinSupporter *pSupporter) {
  SSqlCmd *   pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
  pSql->res.qhandle = 0x1;
  assert(pSql->res.numOfRows == 0);

  if (pSql->pSubs == NULL) {
    pSql->pSubs = calloc(pSupporter->pState->numOfTotal, POINTER_BYTES);
    if (pSql->pSubs == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }
  
  SSqlObj *pNew = createSubqueryObj(pSql, tableIndex, tscJoinQueryCallback, pSupporter, TSDB_SQL_SELECT, NULL);
  if (pNew == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  
  pSql->pSubs[pSql->numOfSubs++] = pNew;
  assert(pSql->numOfSubs <= pSupporter->pState->numOfTotal);
  
  if (QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
    addGroupInfoForSubquery(pSql, pNew, 0, tableIndex);
    
    // refactor as one method
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    assert(pNewQueryInfo != NULL);
    
    // update the table index
    size_t num = taosArrayGetSize(pNewQueryInfo->colList);
    for (int32_t i = 0; i < num; ++i) {
      SColumn* pCol = taosArrayGetP(pNewQueryInfo->colList, i);
      pCol->colIndex.tableIndex = 0;
    }
    
    pSupporter->colList = pNewQueryInfo->colList;
    pNewQueryInfo->colList = NULL;
    
    pSupporter->exprList = pNewQueryInfo->exprList;
    pNewQueryInfo->exprList = NULL;
    
    pSupporter->fieldsInfo = pNewQueryInfo->fieldsInfo;
  
    // this data needs to be transfer to support struct
    memset(&pNewQueryInfo->fieldsInfo, 0, sizeof(SFieldInfo));
    tscTagCondCopy(&pSupporter->tagCond, &pNewQueryInfo->tagCond);//pNewQueryInfo->tagCond;

    pNew->cmd.numOfCols = 0;
    pNewQueryInfo->intervalTime = 0;
    memset(&pNewQueryInfo->limit, 0, sizeof(SLimitVal));
    
    // backup the data and clear it in the sqlcmd object
    pSupporter->groupbyExpr = pNewQueryInfo->groupbyExpr;
    memset(&pNewQueryInfo->groupbyExpr, 0, sizeof(SSqlGroupbyExpr));
    
    tscInitQueryInfo(pNewQueryInfo);
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pNewQueryInfo, 0);
    
    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) { // return the tableId & tag
      SColumnIndex index = {0};

      STagCond* pTagCond = &pSupporter->tagCond;
      assert(pTagCond->joinInfo.hasJoin);

      int32_t tagColId = tscGetJoinTagColIdByUid(pTagCond, pTableMetaInfo->pTableMeta->uid);
      SSchema* s = tscGetTableColumnSchemaById(pTableMetaInfo->pTableMeta, tagColId);

      int16_t bytes = 0;
      int16_t type  = 0;
      int32_t inter = 0;

      getResultDataInfo(s->type, s->bytes, TSDB_FUNC_TID_TAG, 0, &type, &bytes, &inter, 0, 0);

      SSchema s1 = {.colId = s->colId, .type = type, .bytes = bytes};
      pSupporter->tagSize = s1.bytes;
      assert(isValidDataType(s1.type) && s1.bytes > 0);

      // set get tags query type
      TSDB_QUERY_SET_TYPE(pNewQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY);
      tscAddSpecialColumnForSelect(pNewQueryInfo, 0, TSDB_FUNC_TID_TAG, &index, &s1, TSDB_COL_TAG);
      size_t numOfCols = taosArrayGetSize(pNewQueryInfo->colList);
  
      tscDebug(
          "%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%d, transfer to tid_tag query to retrieve (tableId, tags), "
          "exprInfo:%zu, colList:%zu, fieldsInfo:%d, tagIndex:%d, name:%s",
          pSql, pNew, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, tscSqlExprNumOfExprs(pNewQueryInfo),
          numOfCols, pNewQueryInfo->fieldsInfo.numOfOutput, index.columnIndex, pNewQueryInfo->pTableMetaInfo[0]->name);
    } else {
      SSchema      colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = 1};
      SColumnIndex index = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
      tscAddSpecialColumnForSelect(pNewQueryInfo, 0, TSDB_FUNC_TS_COMP, &index, &colSchema, TSDB_COL_NORMAL);

      // set the tags value for ts_comp function
      SSqlExpr *pExpr = tscSqlExprGet(pNewQueryInfo, 0);

      if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
        int16_t tagColId = tscGetJoinTagColIdByUid(&pSupporter->tagCond, pTableMetaInfo->pTableMeta->uid);
        pExpr->param->i64Key = tagColId;
        pExpr->numOfParams = 1;
      }

      // add the filter tag column
      if (pSupporter->colList != NULL) {
        size_t s = taosArrayGetSize(pSupporter->colList);

        for (int32_t i = 0; i < s; ++i) {
          SColumn *pCol = taosArrayGetP(pSupporter->colList, i);

          if (pCol->numOfFilters > 0) {  // copy to the pNew->cmd.colList if it is filtered.
            SColumn *p = tscColumnClone(pCol);
            taosArrayPush(pNewQueryInfo->colList, &p);
          }
        }
      }

      size_t numOfCols = taosArrayGetSize(pNewQueryInfo->colList);

      tscDebug(
          "%p subquery:%p tableIndex:%d, vgroupIndex:%d, type:%u, transfer to ts_comp query to retrieve timestamps, "
          "exprInfo:%zu, colList:%zu, fieldsInfo:%d, name:%s",
          pSql, pNew, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, tscSqlExprNumOfExprs(pNewQueryInfo),
          numOfCols, pNewQueryInfo->fieldsInfo.numOfOutput, pNewQueryInfo->pTableMetaInfo[0]->name);
    }
  } else {
    assert(0);
    SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pNewQueryInfo->type |= TSDB_QUERY_TYPE_SUBQUERY;
  }

  return tscProcessSql(pNew);
}

int32_t tscHandleMasterJoinQuery(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  assert((pQueryInfo->type & TSDB_QUERY_TYPE_SUBQUERY) == 0);
  
  SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
  pState->numOfTotal = pQueryInfo->numOfTables;
  pState->numOfRemain = pState->numOfTotal;

  tscDebug("%p start subquery, total:%d", pSql, pQueryInfo->numOfTables);
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    SJoinSupporter *pSupporter = tscCreateJoinSupporter(pSql, pState, i);
    
    if (pSupporter == NULL) {  // failed to create support struct, abort current query
      tscError("%p tableIndex:%d, failed to allocate join support object, abort further query", pSql, i);
      pState->numOfRemain = i;
      pSql->res.code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      
      return pSql->res.code;
    }
    
    int32_t code = tscLaunchJoinSubquery(pSql, i, pSupporter);
    if (code != TSDB_CODE_SUCCESS) {  // failed to create subquery object, quit query
      tscDestroyJoinSupporter(pSupporter);
      pSql->res.code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      
      break;
    }
  }
  
  pSql->cmd.command = (pSql->numOfSubs <= 0)? TSDB_SQL_RETRIEVE_EMPTY_RESULT:TSDB_SQL_TABLE_JOIN_RETRIEVE;
  
  return TSDB_CODE_SUCCESS;
}

static void doCleanupSubqueries(SSqlObj *pSql, int32_t numOfSubs, SSubqueryState* pState) {
  assert(numOfSubs <= pSql->numOfSubs && numOfSubs >= 0 && pState != NULL);
  
  for(int32_t i = 0; i < numOfSubs; ++i) {
    SSqlObj* pSub = pSql->pSubs[i];
    assert(pSub != NULL);
    
    SRetrieveSupport* pSupport = pSub->param;
    
    tfree(pSupport->localBuffer);
    
    pthread_mutex_unlock(&pSupport->queryMutex);
    pthread_mutex_destroy(&pSupport->queryMutex);
    
    tfree(pSupport);
    
    tscFreeSqlObj(pSub);
  }
  
  free(pState);
}

int32_t tscHandleMasterSTableQuery(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  // pRes->code check only serves in launching metric sub-queries
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    pCmd->command = TSDB_SQL_RETRIEVE_LOCALMERGE;  // enable the abort of kill super table function.
    return pRes->code;
  }
  
  tExtMemBuffer **  pMemoryBuf = NULL;
  tOrderDescriptor *pDesc = NULL;
  SColumnModel *    pModel = NULL;
  
  pRes->qhandle = 0x1;  // hack the qhandle check
  
  const uint32_t nBufferSize = (1u << 16);  // 64KB
  
  SQueryInfo *    pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  pSql->numOfSubs = pTableMetaInfo->vgroupList->numOfVgroups;
  assert(pSql->numOfSubs > 0);
  
  int32_t ret = tscLocalReducerEnvCreate(pSql, &pMemoryBuf, &pDesc, &pModel, nBufferSize);
  if (ret != 0) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tscQueueAsyncRes(pSql);
    tfree(pMemoryBuf);
    return ret;
  }
  
  pSql->pSubs = calloc(pSql->numOfSubs, POINTER_BYTES);
  
  tscDebug("%p retrieved query data from %d vnode(s)", pSql, pSql->numOfSubs);
  SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
  pState->numOfTotal = pSql->numOfSubs;
  pState->numOfRemain = pSql->numOfSubs;

  pRes->code = TSDB_CODE_SUCCESS;
  
  int32_t i = 0;
  for (; i < pSql->numOfSubs; ++i) {
    SRetrieveSupport *trs = (SRetrieveSupport *)calloc(1, sizeof(SRetrieveSupport));
    if (trs == NULL) {
      tscError("%p failed to malloc buffer for SRetrieveSupport, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      break;
    }
    
    trs->pExtMemBuffer = pMemoryBuf;
    trs->pOrderDescriptor = pDesc;
    trs->pState = pState;
    
    trs->localBuffer = (tFilePage *)calloc(1, nBufferSize + sizeof(tFilePage));
    if (trs->localBuffer == NULL) {
      tscError("%p failed to malloc buffer for local buffer, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      tfree(trs);
      break;
    }
    
    trs->subqueryIndex = i;
    trs->pParentSql = pSql;
    trs->pFinalColModel = pModel;
    
    pthread_mutexattr_t mutexattr;
    memset(&mutexattr, 0, sizeof(pthread_mutexattr_t));

    pthread_mutexattr_settype(&mutexattr, PTHREAD_MUTEX_RECURSIVE_NP);
    pthread_mutex_init(&trs->queryMutex, &mutexattr);
    pthread_mutexattr_destroy(&mutexattr);
    
    SSqlObj *pNew = tscCreateSqlObjForSubquery(pSql, trs, NULL);
    if (pNew == NULL) {
      tscError("%p failed to malloc buffer for subObj, orderOfSub:%d, reason:%s", pSql, i, strerror(errno));
      tfree(trs->localBuffer);
      tfree(trs);
      break;
    }
    
    // todo handle multi-vnode situation
    if (pQueryInfo->tsBuf) {
      SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
      pNewQueryInfo->tsBuf = tsBufClone(pQueryInfo->tsBuf);
      assert(pNewQueryInfo->tsBuf != NULL);
    }
    
    tscDebug("%p sub:%p create subquery success. orderOfSub:%d", pSql, pNew, trs->subqueryIndex);
  }
  
  if (i < pSql->numOfSubs) {
    tscError("%p failed to prepare subquery structure and launch subqueries", pSql);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, pSql->numOfSubs);
    doCleanupSubqueries(pSql, i, pState);
    return pRes->code;   // free all allocated resource
  }
  
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    tscLocalReducerEnvDestroy(pMemoryBuf, pDesc, pModel, pSql->numOfSubs);
    doCleanupSubqueries(pSql, i, pState);
    return pRes->code;
  }
  
  for(int32_t j = 0; j < pSql->numOfSubs; ++j) {
    SSqlObj* pSub = pSql->pSubs[j];
    SRetrieveSupport* pSupport = pSub->param;
    
    tscDebug("%p sub:%p launch subquery, orderOfSub:%d.", pSql, pSub, pSupport->subqueryIndex);
    tscProcessSql(pSub);
  }
  
  return TSDB_CODE_SUCCESS;
}

static void tscFreeSubSqlObj(SRetrieveSupport *trsupport, SSqlObj *pSql) {
  tscDebug("%p start to free subquery result", pSql);
  
  if (pSql->res.code == TSDB_CODE_SUCCESS) {
    taos_free_result(pSql);
  }
  
  tfree(trsupport->localBuffer);
  
  pthread_mutex_unlock(&trsupport->queryMutex);
  pthread_mutex_destroy(&trsupport->queryMutex);
  
  tfree(trsupport);
}

static void tscRetrieveFromDnodeCallBack(void *param, TAOS_RES *tres, int numOfRows);
static void tscHandleSubqueryError(SRetrieveSupport *trsupport, SSqlObj *pSql, int numOfRows);

static void tscAbortFurtherRetryRetrieval(SRetrieveSupport *trsupport, TAOS_RES *tres, int32_t code) {
// set no disk space error info
#ifdef WINDOWS
  LPVOID lpMsgBuf;
  FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
                GetLastError(), MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),  // Default language
                (LPTSTR)&lpMsgBuf, 0, NULL);
  tscError("sub:%p failed to flush data to disk:reason:%s", tres, lpMsgBuf);
  LocalFree(lpMsgBuf);
#else
  tscError("sub:%p failed to flush data to disk, reason:%s", tres, tstrerror(code));
#endif

  SSqlObj* pParentSql = trsupport->pParentSql;

  pParentSql->res.code = code;
  trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
  
  pthread_mutex_unlock(&trsupport->queryMutex);
  tscHandleSubqueryError(trsupport, tres, pParentSql->res.code);
}

/*
 * current query failed, and the retry count is less than the available
 * count, retry query clear previous retrieved data, then launch a new sub query
 */
static int32_t tscReissueSubquery(SRetrieveSupport *trsupport, SSqlObj *pSql, int32_t code) {
  SSqlObj *pParentSql = trsupport->pParentSql;
  int32_t  subqueryIndex = trsupport->subqueryIndex;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  SCMVgroupInfo* pVgroup = &pTableMetaInfo->vgroupList->vgroups[0];

  tExtMemBufferClear(trsupport->pExtMemBuffer[subqueryIndex]);

  // clear local saved number of results
  trsupport->localBuffer->num = 0;
  pthread_mutex_unlock(&trsupport->queryMutex);

  tscTrace("%p sub:%p retrieve failed, code:%s, orderOfSub:%d, retry:%d", trsupport->pParentSql, pSql,
           tstrerror(code), subqueryIndex, trsupport->numOfRetry);

  SSqlObj *pNew = tscCreateSqlObjForSubquery(trsupport->pParentSql, trsupport, pSql);

  // todo add to async res or not??
  if (pNew == NULL) {
    tscError("%p sub:%p failed to create new subquery due to out of memory, abort retry, vgId:%d, orderOfSub:%d",
             trsupport->pParentSql, pSql, pVgroup->vgId, trsupport->subqueryIndex);

    pParentSql->res.code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;

    return pParentSql->res.code;
  }

  taos_free_result(pSql);
  return tscProcessSql(pNew);
}

void tscHandleSubqueryError(SRetrieveSupport *trsupport, SSqlObj *pSql, int numOfRows) {
  SSqlObj *pParentSql = trsupport->pParentSql;
  int32_t  subqueryIndex = trsupport->subqueryIndex;
  
  assert(pSql != NULL);
  SSubqueryState* pState = trsupport->pState;
  assert(pState->numOfRemain <= pState->numOfTotal && pState->numOfRemain >= 0 && pParentSql->numOfSubs == pState->numOfTotal);
  
  // retrieved in subquery failed. OR query cancelled in retrieve phase.
  if (taos_errno(pSql) == TSDB_CODE_SUCCESS && pParentSql->res.code != TSDB_CODE_SUCCESS) {

    /*
     * kill current sub-query connection, which may retrieve data from vnodes;
     * Here we get: pPObj->res.code == TSDB_CODE_TSC_QUERY_CANCELLED
     */
    pSql->res.numOfRows = 0;
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;  // disable retry efforts
    tscDebug("%p query is cancelled, sub:%p, orderOfSub:%d abort retrieve, code:%d", pParentSql, pSql,
             subqueryIndex, pParentSql->res.code);
  }

  if (numOfRows >= 0) {  // current query is successful, but other sub query failed, still abort current query.
    tscDebug("%p sub:%p retrieve numOfRows:%d,orderOfSub:%d", pParentSql, pSql, numOfRows, subqueryIndex);
    tscError("%p sub:%p abort further retrieval due to other queries failure,orderOfSub:%d,code:%d", pParentSql, pSql,
             subqueryIndex, pParentSql->res.code);
  } else {
    if (trsupport->numOfRetry++ < MAX_NUM_OF_SUBQUERY_RETRY && pParentSql->res.code == TSDB_CODE_SUCCESS) {
      if (tscReissueSubquery(trsupport, pSql, numOfRows) == TSDB_CODE_SUCCESS) {
        return;
      }
    } else {  // reach the maximum retry count, abort
      atomic_val_compare_exchange_32(&pParentSql->res.code, TSDB_CODE_SUCCESS, numOfRows);
      tscError("%p sub:%p retrieve failed,code:%s,orderOfSub:%d failed.no more retry,set global code:%s", pParentSql, pSql,
               tstrerror(numOfRows), subqueryIndex, tstrerror(pParentSql->res.code));
    }
  }

  int32_t remain = -1;
  if ((remain = atomic_sub_fetch_32(&pState->numOfRemain, 1)) > 0) {
    tscDebug("%p sub:%p orderOfSub:%d freed, finished subqueries:%d", pParentSql, pSql, trsupport->subqueryIndex,
        pState->numOfTotal - remain);

    return tscFreeSubSqlObj(trsupport, pSql);
  }
  
  // all subqueries are failed
  tscError("%p retrieve from %d vnode(s) completed,code:%s.FAILED.", pParentSql, pState->numOfTotal,
      tstrerror(pParentSql->res.code));

  // release allocated resource
  tscLocalReducerEnvDestroy(trsupport->pExtMemBuffer, trsupport->pOrderDescriptor, trsupport->pFinalColModel,
                            pState->numOfTotal);
  
  tfree(trsupport->pState);
  tscFreeSubSqlObj(trsupport, pSql);
  
  // in case of second stage join subquery, invoke its callback function instead of regular QueueAsyncRes
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, 0);
  
  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE)) {
    (*pParentSql->fp)(pParentSql->param, pParentSql, pParentSql->res.code);
  } else {  // regular super table query
    if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
      tscQueueAsyncRes(pParentSql);
    }
  }
}

static void tscAllDataRetrievedFromDnode(SRetrieveSupport *trsupport, SSqlObj* pSql) {
  int32_t           idx = trsupport->subqueryIndex;
  SSqlObj *         pParentSql = trsupport->pParentSql;
  tOrderDescriptor *pDesc = trsupport->pOrderDescriptor;
  
  SSubqueryState* pState = trsupport->pState;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[0];
  
  // data in from current vnode is stored in cache and disk
  uint32_t numOfRowsFromSubquery = trsupport->pExtMemBuffer[idx]->numOfTotalElems + trsupport->localBuffer->num;
    tscDebug("%p sub:%p all data retrieved from ip:%s, vgId:%d, numOfRows:%d, orderOfSub:%d", pParentSql, pSql,
        pTableMetaInfo->vgroupList->vgroups[0].ipAddr[0].fqdn, pTableMetaInfo->vgroupList->vgroups[0].vgId,
        numOfRowsFromSubquery, idx);
  
  tColModelCompact(pDesc->pColumnModel, trsupport->localBuffer, pDesc->pColumnModel->capacity);

#ifdef _DEBUG_VIEW
  printf("%" PRIu64 " rows data flushed to disk:\n", trsupport->localBuffer->num);
    SSrcColumnInfo colInfo[256] = {0};
    tscGetSrcColumnInfo(colInfo, pQueryInfo);
    tColModelDisplayEx(pDesc->pColumnModel, trsupport->localBuffer->data, trsupport->localBuffer->num,
                       trsupport->localBuffer->num, colInfo);
#endif
  
  if (tsTotalTmpDirGB != 0 && tsAvailTmpDirectorySpace < tsReservedTmpDirectorySpace) {
    tscError("%p sub:%p client disk space remain %.3f GB, need at least %.3f GB, stop query", pParentSql, pSql,
             tsAvailTmpDirectorySpace, tsReservedTmpDirectorySpace);
    return tscAbortFurtherRetryRetrieval(trsupport, pSql, TSDB_CODE_TSC_NO_DISKSPACE);
  }
  
  // each result for a vnode is ordered as an independant list,
  // then used as an input of loser tree for disk-based merge
  int32_t code = tscFlushTmpBuffer(trsupport->pExtMemBuffer[idx], pDesc, trsupport->localBuffer, pQueryInfo->groupbyExpr.orderType);
  if (code != 0) { // set no disk space error info, and abort retry
    return tscAbortFurtherRetryRetrieval(trsupport, pSql, code);
  }
  
  int32_t remain = -1;
  if ((remain = atomic_sub_fetch_32(&pState->numOfRemain, 1)) > 0) {
    tscDebug("%p sub:%p orderOfSub:%d freed, finished subqueries:%d", pParentSql, pSql, trsupport->subqueryIndex,
        pState->numOfTotal - remain);

    return tscFreeSubSqlObj(trsupport, pSql);
  }
  
  // all sub-queries are returned, start to local merge process
  pDesc->pColumnModel->capacity = trsupport->pExtMemBuffer[idx]->numOfElemsPerPage;
  
  tscDebug("%p retrieve from %d vnodes completed.final NumOfRows:%" PRId64 ",start to build loser tree", pParentSql,
           pState->numOfTotal, pState->numOfRetrievedRows);
  
  SQueryInfo *pPQueryInfo = tscGetQueryInfoDetail(&pParentSql->cmd, 0);
  tscClearInterpInfo(pPQueryInfo);
  
  tscCreateLocalReducer(trsupport->pExtMemBuffer, pState->numOfTotal, pDesc, trsupport->pFinalColModel, pParentSql);
  tscDebug("%p build loser tree completed", pParentSql);
  
  pParentSql->res.precision = pSql->res.precision;
  pParentSql->res.numOfRows = 0;
  pParentSql->res.row = 0;
  
  // only free once
  tfree(trsupport->pState);
  tscFreeSubSqlObj(trsupport, pSql);
  
  // set the command flag must be after the semaphore been correctly set.
  pParentSql->cmd.command = TSDB_SQL_RETRIEVE_LOCALMERGE;
  if (pParentSql->res.code == TSDB_CODE_SUCCESS) {
    (*pParentSql->fp)(pParentSql->param, pParentSql, 0);
  } else {
    tscQueueAsyncRes(pParentSql);
  }
}

static void tscRetrieveFromDnodeCallBack(void *param, TAOS_RES *tres, int numOfRows) {
  SRetrieveSupport *trsupport = (SRetrieveSupport *)param;
  tOrderDescriptor *pDesc = trsupport->pOrderDescriptor;
  int32_t           idx   = trsupport->subqueryIndex;
  SSqlObj *         pParentSql = trsupport->pParentSql;

  SSqlObj *pSql = (SSqlObj *)tres;
  if (pSql == NULL) {  // sql object has been released in error process, return immediately
    tscDebug("%p subquery has been released, idx:%d, abort", pParentSql, idx);
    return;
  }
  
  SSubqueryState* pState = trsupport->pState;
  assert(pState->numOfRemain <= pState->numOfTotal && pState->numOfRemain >= 0 && pParentSql->numOfSubs == pState->numOfTotal);
  
  // query process and cancel query process may execute at the same time
  pthread_mutex_lock(&trsupport->queryMutex);

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  SCMVgroupInfo* pVgroup = &pTableMetaInfo->vgroupList->vgroups[0];

  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
    tscTrace("%p query cancelled or failed, sub:%p, vgId:%d, orderOfSub:%d, code:%s, global code:%s",
             pParentSql, pSql, pVgroup->vgId, trsupport->subqueryIndex, tstrerror(numOfRows), tstrerror(pParentSql->res.code));

    tscHandleSubqueryError(param, tres, numOfRows);
    return;
  }

  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    assert(numOfRows == taos_errno(pSql));

    if (trsupport->numOfRetry++ < MAX_NUM_OF_SUBQUERY_RETRY) {
      tscTrace("%p sub:%p failed code:%s, retry:%d", pParentSql, pSql, tstrerror(numOfRows), trsupport->numOfRetry);

      if (tscReissueSubquery(trsupport, pSql, numOfRows) == TSDB_CODE_SUCCESS) {
        return;
      }
    } else {
      tscTrace("%p sub:%p reach the max retry times, set global code:%s", pParentSql, pSql, tstrerror(numOfRows));
      atomic_val_compare_exchange_32(&pParentSql->res.code, TSDB_CODE_SUCCESS, numOfRows);  // set global code and abort
    }

    tscHandleSubqueryError(param, tres, numOfRows);
    return;
  }
  
  SSqlRes *   pRes = &pSql->res;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  if (numOfRows > 0) {
    assert(pRes->numOfRows == numOfRows);
    int64_t num = atomic_add_fetch_64(&pState->numOfRetrievedRows, numOfRows);
    
    tscDebug("%p sub:%p retrieve numOfRows:%" PRId64 " totalNumOfRows:%" PRIu64 " from ip:%s, orderOfSub:%d", pParentSql, pSql,
             pRes->numOfRows, pState->numOfRetrievedRows, pSql->ipList.fqdn[pSql->ipList.inUse], idx);

    if (num > tsMaxNumOfOrderedResults && tscIsProjectionQueryOnSTable(pQueryInfo, 0)) {
      tscError("%p sub:%p num of OrderedRes is too many, max allowed:%" PRId32 " , current:%" PRId64,
               pParentSql, pSql, tsMaxNumOfOrderedResults, num);
      return tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_TSC_SORTED_RES_TOO_MANY);
    }

#ifdef _DEBUG_VIEW
    printf("received data from vnode: %"PRIu64" rows\n", pRes->numOfRows);
    SSrcColumnInfo colInfo[256] = {0};

    tscGetSrcColumnInfo(colInfo, pQueryInfo);
    tColModelDisplayEx(pDesc->pColumnModel, pRes->data, pRes->numOfRows, pRes->numOfRows, colInfo);
#endif
    
    // no disk space for tmp directory
    if (tsTotalTmpDirGB != 0 && tsAvailTmpDirectorySpace < tsReservedTmpDirectorySpace) {
      tscError("%p sub:%p client disk space remain %.3f GB, need at least %.3f GB, stop query", pParentSql, pSql,
               tsAvailTmpDirectorySpace, tsReservedTmpDirectorySpace);
      return tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_TSC_NO_DISKSPACE);
    }
    
    int32_t ret = saveToBuffer(trsupport->pExtMemBuffer[idx], pDesc, trsupport->localBuffer, pRes->data,
                               pRes->numOfRows, pQueryInfo->groupbyExpr.orderType);
    if (ret != 0) { // set no disk space error info, and abort retry
      tscAbortFurtherRetryRetrieval(trsupport, tres, TSDB_CODE_TSC_NO_DISKSPACE);
      
    } else if (pRes->completed) {
      tscAllDataRetrievedFromDnode(trsupport, pSql);
      return;
      
    } else { // continue fetch data from dnode
      pthread_mutex_unlock(&trsupport->queryMutex);
      taos_fetch_rows_a(tres, tscRetrieveFromDnodeCallBack, param);
    }
    
  } else { // all data has been retrieved to client
    tscAllDataRetrievedFromDnode(trsupport, pSql);
  }
}

static SSqlObj *tscCreateSqlObjForSubquery(SSqlObj *pSql, SRetrieveSupport *trsupport, SSqlObj *prevSqlObj) {
  const int32_t table_index = 0;
  
  SSqlObj *pNew = createSubqueryObj(pSql, table_index, tscRetrieveDataRes, trsupport, TSDB_SQL_SELECT, prevSqlObj);
  if (pNew != NULL) {  // the sub query of two-stage super table query
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pNew->cmd, 0);
    pQueryInfo->type |= TSDB_QUERY_TYPE_STABLE_SUBQUERY;
    
    assert(pQueryInfo->numOfTables == 1 && pNew->cmd.numOfClause == 1);
    
    // launch subquery for each vnode, so the subquery index equals to the vgroupIndex.
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, table_index);
    pTableMetaInfo->vgroupIndex = trsupport->subqueryIndex;
    
    pSql->pSubs[trsupport->subqueryIndex] = pNew;
  }
  
  return pNew;
}

void tscRetrieveDataRes(void *param, TAOS_RES *tres, int code) {
  SRetrieveSupport *trsupport = (SRetrieveSupport *) param;
  
  SSqlObj*  pParentSql = trsupport->pParentSql;
  SSqlObj*  pSql = (SSqlObj *) tres;
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  assert(pSql->cmd.numOfClause == 1 && pQueryInfo->numOfTables == 1);
  
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  SCMVgroupInfo* pVgroup = &pTableMetaInfo->vgroupList->vgroups[trsupport->subqueryIndex];

  // stable query killed or other subquery failed, all query stopped
  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
    tscTrace("%p query cancelled or failed, sub:%p, vgId:%d, orderOfSub:%d, code:%s, global code:%s",
        pParentSql, pSql, pVgroup->vgId, trsupport->subqueryIndex, tstrerror(code), tstrerror(pParentSql->res.code));

    tscHandleSubqueryError(param, tres, code);
    return;
  }
  
  /*
   * if a subquery on a vnode failed, all retrieve operations from vnode that occurs later
   * than this one are actually not necessary, we simply call the tscRetrieveFromDnodeCallBack
   * function to abort current and remain retrieve process.
   *
   * NOTE: thread safe is required.
   */
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    assert(code == taos_errno(pSql));

    if (trsupport->numOfRetry++ < MAX_NUM_OF_SUBQUERY_RETRY) {
      tscTrace("%p sub:%p failed code:%s, retry:%d", pParentSql, pSql, tstrerror(code), trsupport->numOfRetry);
      if (tscReissueSubquery(trsupport, pSql, code) == TSDB_CODE_SUCCESS) {
        return;
      }
    } else {
      tscTrace("%p sub:%p reach the max retry times, set global code:%s", pParentSql, pSql, tstrerror(code));
      atomic_val_compare_exchange_32(&pParentSql->res.code, TSDB_CODE_SUCCESS, code);  // set global code and abort
    }

    tscHandleSubqueryError(param, tres, pParentSql->res.code);
    return;
  }

  tscTrace("%p sub:%p query complete, ip:%s, vgId:%d, orderOfSub:%d, retrieve data", trsupport->pParentSql, pSql,
             pVgroup->ipAddr[0].fqdn, pVgroup->vgId, trsupport->subqueryIndex);

  if (pSql->res.qhandle == 0) { // qhandle is NULL, code is TSDB_CODE_SUCCESS means no results generated from this vnode
    tscRetrieveFromDnodeCallBack(param, pSql, 0);
  } else {
    taos_fetch_rows_a(tres, tscRetrieveFromDnodeCallBack, param);
  }
}

static void multiVnodeInsertFinalize(void* param, TAOS_RES* tres, int numOfRows) {
  SInsertSupporter *pSupporter = (SInsertSupporter *)param;
  SSqlObj* pParentObj = pSupporter->pSql;
  SSubqueryState* pState = pSupporter->pState;

  // record the total inserted rows
  if (numOfRows > 0) {
    pParentObj->res.numOfRows += numOfRows;
  }

  if (taos_errno(tres) != TSDB_CODE_SUCCESS) {
    SSqlObj* pSql = (SSqlObj*) tres;
    assert(pSql != NULL && pSql->res.code == numOfRows);
    
    pParentObj->res.code = pSql->res.code;
  }

  taos_free_result(tres);
  tfree(pSupporter);

  if (atomic_sub_fetch_32(&pState->numOfRemain, 1) > 0) {
    return;
  }
  
  tscDebug("%p Async insertion completed, total inserted:%" PRId64, pParentObj, pParentObj->res.numOfRows);

  // release data block data
  tfree(pState);

  // restore user defined fp
  pParentObj->fp = pParentObj->fetchFp;

  // todo remove this parameter in async callback function definition.
  // all data has been sent to vnode, call user function
  int32_t v = (pParentObj->res.code != TSDB_CODE_SUCCESS)? pParentObj->res.code:pParentObj->res.numOfRows;
  (*pParentObj->fp)(pParentObj->param, pParentObj, v);
}

/**
 * it is a subquery, so after parse the sql string, copy the submit block to payload of itself
 * @param pSql
 * @return
 */
int32_t tscHandleInsertRetry(SSqlObj* pSql) {
  assert(pSql != NULL && pSql->param != NULL);
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SInsertSupporter* pSupporter = (SInsertSupporter*) pSql->param;
  assert(pSupporter->index < pSupporter->pState->numOfTotal);

  STableDataBlocks* pTableDataBlock = taosArrayGetP(pCmd->pDataBlocks, pSupporter->index);
  pRes->code = tscCopyDataBlockToPayload(pSql, pTableDataBlock);
  if (pRes->code != TSDB_CODE_SUCCESS) {
    return pRes->code;
  }

  return tscProcessSql(pSql);
}

int32_t tscHandleMultivnodeInsert(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  size_t size = taosArrayGetSize(pCmd->pDataBlocks);
  assert(size > 0);

  pSql->pSubs = calloc(size, POINTER_BYTES);
  pSql->numOfSubs = size;

  tscDebug("%p submit data to %zu vnode(s)", pSql, size);

  SSubqueryState *pState = calloc(1, sizeof(SSubqueryState));
  pState->numOfTotal = pSql->numOfSubs;
  pState->numOfRemain = pSql->numOfSubs;
 
  pRes->code = TSDB_CODE_SUCCESS;
  int32_t numOfSub = 0;

  while(numOfSub < pSql->numOfSubs) {
    SInsertSupporter* pSupporter = calloc(1, sizeof(SInsertSupporter));
    pSupporter->pSql   = pSql;
    pSupporter->pState = pState;
    pSupporter->index  = numOfSub;

    SSqlObj *pNew = createSimpleSubObj(pSql, multiVnodeInsertFinalize, pSupporter, TSDB_SQL_INSERT);
    if (pNew == NULL) {
      tscError("%p failed to malloc buffer for subObj, orderOfSub:%d, reason:%s", pSql, numOfSub, strerror(errno));
      goto _error;
    }
  
    /*
     * assign the callback function to fetchFp to make sure that the error process function can restore
     * the callback function (multiVnodeInsertFinalize) correctly.
     */
    pNew->fetchFp = pNew->fp;
    pSql->pSubs[numOfSub] = pNew;

    STableDataBlocks* pTableDataBlock = taosArrayGetP(pCmd->pDataBlocks, numOfSub);
    pRes->code = tscCopyDataBlockToPayload(pNew, pTableDataBlock);
    if (pRes->code == TSDB_CODE_SUCCESS) {
      tscDebug("%p sub:%p create subObj success. orderOfSub:%d", pSql, pNew, numOfSub);
      numOfSub++;
    } else {
      tscDebug("%p prepare submit data block failed in async insertion, vnodeIdx:%d, total:%zu, code:%s", pSql, numOfSub,
               size, tstrerror(pRes->code));
      goto _error;
    }
  }
  
  if (numOfSub < pSql->numOfSubs) {
    tscError("%p failed to prepare subObj structure and launch sub-insertion", pSql);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return pRes->code;  // free all allocated resource
  }

  pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);

  // use the local variable
  for (int32_t j = 0; j < numOfSub; ++j) {
    SSqlObj *pSub = pSql->pSubs[j];
    tscDebug("%p sub:%p launch sub insert, orderOfSub:%d", pSql, pSub, j);
    tscProcessSql(pSub);
  }

  return TSDB_CODE_SUCCESS;

  _error:
  for(int32_t j = 0; j < numOfSub; ++j) {
    tfree(pSql->pSubs[j]->param);
    taos_free_result(pSql->pSubs[j]);
  }

  tfree(pState);
  return TSDB_CODE_TSC_OUT_OF_MEMORY;
}

void tscBuildResFromSubqueries(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  
  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscQueueAsyncRes(pSql);
    return;
  }
  
  while (1) {
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);
    size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
    
    if (pRes->tsrow == NULL) {
      pRes->tsrow = calloc(numOfExprs, POINTER_BYTES);
      pRes->length = calloc(numOfExprs, sizeof(int32_t));
    }
    
    bool success = false;
    
    int32_t numOfTableHasRes = 0;
    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      if (pSql->pSubs[i] != NULL) {
        numOfTableHasRes++;
      }
    }
    
    if (numOfTableHasRes >= 2) {  // do merge result
      success = (doSetResultRowData(pSql->pSubs[0], false) != NULL) && (doSetResultRowData(pSql->pSubs[1], false) != NULL);
    } else {  // only one subquery
      SSqlObj *pSub = pSql->pSubs[0];
      if (pSub == NULL) {
        pSub = pSql->pSubs[1];
      }
      
      success = (doSetResultRowData(pSub, false) != NULL);
    }
    
    if (success) {  // current row of final output has been built, return to app
      for (int32_t i = 0; i < numOfExprs; ++i) {
        SColumnIndex* pIndex = &pRes->pColumnIndex[i];
        SSqlRes *pRes1 = &pSql->pSubs[pIndex->tableIndex]->res;
        pRes->tsrow[i] = pRes1->tsrow[pIndex->columnIndex];
        pRes->length[i] = pRes1->length[pIndex->columnIndex];
      }
      
      pRes->numOfClauseTotal++;
      break;
    } else {  // continue retrieve data from vnode
      if (!tscHasRemainDataInSubqueryResultSet(pSql)) {
        tscDebug("%p at least one subquery exhausted, free all other %d subqueries", pSql, pSql->numOfSubs - 1);
        SSubqueryState *pState = NULL;
        
        // free all sub sqlobj
        for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
          SSqlObj *pChildObj = pSql->pSubs[i];
          if (pChildObj == NULL) {
            continue;
          }
          
          SJoinSupporter *pSupporter = (SJoinSupporter *)pChildObj->param;
          pState = pSupporter->pState;
          
          tscDestroyJoinSupporter(pChildObj->param);
          taos_free_result(pChildObj);
        }
        
        free(pState);
        
        pRes->completed = true; // set query completed
        sem_post(&pSql->rspSem);
        return;
      }
      
      tscFetchDatablockFromSubquery(pSql);
      if (pRes->code != TSDB_CODE_SUCCESS) {
        return;
      }
    }
  }
  
  if (pSql->res.code == TSDB_CODE_SUCCESS) {
    (*pSql->fp)(pSql->param, pSql, 0);
  } else {
    tscQueueAsyncRes(pSql);
  }
}

static void transferNcharData(SSqlObj *pSql, int32_t columnIndex, TAOS_FIELD *pField) {
  SSqlRes *pRes = &pSql->res;
  
  if (pRes->tsrow[columnIndex] != NULL && pField->type == TSDB_DATA_TYPE_NCHAR) {
    // convert unicode to native code in a temporary buffer extra one byte for terminated symbol
    if (pRes->buffer[columnIndex] == NULL) {
      pRes->buffer[columnIndex] = malloc(pField->bytes + TSDB_NCHAR_SIZE);
    }
    
    /* string terminated char for binary data*/
    memset(pRes->buffer[columnIndex], 0, pField->bytes + TSDB_NCHAR_SIZE);
    
    int32_t length = taosUcs4ToMbs(pRes->tsrow[columnIndex], pRes->length[columnIndex], pRes->buffer[columnIndex]);
    if ( length >= 0 ) {
      pRes->tsrow[columnIndex] = pRes->buffer[columnIndex];
      pRes->length[columnIndex] = length;
    } else {
      tscError("%p charset:%s to %s. val:%s convert failed.", pSql, DEFAULT_UNICODE_ENCODEC, tsCharset, (char*)pRes->tsrow[columnIndex]);
      pRes->tsrow[columnIndex] = NULL;
      pRes->length[columnIndex] = 0;
    }
  }
}

static char *getArithemicInputSrc(void *param, const char *name, int32_t colId) {
  SArithmeticSupport *pSupport = (SArithmeticSupport *) param;

  int32_t index = -1;
  SSqlExpr* pExpr = NULL;
  
  for (int32_t i = 0; i < pSupport->numOfCols; ++i) {
    pExpr = taosArrayGetP(pSupport->exprList, i);
    if (strncmp(name, pExpr->aliasName, sizeof(pExpr->aliasName) - 1) == 0) {
      index = i;
      break;
    }
  }

  assert(index >= 0 && index < pSupport->numOfCols);
  return pSupport->data[index] + pSupport->offset * pExpr->resBytes;
}

void **doSetResultRowData(SSqlObj *pSql, bool finalResult) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  
  assert(pRes->row >= 0 && pRes->row <= pRes->numOfRows);
  
  if(pCmd->command == TSDB_SQL_TABLE_JOIN_RETRIEVE) {
    if (pRes->completed) {
      tfree(pRes->tsrow);
    }

    return pRes->tsrow;
  }
  
  if (pRes->row >= pRes->numOfRows) {  // all the results has returned to invoker
    tfree(pRes->tsrow);
    return pRes->tsrow;
  }
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
  size_t size = tscNumOfFields(pQueryInfo);
  for (int i = 0; i < size; ++i) {
    SFieldSupInfo* pSup = tscFieldInfoGetSupp(&pQueryInfo->fieldsInfo, i);
    if (pSup->pSqlExpr != NULL) {
      tscGetResultColumnChr(pRes, &pQueryInfo->fieldsInfo, i);
    }
    
    // primary key column cannot be null in interval query, no need to check
    if (i == 0 && pQueryInfo->intervalTime > 0) {
      continue;
    }
    
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
    transferNcharData(pSql, i, pField);
    
    // calculate the result from several other columns
    if (pSup->pArithExprInfo != NULL) {
      if (pRes->pArithSup == NULL) {
        SArithmeticSupport *sas = (SArithmeticSupport *) calloc(1, sizeof(SArithmeticSupport));
        sas->offset     = 0;
        sas->pArithExpr = pSup->pArithExprInfo;
        sas->numOfCols  = tscSqlExprNumOfExprs(pQueryInfo);
        sas->exprList   = pQueryInfo->exprList;
        sas->data       = calloc(sas->numOfCols, POINTER_BYTES);
        
        pRes->pArithSup = sas;
      }
      
      if (pRes->buffer[i] == NULL) {
        TAOS_FIELD* field = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
        pRes->buffer[i] = malloc(field->bytes);
      }

      for(int32_t k = 0; k < pRes->pArithSup->numOfCols; ++k) {
        SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, k);
        pRes->pArithSup->data[k] = (pRes->data + pRes->numOfRows* pExpr->offset) + pRes->row*pExpr->resBytes;
      }
  
      tExprTreeCalcTraverse(pRes->pArithSup->pArithExpr->pExpr, 1, pRes->buffer[i], pRes->pArithSup,
          TSDB_ORDER_ASC, getArithemicInputSrc);
      pRes->tsrow[i] = pRes->buffer[i];
    }
  }
  
  pRes->row++;  // index increase one-step
  return pRes->tsrow;
}

static bool tscHasRemainDataInSubqueryResultSet(SSqlObj *pSql) {
  bool     hasData = true;
  SSqlCmd *pCmd = &pSql->cmd;
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
    bool allSubqueryExhausted = true;
    
    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      if (pSql->pSubs[i] == NULL) {
        continue;
      }
      
      SSqlRes *pRes1 = &pSql->pSubs[i]->res;
      SSqlCmd *pCmd1 = &pSql->pSubs[i]->cmd;
      
      SQueryInfo *pQueryInfo1 = tscGetQueryInfoDetail(pCmd1, pCmd1->clauseIndex);
      assert(pQueryInfo1->numOfTables == 1);
      
      STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo1, 0);
      
      /*
       * if the global limitation is not reached, and current result has not exhausted, or next more vnodes are
       * available, goes on
       */
      if (pTableMetaInfo->vgroupIndex < pTableMetaInfo->vgroupList->numOfVgroups && pRes1->row < pRes1->numOfRows &&
          (!tscHasReachLimitation(pQueryInfo1, pRes1))) {
        allSubqueryExhausted = false;
        break;
      }
    }
    
    hasData = !allSubqueryExhausted;
  } else {  // otherwise, in case inner join, if any subquery exhausted, query completed.
    for (int32_t i = 0; i < pSql->numOfSubs; ++i) {
      if (pSql->pSubs[i] == 0) {
        continue;
      }
      
      SSqlRes *   pRes1 = &pSql->pSubs[i]->res;
      SQueryInfo *pQueryInfo1 = tscGetQueryInfoDetail(&pSql->pSubs[i]->cmd, 0);
      
      if ((pRes1->row >= pRes1->numOfRows && tscHasReachLimitation(pQueryInfo1, pRes1) &&
          tscIsProjectionQuery(pQueryInfo1)) || (pRes1->numOfRows == 0)) {
        hasData = false;
        break;
      }
    }
  }
  
  return hasData;
}
