//#include "tskiplistQuery.h"
//
///*
// * query processor based on query condition
// */
//int32_t tSkipListQuery(SSkipList *pSkipList, tSKipListQueryCond *pQueryCond, tSkipListNode ***pResult) {
//  // query condition check
//  int32_t       rel = 0;
//  __compar_fn_t comparFn = getKeyComparator(pQueryCond->lowerBnd.nType);
//
//  if (pSkipList == NULL || pQueryCond == NULL || pSkipList->nSize == 0 ||
//      (((rel = comparFn(&pQueryCond->lowerBnd, &pQueryCond->upperBnd)) > 0 &&
//          pQueryCond->lowerBnd.nType != TSDB_DATA_TYPE_NCHAR && pQueryCond->lowerBnd.nType != TSDB_DATA_TYPE_BINARY))) {
//    (*pResult) = NULL;
//    return 0;
//  }
//
//  if (rel == 0) {
//    /*
//     * 0 means: pQueryCond->lowerBnd == pQueryCond->upperBnd
//     * point query
//     */
//    if (pQueryCond->lowerBndRelOptr == TSDB_RELATION_LARGE_EQUAL &&
//        pQueryCond->upperBndRelOptr == TSDB_RELATION_LESS_EQUAL) {  // point query
//      return tSkipListGets(pSkipList, &pQueryCond->lowerBnd, pResult);
//    } else {
//      (*pResult) = NULL;
//      return 0;
//    }
//  } else {
//    /* range query, query operation code check */
//    return tSkipListRangeQuery(pSkipList, pQueryCond, pResult);
//  }
//}
//
//typedef struct MultipleQueryResult {
//  int32_t         len;
//  tSkipListNode **pData;
//} MultipleQueryResult;
//
//static int32_t mergeQueryResult(MultipleQueryResult *pResults, int32_t numOfResSet, tSkipListNode ***pRes) {
//  int32_t total = 0;
//  for (int32_t i = 0; i < numOfResSet; ++i) {
//    total += pResults[i].len;
//  }
//
//  (*pRes) = malloc(POINTER_BYTES * total);
//  int32_t idx = 0;
//
//  for (int32_t i = 0; i < numOfResSet; ++i) {
//    MultipleQueryResult *pOneResult = &pResults[i];
//    for (int32_t j = 0; j < pOneResult->len; ++j) {
//      (*pRes)[idx++] = pOneResult->pData[j];
//    }
//  }
//
//  return total;
//}
//
//static void removeDuplicateKey(SSkipListKey *pKey, int32_t *numOfKey, __compar_fn_t comparFn) {
//  if (*numOfKey == 1) {
//    return;
//  }
//
//  qsort(pKey, *numOfKey, sizeof(pKey[0]), comparFn);
//  int32_t i = 0, j = 1;
//
//  while (i < (*numOfKey) && j < (*numOfKey)) {
//    int32_t ret = comparFn(&pKey[i], &pKey[j]);
//    if (ret == 0) {
//      j++;
//    } else {
//      pKey[i + 1] = pKey[j];
//      i++;
//      j++;
//    }
//  }
//
//  (*numOfKey) = i + 1;
//}
//
//int32_t mergeResult(const SSkipListKey *pKey, int32_t numOfKey, tSkipListNode ***pRes, __compar_fn_t comparFn,
//                    tSkipListNode *pNode) {
//  int32_t i = 0, j = 0;
//  // merge two sorted arrays in O(n) time
//  while (i < numOfKey && pNode != NULL) {
//    int32_t ret = comparFn(&pNode->key, &pKey[i]);
//    if (ret < 0) {
//      (*pRes)[j++] = pNode;
//      pNode = pNode->pForward[0];
//    } else if (ret == 0) {
//      pNode = pNode->pForward[0];
//    } else {  // pNode->key > pkey[i]
//      i++;
//    }
//  }
//
//  while (pNode != NULL) {
//    (*pRes)[j++] = pNode;
//    pNode = pNode->pForward[0];
//  }
//  return j;
//}
//
//int32_t tSkipListPointQuery(SSkipList *pSkipList, SSkipListKey *pKey, int32_t numOfKey, tSkipListPointQueryType type,
//                            tSkipListNode ***pRes) {
//  if (numOfKey == 0 || pKey == NULL || pSkipList == NULL || pSkipList->nSize == 0 ||
//      (type != INCLUDE_POINT_QUERY && type != EXCLUDE_POINT_QUERY)) {
//    (*pRes) = NULL;
//    return 0;
//  }
//
//  __compar_fn_t comparFn = getKeyComparator(pKey->nType);
//  removeDuplicateKey(pKey, &numOfKey, comparFn);
//
//  if (type == INCLUDE_POINT_QUERY) {
//    if (numOfKey == 1) {
//      return tSkipListGets(pSkipList, &pKey[0], pRes);
//    } else {
//      MultipleQueryResult *pTempResult = (MultipleQueryResult *)malloc(sizeof(MultipleQueryResult) * numOfKey);
//      for (int32_t i = 0; i < numOfKey; ++i) {
//        pTempResult[i].len = tSkipListGets(pSkipList, &pKey[i], &pTempResult[i].pData);
//      }
//      int32_t num = mergeQueryResult(pTempResult, numOfKey, pRes);
//
//      for (int32_t i = 0; i < numOfKey; ++i) {
//        free(pTempResult[i].pData);
//      }
//      free(pTempResult);
//      return num;
//    }
//  } else {  // exclude query
//    *pRes = malloc(POINTER_BYTES * pSkipList->nSize);
//
//    __compar_fn_t filterComparator = getKeyFilterComparator(pSkipList, pKey->nType);
//
//    tSkipListNode *pNode = pSkipList->pHead.pForward[0];
//    int32_t        retLen = mergeResult(pKey, numOfKey, pRes, filterComparator, pNode);
//
//    if (retLen < pSkipList->nSize) {
//      (*pRes) = realloc(*pRes, POINTER_BYTES * retLen);
//    }
//    return retLen;
//  }
//}
//
