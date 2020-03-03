#ifndef TDENGINE_TSKIPLISTQUERY_H
#define TDENGINE_TSKIPLISTQUERY_H

#include "tskiplist.h"

typedef enum tSkipListPointQueryType {
  INCLUDE_POINT_QUERY,
  EXCLUDE_POINT_QUERY,
} tSkipListPointQueryType;

/*
 * query condition structure to denote the range query
 * todo merge the point query cond with range query condition
 */
typedef struct tSKipListQueryCond {
  // when the upper bounding == lower bounding, it is a point query
  SSkipListKey lowerBnd;
  SSkipListKey upperBnd;
  int32_t      lowerBndRelOptr;  // relation operator to denote if lower bound is
  int32_t      upperBndRelOptr;  // included or not
} tSKipListQueryCond;

// for debug purpose only
void tSkipListPrint(SSkipList *pSkipList, int16_t nlevel);

/*
 * range query & single point query function
 */
int32_t tSkipListQuery(SSkipList *pSkipList, tSKipListQueryCond *pQueryCond, SSkipListNode ***pResult);

/*
 * include/exclude point query
 */
int32_t tSkipListPointQuery(SSkipList *pSkipList, SSkipListKey *pKey, int32_t numOfKey, tSkipListPointQueryType type,
                            SSkipListNode ***pResult);

/*
 * get all data with the same keys
 */
int32_t tSkipListGets(SSkipList *pSkipList, SSkipListKey *pKey, SSkipListNode ***pRes);

#endif  // TDENGINE_TSKIPLISTQUERY_H
