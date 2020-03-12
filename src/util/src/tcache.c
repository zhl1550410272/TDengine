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

#include "hash.h"
#include "hashutil.h"
#include "tcache.h"
#include "tlog.h"
#include "tref.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

#define HASH_MAX_CAPACITY (1024 * 1024 * 16)
#define HASH_VALUE_IN_TRASH (-1)
#define HASH_DEFAULT_LOAD_FACTOR (0.75)
#define HASH_INDEX(v, c) ((v) & ((c)-1))

/**
 * todo: refactor to extract the hash table out of cache structure
 */
typedef struct SCacheStatis {
  int64_t missCount;
  int64_t hitCount;
  int64_t totalAccess;
  int64_t refreshCount;
  int32_t numOfCollision;
} SCacheStatis;

typedef struct SDataNode {
  uint64_t addedTime;    // the added time when this element is added or updated into cache
  uint64_t expiredTime;  // expiredTime expiredTime when this element should be remove from cache
  uint64_t signature;
  uint32_t size;  // allocated size for current SDataNode
  uint16_t keySize:15;
  bool     inTrash:1;    // denote if it is in trash or not
  
  T_REF_DECLARE()
  char *key;
  
  char  data[];
} SDataNode;

typedef struct STrashElem {
  struct STrashElem *prev;
  struct STrashElem *next;
  SDataNode *        pData;
} STrashElem;

typedef struct {
  int64_t totalSize;  // total allocated buffer in this hash table, SCacheObj is not included.
  int64_t refreshTime;

  /*
   * to accommodate the old datanode which has the same key value of new one in hashList
   * when an new node is put into cache, if an existed one with the same key:
   * 1. if the old one does not be referenced, update it.
   * 2. otherwise, move the old one to pTrash, addedTime the new one.
   *
   * when the node in pTrash does not be referenced, it will be release at the expired expiredTime
   */
  STrashElem * pTrash;
  void *       tmrCtrl;
  void *       pTimer;
  SCacheStatis statistics;
  SHashObj *   pHashTable;
  int          numOfElemsInTrash;  // number of element in trash
  int16_t      deleting;           // set the deleting flag to stop refreshing ASAP.

#if defined(LINUX)
  pthread_rwlock_t lock;
#else
  pthread_mutex_t lock;
#endif

} SCacheObj;

static FORCE_INLINE void __cache_wr_lock(SCacheObj *pObj) {
#if defined(LINUX)
  pthread_rwlock_wrlock(&pObj->lock);
#else
  pthread_mutex_lock(&pObj->lock);
#endif
}

static FORCE_INLINE void __cache_rd_lock(SCacheObj *pObj) {
#if defined(LINUX)
  pthread_rwlock_rdlock(&pObj->lock);
#else
  pthread_mutex_lock(&pObj->lock);
#endif
}

static FORCE_INLINE void __cache_unlock(SCacheObj *pObj) {
#if defined(LINUX)
  pthread_rwlock_unlock(&pObj->lock);
#else
  pthread_mutex_unlock(&pObj->lock);
#endif
}

static FORCE_INLINE int32_t __cache_lock_init(SCacheObj *pObj) {
#if defined(LINUX)
  return pthread_rwlock_init(&pObj->lock, NULL);
#else
  return pthread_mutex_init(&pObj->lock, NULL);
#endif
}

static FORCE_INLINE void __cache_lock_destroy(SCacheObj *pObj) {
#if defined(LINUX)
  pthread_rwlock_destroy(&pObj->lock);
#else
  pthread_mutex_destroy(&pObj->lock);
#endif
}

static void taosFreeNode(void* data) {
  SDataNode* pNode = *(SDataNode**) data;
  free(pNode);
}


/**
 * @param key      key of object for hash, usually a null-terminated string
 * @param keyLen   length of key
 * @param pData    actually data. required a consecutive memory block, no pointer is allowed
 *                 in pData. Pointer copy causes memory access error.
 * @param size     size of block
 * @param lifespan total survial expiredTime from now
 * @return         SDataNode
 */
static SDataNode *taosCreateHashNode(const char* key, size_t keyLen, const char *pData, size_t size, uint64_t duration) {
  size_t totalSize = size + sizeof(SDataNode) + keyLen;

  SDataNode *pNewNode = calloc(1, totalSize);
  if (pNewNode == NULL) {
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  memcpy(pNewNode->data, pData, size);
  
  pNewNode->key = (char*)pNewNode + sizeof(SDataNode) + size;
  pNewNode->keySize = keyLen;
  
  memcpy(pNewNode->key, key, keyLen);

  pNewNode->addedTime = (uint64_t)taosGetTimestampMs();
  pNewNode->expiredTime = pNewNode->addedTime + duration;

  pNewNode->signature = (uint64_t)pNewNode;
  pNewNode->size = (uint32_t)totalSize;

  return pNewNode;
}

/**
 * addedTime object node into trash, and this object is closed for referencing if it is addedTime to trash
 * It will be removed until the pNode->refCount == 0
 * @param pObj    Cache object
 * @param pNode   Cache slot object
 */
static void taosAddToTrash(SCacheObj *pObj, SDataNode *pNode) {
  if (pNode->inTrash) { /* node is already in trash */
    return;
  }

  STrashElem *pElem = calloc(1, sizeof(STrashElem));
  pElem->pData = pNode;

  pElem->next = pObj->pTrash;
  if (pObj->pTrash) {
    pObj->pTrash->prev = pElem;
  }

  pElem->prev = NULL;
  pObj->pTrash = pElem;

  pNode->inTrash = true;
  pObj->numOfElemsInTrash++;

  pTrace("key:%s %p move to trash, numOfElem in trash:%d", pNode->key, pNode, pObj->numOfElemsInTrash);
}

static void taosRemoveFromTrash(SCacheObj *pObj, STrashElem *pElem) {
  if (pElem->pData->signature != (uint64_t)pElem->pData) {
    pError("key:sig:%d %p data has been released, ignore", pElem->pData->signature, pElem->pData);
    return;
  }

  pObj->numOfElemsInTrash--;
  if (pElem->prev) {
    pElem->prev->next = pElem->next;
  } else { /* pnode is the header, update header */
    pObj->pTrash = pElem->next;
  }

  if (pElem->next) {
    pElem->next->prev = pElem->prev;
  }

  pElem->pData->signature = 0;
  free(pElem->pData);
  free(pElem);
}
/**
 * remove nodes in trash with refCount == 0 in cache
 * @param pNode
 * @param pObj
 * @param force   force model, if true, remove data in trash without check refcount.
 *                may cause corruption. So, forece model only applys before cache is closed
 */
static void taosClearCacheTrash(SCacheObj *pObj, bool force) {
  __cache_wr_lock(pObj);

  if (pObj->numOfElemsInTrash == 0) {
    if (pObj->pTrash != NULL) {
      pError("key:inconsistency data in cache, numOfElem in trash:%d", pObj->numOfElemsInTrash);
    }
    pObj->pTrash = NULL;

    __cache_unlock(pObj);
    return;
  }

  STrashElem *pElem = pObj->pTrash;

  while (pElem) {
    T_REF_VAL_CHECK(pElem->pData);
    if (pElem->next == pElem) {
      pElem->next = NULL;
    }

    if (force || (T_REF_VAL_GET(pElem->pData) == 0)) {
      pTrace("key:%s %p removed from trash. numOfElem in trash:%d", pElem->pData->key, pElem->pData,
             pObj->numOfElemsInTrash - 1);
      STrashElem* p = pElem;
      
      pElem = pElem->next;
      taosRemoveFromTrash(pObj, p);
    } else {
      pElem = pElem->next;
    }
  }

  assert(pObj->numOfElemsInTrash >= 0);
  __cache_unlock(pObj);
}

/**
 * release node
 * @param pObj      cache object
 * @param pNode     data node
 */
static FORCE_INLINE void taosCacheReleaseNode(SCacheObj *pObj, SDataNode *pNode) {
  if (pNode->signature != (uint64_t)pNode) {
    pError("key:%s, %p data is invalid, or has been released", pNode->key, pNode);
    return;
  }

  taosHashRemove(pObj->pHashTable, pNode->key, pNode->keySize);
  pTrace("key:%s is removed from cache,total:%d,size:%ldbytes", pNode->key, pObj->totalSize, pObj->totalSize);
  
  free(pNode);
}

/**
 * move the old node into trash
 * @param pObj
 * @param pNode
 */
static FORCE_INLINE void taosCacheMoveNodeToTrash(SCacheObj *pObj, SDataNode *pNode) {
  taosHashRemove(pObj->pHashTable, pNode->key, pNode->keySize);
  taosAddToTrash(pObj, pNode);
}

/**
 * update data in cache
 * @param pObj
 * @param pNode
 * @param key
 * @param keyLen
 * @param pData
 * @param dataSize
 * @return
 */
static SDataNode *taosUpdateCacheImpl(SCacheObj *pObj, SDataNode *pNode, char *key, int32_t keyLen, void *pData,
                                      uint32_t dataSize, uint64_t duration) {
  SDataNode *pNewNode = NULL;

  // only a node is not referenced by any other object, in-place update it
  if (T_REF_VAL_GET(pNode) == 0) {
    size_t newSize = sizeof(SDataNode) + dataSize + keyLen;

    pNewNode = (SDataNode *)realloc(pNode, newSize);
    if (pNewNode == NULL) {
      return NULL;
    }

    pNewNode->signature = (uint64_t)pNewNode;
    memcpy(pNewNode->data, pData, dataSize);
    
    pNewNode->key = (char*)pNewNode + sizeof(SDataNode) + dataSize;
    pNewNode->keySize = keyLen;
    memcpy(pNewNode->key, key, keyLen);

    // update the timestamp information for updated key/value
    pNewNode->addedTime = taosGetTimestampMs();
    pNewNode->expiredTime = pNewNode->addedTime + duration;

    T_REF_INC(pNewNode);

    // the address of this node may be changed, so the prev and next element should update the corresponding pointer
    taosHashPut(pObj->pHashTable, key, keyLen, &pNewNode, sizeof(void *));
  } else {
    taosCacheMoveNodeToTrash(pObj, pNode);

    pNewNode = taosCreateHashNode(key, keyLen, pData, dataSize, duration);
    if (pNewNode == NULL) {
      return NULL;
    }

    T_REF_INC(pNewNode);

    // addedTime new element to hashtable
    taosHashPut(pObj->pHashTable, key, keyLen, &pNewNode, sizeof(void *));
  }

  return pNewNode;
}

/**
 * addedTime data into hash table
 * @param key
 * @param pData
 * @param size
 * @param pObj
 * @param keyLen
 * @param pNode
 * @return
 */
static FORCE_INLINE SDataNode *taosAddToCacheImpl(SCacheObj *pObj, char *key, size_t keyLen, const void *pData,
                                                  size_t dataSize, uint64_t duration) {
  SDataNode *pNode = taosCreateHashNode(key, keyLen, pData, dataSize, duration);
  if (pNode == NULL) {
    return NULL;
  }

  T_REF_INC(pNode);
  taosHashPut(pObj->pHashTable, key, keyLen, &pNode, sizeof(void *));
  return pNode;
}

/**
 * addedTime data into cache
 *
 * @param handle        cache object
 * @param key           key
 * @param pData         cached data
 * @param dataSize      data size
 * @param duration      survival expiredTime in second
 * @return              cached element
 */
void *taosAddDataIntoCache(void *handle, char *key, char *pData, int dataSize, int duration) {
  SDataNode *pNode;
  SCacheObj *pObj;

  pObj = (SCacheObj *)handle;
  if (pObj == NULL || pObj->pHashTable == NULL) {
    return NULL;
  }

  size_t keyLen = strlen(key);

  __cache_wr_lock(pObj);
  SDataNode **pt = (SDataNode**) taosHashGet(pObj->pHashTable, key, keyLen);
  SDataNode* pOld = (pt != NULL)? (*pt):NULL;
  
  if (pOld == NULL) {  // do addedTime to cache
    pNode = taosAddToCacheImpl(pObj, key, keyLen, pData, dataSize, duration * 1000L);
    if (NULL != pNode) {
      pTrace("key:%s %p added into cache, addedTime:%" PRIu64 ", expireTime:%" PRIu64 ", cache total:%d, size:%" PRId64
             " bytes, collision:%d",
             key, pNode, pNode->addedTime, pNode->expiredTime, dataSize, pObj->totalSize,
             pObj->statistics.numOfCollision);
    }
  } else {  // old data exists, update the node
    pNode = taosUpdateCacheImpl(pObj, pOld, key, keyLen, pData, dataSize, duration * 1000L);
    pTrace("key:%s %p exist in cache, updated", key, pNode);
  }

  __cache_unlock(pObj);

  return (pNode != NULL) ? pNode->data : NULL;
}

/**
 * remove data in cache, the data will not be removed immediately.
 * if it is referenced by other object, it will be remain in cache
 * @param handle
 * @param data
 */
void taosRemoveDataFromCache(void *handle, void **data, bool _remove) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL || (*data) == NULL || (taosHashGetSize(pObj->pHashTable) + pObj->numOfElemsInTrash == 0)) {
    return;
  }

  size_t     offset = offsetof(SDataNode, data);
  SDataNode *pNode = (SDataNode *)((char *)(*data) - offset);

  if (pNode->signature != (uint64_t)pNode) {
    pError("key: %p release invalid cache data", pNode);
    return;
  }

  *data = NULL;

  if (_remove) {
    __cache_wr_lock(pObj);
    // pNode may be released immediately by other thread after the reference count of pNode is set to 0,
    // So we need to lock it in the first place.
    T_REF_DEC(pNode);
    taosCacheMoveNodeToTrash(pObj, pNode);

    __cache_unlock(pObj);
  } else {
    T_REF_DEC(pNode);
  }
}

/**
 * get data from cache
 * @param handle        cache object
 * @param key           key
 * @return              cached data or NULL
 */
void *taosGetDataFromCache(void *handle, char *key) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL || taosHashGetSize(pObj->pHashTable) == 0) {
    return NULL;
  }

  uint32_t keyLen = (uint32_t)strlen(key);

  __cache_rd_lock(pObj);

  SDataNode **ptNode = (SDataNode **)taosHashGet(pObj->pHashTable, key, keyLen);
  if (ptNode != NULL) {
    T_REF_INC(*ptNode);
  }
  
  __cache_unlock(pObj);

  if (ptNode != NULL) {
    atomic_add_fetch_32(&pObj->statistics.hitCount, 1);
    pTrace("key:%s is retrieved from cache,refcnt:%d", key, T_REF_VAL_GET(*ptNode));
  } else {
    atomic_add_fetch_32(&pObj->statistics.missCount, 1);
    pTrace("key:%s not in cache,retrieved failed", key);
  }

  atomic_add_fetch_32(&pObj->statistics.totalAccess, 1);
  return ((*ptNode) != NULL) ? (*ptNode)->data : NULL;
}

static void doCleanupDataCache(SCacheObj *pObj) {
  __cache_wr_lock(pObj);

  if (taosHashGetSize(pObj->pHashTable) > 0) {
    taosHashCleanup(pObj->pHashTable);
  }

  __cache_unlock(pObj);

  taosClearCacheTrash(pObj, true);
  __cache_lock_destroy(pObj);

  memset(pObj, 0, sizeof(SCacheObj));
  free(pObj);
}

/**
 * refresh cache to remove data in both hash list and trash, if any nodes' refcount == 0, every pObj->refreshTime
 * @param handle   Cache object handle
 */
void taosRefreshDataCache(void *handle, void *tmrId) {
  SCacheObj *pObj = (SCacheObj *)handle;

  if (pObj == NULL || taosHashGetSize(pObj->pHashTable) == 0) {
    pTrace("object is destroyed. no refresh retry");
    return;
  }

  if (pObj->deleting == 1) {
    doCleanupDataCache(pObj);
    return;
  }

  uint64_t expiredTime = taosGetTimestampMs();
  pObj->statistics.refreshCount++;
  
  SHashMutableIterator* pIter = taosHashCreateIter(pObj->pHashTable);
  
  __cache_wr_lock(pObj);
  while(taosHashIterNext(pIter)) {
    
    if (pObj->deleting == 1) {
      taosHashDestroyIter(pIter);
      break;
    }
    
    SDataNode* pNode = *(SDataNode**) taosHashIterGet(pIter);
    if (pNode->expiredTime <= expiredTime && T_REF_VAL_GET(pNode) <= 0) {
      taosCacheReleaseNode(pObj, pNode);
    }
  }
  
  __cache_unlock(pObj);
  
  taosHashDestroyIter(pIter);

  if (pObj->deleting == 1) {  // clean up resources and abort
    doCleanupDataCache(pObj);
  } else {
    taosClearCacheTrash(pObj, false);
    taosTmrReset(taosRefreshDataCache, pObj->refreshTime, pObj, pObj->tmrCtrl, &pObj->pTimer);
  }
}

void taosClearDataCache(void *handle) {
  SCacheObj *pObj = (SCacheObj *)handle;
  
  SHashMutableIterator* pIter = taosHashCreateIter(pObj->pHashTable);
  
  __cache_wr_lock(pObj);
  while(taosHashIterNext(pIter)) {
    
    if (pObj->deleting == 1) {
      taosHashDestroyIter(pIter);
      break;
    }
    
    SDataNode* pNode = *(SDataNode**) taosHashIterGet(pIter);
    taosCacheMoveNodeToTrash(pObj, pNode);
  }
  __cache_unlock(pObj);
  
  taosHashDestroyIter(pIter);
  taosClearCacheTrash(pObj, false);
}

/**
 * @param capacity          maximum slots available for hash elements
 * @param tmrCtrl           timer ctrl
 * @param refreshTime       refresh operation interval expiredTime, the maximum survival expiredTime when one element is
 * expired and not referenced by other objects
 * @return
 */
void *taosInitDataCache(void *tmrCtrl, int64_t refreshTime) {
  if (tmrCtrl == NULL || refreshTime <= 0) {
    return NULL;
  }

  SCacheObj *pObj = (SCacheObj *)calloc(1, sizeof(SCacheObj));
  if (pObj == NULL) {
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  pObj->pHashTable = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false);
  if (pObj->pHashTable == NULL) {
    free(pObj);
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }
  
  // set free cache node callback function for hash table
  taosHashSetFreecb(pObj->pHashTable, taosFreeNode);

  pObj->refreshTime = refreshTime * 1000;
  pObj->tmrCtrl = tmrCtrl;

  taosTmrReset(taosRefreshDataCache, pObj->refreshTime, pObj, pObj->tmrCtrl, &pObj->pTimer);

  if (__cache_lock_init(pObj) != 0) {
    taosTmrStopA(&pObj->pTimer);
    taosHashCleanup(pObj->pHashTable);
    free(pObj);

    pError("failed to init lock, reason:%s", strerror(errno));
    return NULL;
  }

  return (void *)pObj;
}

/**
 * release all allocated memory and destroy the cache object.
 *
 * This function only set the deleting flag, and the specific work of clean up cache is delegated to
 * taosRefreshDataCache function, which will executed every SCacheObj->refreshTime sec.
 *
 * If the value of SCacheObj->refreshTime is too large, the taosRefreshDataCache function may not be invoked
 * before the main thread terminated, in which case all allocated resources are simply recycled by OS.
 *
 * @param handle
 */
void taosCleanupDataCache(void *handle) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL) {
    return;
  }

  pObj->deleting = 1;
}

void *taosGetDataFromExists(void *handle, void *data) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL || data == NULL) return NULL;

  size_t     offset = offsetof(SDataNode, data);
  SDataNode *ptNode = (SDataNode *)((char *)data - offset);

  if (ptNode->signature != (uint64_t)ptNode) {
    pError("key: %p the data from cache is invalid", ptNode);
    return NULL;
  }

  int32_t ref = T_REF_INC(ptNode);
  pTrace("%p addedTime ref data in cache, refCnt:%d", data, ref)

      // the data if referenced by at least one object, so the reference count must be greater than the value of 2.
      assert(ref >= 2);
  return data;
}

void *taosTransferDataInCache(void *handle, void **data) {
  SCacheObj *pObj = (SCacheObj *)handle;
  if (pObj == NULL || data == NULL) return NULL;

  size_t     offset = offsetof(SDataNode, data);
  SDataNode *ptNode = (SDataNode *)((char *)(*data) - offset);

  if (ptNode->signature != (uint64_t)ptNode) {
    pError("key: %p the data from cache is invalid", ptNode);
    return NULL;
  }

  assert(T_REF_VAL_GET(ptNode) >= 1);

  char *d = *data;

  // clear its reference to old area
  *data = NULL;

  return d;
}
