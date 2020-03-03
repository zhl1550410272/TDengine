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

#ifndef TDENGINE_HASH_H
#define TDENGINE_HASH_H

#ifdef __cplusplus
extern "C" {
#endif

#include "hashutil.h"

#define HASH_MAX_CAPACITY (1024 * 1024 * 16)
#define HASH_VALUE_IN_TRASH (-1)
#define HASH_DEFAULT_LOAD_FACTOR (0.75)
#define HASH_INDEX(v, c) ((v) & ((c)-1))

typedef struct SHashNode {
  char *key;  // null-terminated string
  union {
    struct SHashNode * prev;
    struct SHashEntry *prev1;
  };

  struct SHashNode *next;
  uint32_t          hashVal;  // the hash value of key, if hashVal == HASH_VALUE_IN_TRASH, this node is moved to trash
  uint32_t          keyLen;   // length of the key
  char              data[];
} SHashNode;

typedef struct SHashEntry {
  SHashNode *next;
  uint32_t   num;
} SHashEntry;

typedef struct HashObj {
  SHashEntry **hashList;
  uint32_t     capacity;    // number of slots
  int32_t      size;        // number of elements in hash table
  _hash_fn_t   hashFp;      // hash function
  bool         threadsafe;  // enable lock or not

#if defined LINUX
  pthread_rwlock_t lock;
#else
  pthread_mutex_t lock;
#endif

} HashObj;

/**
 * @param capacity initial capacity available for hash elements
 * @param fn       hash function
 * @return
 */
void *taosHashInit(size_t capacity, _hash_fn_t fn, bool threadsafe);

/**
 *
 * @param pObj
 * @param key
 * @param keyLen
 */
void taosHashDelete(HashObj *pObj, const char *key, size_t keyLen);

/**
 *
 * @param pObj
 * @param key
 * @param keyLen
 * @param data
 * @param size
 * @return
 */
int32_t taosHashAdd(HashObj *pObj, const char *key, size_t keyLen, void *data, size_t size);

/**
 *
 * @param pObj
 * @return
 */
int32_t taosHashGetSize(HashObj *pObj);

/**
 *
 * @param pObj
 * @param key
 * @param keyLen
 * @return
 */
void *taosHashGet(HashObj *pObj, const char *key, size_t keyLen);

/**
 *
 * @param handle
 */
void taosHashCleanup(HashObj *pObj);

int32_t taosGetHashMaxOverflowLength(HashObj *pObj);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASH_H
