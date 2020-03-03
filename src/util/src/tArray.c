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

#include "tArray.h"

void* taosArrayInit(size_t size, size_t elemSize) {
  assert(elemSize > 0);

  if (size < TARRAY_MIN_SIZE) {
    size = TARRAY_MIN_SIZE;
  }

  tArray* pArray = calloc(1, sizeof(tArray));
  if (pArray == NULL) {
    return NULL;
  }

  pArray->pData = calloc(size, elemSize * size);
  if (pArray->pData == NULL) {
    free(pArray);
    return NULL;
  }

  pArray->capacity = size;
  pArray->elemSize = elemSize;
  return pArray;
}

static void taosArrayResize(tArray* pArray) {
  assert(pArray->size >= pArray->capacity);

  size_t size = pArray->capacity;
  size = (size << 1u);

  void* tmp = realloc(pArray->pData, size * pArray->elemSize);
  if (tmp == NULL) {
    // todo
  }

  pArray->pData = tmp;
  pArray->capacity = size;
}

void* taosArrayPush(tArray* pArray, void* pData) {
  if (pArray == NULL || pData == NULL) {
    return NULL;
  }

  if (pArray->size >= pArray->capacity) {
    taosArrayResize(pArray);
  }

  void* dst = TARRAY_GET_ELEM(pArray, pArray->size);
  memcpy(dst, pData, pArray->elemSize);

  pArray->size += 1;
  return dst;
}

void taosArrayPop(tArray* pArray) {
  if (pArray == NULL || pArray->size == 0) {
    return;
  }

  pArray->size -= 1;
}

void* taosArrayGet(tArray* pArray, size_t index) {
  assert(index < pArray->size);
  return TARRAY_GET_ELEM(pArray, index);
}

size_t taosArrayGetSize(tArray* pArray) { return pArray->size; }

void taosArrayInsert(tArray* pArray, int32_t index, void* pData) {
  if (pArray == NULL || pData == NULL) {
    return;
  }

  if (index >= pArray->size) {
    taosArrayPush(pArray, pData);
    return;
  }

  if (pArray->size >= pArray->capacity) {
    taosArrayResize(pArray);
  }

  void* dst = TARRAY_GET_ELEM(pArray, index);

  int32_t remain = pArray->size - index;
  memmove(dst + pArray->elemSize, dst, pArray->elemSize * remain);
  memcpy(dst, pData, pArray->elemSize);

  pArray->size += 1;
}

void taosArrayDestory(tArray* pArray) {
  if (pArray == NULL) {
    return;
  }

  free(pArray->pData);
  free(pArray);
}
