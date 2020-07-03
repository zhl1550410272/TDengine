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
#include "os.h"
#include "taosmsg.h"
#include "tsocket.h"
#include "tutil.h"
#include "ttime.h"
#include "ttimer.h"
#include "tglobal.h"
#include "httpInt.h"
#include "httpContext.h"
#include "httpResp.h"
#include "httpUtil.h"

#ifndef EPOLLWAKEUP
 #define EPOLLWAKEUP (1u << 29)
#endif

static void httpStopThread(HttpThread* pThread) {
  pThread->stop = true;

  // signal the thread to stop, try graceful method first,
  // and use pthread_cancel when failed
  struct epoll_event event = { .events = EPOLLIN };
  eventfd_t fd = eventfd(1, 0);
  if (fd == -1) {
    httpError("%s, failed to create eventfd, will call pthread_cancel instead, which may result in data corruption: %s", pThread->label, strerror(errno));
    pthread_cancel(pThread->thread);
  } else if (epoll_ctl(pThread->pollFd, EPOLL_CTL_ADD, fd, &event) < 0) {
    httpError("%s, failed to call epoll_ctl, will call pthread_cancel instead, which may result in data corruption: %s", pThread->label, strerror(errno));
    pthread_cancel(pThread->thread);
  }

  pthread_join(pThread->thread, NULL);
  if (fd != -1) {
    close(fd);
  }

  close(pThread->pollFd);
  pthread_mutex_destroy(&(pThread->threadMutex));
}

void httpCleanUpConnect() {
  HttpServer *pServer = &tsHttpServer;
  if (pServer->pThreads == NULL) return;

  pthread_join(pServer->thread, NULL);
  for (int i = 0; i < pServer->numOfThreads; ++i) {
    HttpThread* pThread = pServer->pThreads + i;
    if (pThread != NULL) {
      httpStopThread(pThread);
    }
  }

  httpDebug("http server:%s is cleaned up", pServer->label);
}

bool httpReadDataImp(HttpContext *pContext) {
  HttpParser *pParser = &pContext->parser;

  while (pParser->bufsize <= (HTTP_BUFFER_SIZE - HTTP_STEP_SIZE)) {
    int nread = (int)taosReadSocket(pContext->fd, pParser->buffer + pParser->bufsize, HTTP_STEP_SIZE);
    if (nread >= 0 && nread < HTTP_STEP_SIZE) {
      pParser->bufsize += nread;
      break;
    } else if (nread < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        httpDebug("context:%p, fd:%d, ip:%s, read from socket error:%d, wait another event",
                  pContext, pContext->fd, pContext->ipstr, errno);
        break;
      } else {
        httpError("context:%p, fd:%d, ip:%s, read from socket error:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, errno);
        return false;
      }
    } else {
      pParser->bufsize += nread;
    }

    if (pParser->bufsize >= (HTTP_BUFFER_SIZE - HTTP_STEP_SIZE)) {
      httpError("context:%p, fd:%d, ip:%s, thread:%s, request big than:%d",
                pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, HTTP_BUFFER_SIZE);
      httpSendErrorResp(pContext, HTTP_REQUSET_TOO_BIG);
      httpNotifyContextClose(pContext);
      return false;
    }
  }

  pParser->buffer[pParser->bufsize] = 0;

  return true;
}

static bool httpDecompressData(HttpContext *pContext) {
  if (pContext->contentEncoding != HTTP_COMPRESS_GZIP) {
    httpTraceDump("context:%p, fd:%d, ip:%s, content:%s", pContext, pContext->fd, pContext->ipstr, pContext->parser.data.pos);
    return true;
  }

  char   *decompressBuf = calloc(HTTP_DECOMPRESS_BUF_SIZE, 1);
  int32_t decompressBufLen = HTTP_DECOMPRESS_BUF_SIZE;
  size_t  bufsize = sizeof(pContext->parser.buffer) - (pContext->parser.data.pos - pContext->parser.buffer) - 1;
  if (decompressBufLen > (int)bufsize) {
    decompressBufLen = (int)bufsize;
  }

  int ret = httpGzipDeCompress(pContext->parser.data.pos, pContext->parser.data.len, decompressBuf, &decompressBufLen);

  if (ret == 0) {
    memcpy(pContext->parser.data.pos, decompressBuf, decompressBufLen);
    pContext->parser.data.pos[decompressBufLen] = 0;
    httpTraceDump("context:%p, fd:%d, ip:%s, rawSize:%d, decompressSize:%d, content:%s",
              pContext, pContext->fd, pContext->ipstr, pContext->parser.data.len, decompressBufLen,  decompressBuf);
    pContext->parser.data.len = decompressBufLen;
  } else {
    httpError("context:%p, fd:%d, ip:%s, failed to decompress data, rawSize:%d, error:%d",
              pContext, pContext->fd, pContext->ipstr, pContext->parser.data.len, ret);
  }

  free(decompressBuf);
  return ret == 0;
}

static bool httpReadData(HttpContext *pContext) {
  if (!pContext->parsed) {
    httpInitContext(pContext);
  }

  if (!httpReadDataImp(pContext)) {
    httpNotifyContextClose(pContext);
    return false;
  }

  if (!httpParseRequest(pContext)) {
    httpNotifyContextClose(pContext);
    return false;
  }

  int ret = httpCheckReadCompleted(pContext);
  if (ret == HTTP_CHECK_BODY_CONTINUE) {
    //httpDebug("context:%p, fd:%d, ip:%s, not finished yet, wait another event", pContext, pContext->fd, pContext->ipstr);
    return false;
  } else if (ret == HTTP_CHECK_BODY_SUCCESS){
    httpDebug("context:%p, fd:%d, ip:%s, thread:%s, read size:%d, dataLen:%d",
              pContext, pContext->fd, pContext->ipstr, pContext->pThread->label, pContext->parser.bufsize, pContext->parser.data.len);
    if (httpDecompressData(pContext)) {
      return true;
    } else {
      httpNotifyContextClose(pContext);
      return false;
    }
  } else {
    httpError("context:%p, fd:%d, ip:%s, failed to read http body, close connect", pContext, pContext->fd, pContext->ipstr);
    httpNotifyContextClose(pContext);
    return false;
  }
}

static void httpProcessHttpData(void *param) {
  HttpServer  *pServer = &tsHttpServer;
  HttpThread  *pThread = (HttpThread *)param;
  HttpContext *pContext;
  int          fdNum;

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  pthread_sigmask(SIG_SETMASK, &set, NULL);

  while (1) {
    struct epoll_event events[HTTP_MAX_EVENTS];
    //-1 means uncertainty, 0-nowait, 1-wait 1 ms, set it from -1 to 1
    fdNum = epoll_wait(pThread->pollFd, events, HTTP_MAX_EVENTS, 1);
    if (pThread->stop) {
      httpDebug("%p, http thread get stop event, exiting...", pThread);
      break;
    }
    if (fdNum <= 0) continue;

    for (int i = 0; i < fdNum; ++i) {
      pContext = httpGetContext(events[i].data.ptr);
      if (pContext == NULL) {
        httpError("context:%p, is already released, close connect", events[i].data.ptr);
        //epoll_ctl(pThread->pollFd, EPOLL_CTL_DEL, events[i].data.fd, NULL);
        //tclose(events[i].data.fd);
        continue;
      }

      if (events[i].events & EPOLLPRI) {
        httpDebug("context:%p, fd:%d, ip:%s, state:%s, EPOLLPRI events occured, accessed:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpCloseContextByServer(pContext);
        continue;
      }

      if (events[i].events & EPOLLRDHUP) {
        httpDebug("context:%p, fd:%d, ip:%s, state:%s, EPOLLRDHUP events occured, accessed:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpCloseContextByServer(pContext);
        continue;
      }

      if (events[i].events & EPOLLERR) {
        httpDebug("context:%p, fd:%d, ip:%s, state:%s, EPOLLERR events occured, accessed:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpCloseContextByServer(pContext);
        continue;
      }

      if (events[i].events & EPOLLHUP) {
        httpDebug("context:%p, fd:%d, ip:%s, state:%s, EPOLLHUP events occured, accessed:%d, close connect",
                  pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpCloseContextByServer(pContext);
        continue;
      }

      if (!httpAlterContextState(pContext, HTTP_CONTEXT_STATE_READY, HTTP_CONTEXT_STATE_READY)) {
        httpDebug("context:%p, fd:%d, ip:%s, state:%s, not in ready state, ignore read events",
                pContext, pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state));
        httpReleaseContext(pContext);
        continue;
      }

      if (pServer->status != HTTP_SERVER_RUNNING) {
        httpDebug("context:%p, fd:%d, ip:%s, state:%s, server is not running, accessed:%d, close connect", pContext,
                  pContext->fd, pContext->ipstr, httpContextStateStr(pContext->state), pContext->accessTimes);
        httpSendErrorResp(pContext, HTTP_SERVER_OFFLINE);
        httpNotifyContextClose(pContext);
      } else {
        if (httpReadData(pContext)) {
          (*(pThread->processData))(pContext);
          atomic_fetch_add_32(&pServer->requestNum, 1);
        }
      }
    }
  }
}

static void *httpAcceptHttpConnection(void *arg) {
  int                connFd = -1;
  struct sockaddr_in clientAddr;
  int                threadId = 0;
  HttpServer *       pServer = &tsHttpServer;
  HttpThread *       pThread = NULL;
  HttpContext *      pContext = NULL;
  int                totalFds = 0;

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  pthread_sigmask(SIG_SETMASK, &set, NULL);

  pServer->fd = taosOpenTcpServerSocket(pServer->serverIp, pServer->serverPort);

  if (pServer->fd < 0) {
    httpError("http server:%s, failed to open http socket, ip:%s:%u error:%s", pServer->label,
              taosIpStr(pServer->serverIp), pServer->serverPort, strerror(errno));
    return NULL;
  } else {
    httpInfo("http server init success at %u", pServer->serverPort);
    pServer->status = HTTP_SERVER_RUNNING;
  }

  while (1) {
    socklen_t addrlen = sizeof(clientAddr);
    connFd = (int)accept(pServer->fd, (struct sockaddr *)&clientAddr, &addrlen);
    if (connFd == -1) {
      if (errno == EINVAL) {
        httpDebug("http server:%s socket was shutdown, exiting...", pServer->label);
        break;
      }
      httpError("http server:%s, accept connect failure, errno:%d reason:%s", pServer->label, errno, strerror(errno));
      continue;
    }

    totalFds = 1;
    for (int i = 0; i < pServer->numOfThreads; ++i) {
      totalFds += pServer->pThreads[i].numOfFds;
    }

    if (totalFds > tsHttpCacheSessions * 100) {
      httpError("fd:%d, ip:%s:%u, totalFds:%d larger than httpCacheSessions:%d*100, refuse connection", connFd,
                inet_ntoa(clientAddr.sin_addr), htons(clientAddr.sin_port), totalFds, tsHttpCacheSessions);
      taosCloseSocket(connFd);
      continue;
    }

    taosKeepTcpAlive(connFd);
    taosSetNonblocking(connFd, 1);

    // pick up the thread to handle this connection
    pThread = pServer->pThreads + threadId;

    pContext = httpCreateContext(connFd);
    if (pContext == NULL) {
      httpError("fd:%d, ip:%s:%u, no enough resource to allocate http context", connFd, inet_ntoa(clientAddr.sin_addr),
                htons(clientAddr.sin_port));
      taosCloseSocket(connFd);
      continue;
    }

    pContext->pThread = pThread;
    sprintf(pContext->ipstr, "%s:%u", inet_ntoa(clientAddr.sin_addr), htons(clientAddr.sin_port));
    
    struct epoll_event event;
    event.events = EPOLLIN | EPOLLPRI | EPOLLWAKEUP | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    event.data.ptr = pContext;
    if (epoll_ctl(pThread->pollFd, EPOLL_CTL_ADD, connFd, &event) < 0) {
      httpError("context:%p, fd:%d, ip:%s, thread:%s, failed to add http fd for epoll, error:%s", pContext, connFd,
                pContext->ipstr, pThread->label, strerror(errno));
      tclose(pContext->fd);
      httpReleaseContext(pContext);
      continue;
    }

    // notify the data process, add into the FdObj list
    atomic_add_fetch_32(&pThread->numOfFds, 1);
    httpDebug("context:%p, fd:%d, ip:%s, thread:%s numOfFds:%d totalFds:%d, accept a new connection", pContext, connFd,
              pContext->ipstr, pThread->label, pThread->numOfFds, totalFds);

    // pick up next thread for next connection
    threadId++;
    threadId = threadId % pServer->numOfThreads;
  }

  close(pServer->fd);
  return NULL;
}

bool httpInitConnect() {
  HttpServer *pServer = &tsHttpServer;
  pServer->pThreads = calloc(pServer->numOfThreads, sizeof(HttpThread));
  if (pServer->pThreads == NULL) {
    httpError("init error no enough memory");
    return false;
  }

  HttpThread *pThread = pServer->pThreads;
  for (int i = 0; i < pServer->numOfThreads; ++i) {
    sprintf(pThread->label, "%s%d", pServer->label, i);
    pThread->processData = pServer->processData;
    pThread->threadId = i;

    if (pthread_mutex_init(&(pThread->threadMutex), NULL) < 0) {
      httpError("http thread:%s, failed to init HTTP process data mutex, reason:%s", pThread->label, strerror(errno));
      return false;
    }

    pThread->pollFd = epoll_create(HTTP_MAX_EVENTS);  // size does not matter
    if (pThread->pollFd < 0) {
      httpError("http thread:%s, failed to create HTTP epoll", pThread->label);
      pthread_mutex_destroy(&(pThread->threadMutex));
      return false;
    }

    pthread_attr_t thattr;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&(pThread->thread), &thattr, (void *)httpProcessHttpData, (void *)(pThread)) != 0) {
      httpError("http thread:%s, failed to create HTTP process data thread, reason:%s", pThread->label,
                strerror(errno));
      pthread_mutex_destroy(&(pThread->threadMutex));        
      return false;
    }
    pthread_attr_destroy(&thattr);

    httpDebug("http thread:%p:%s, initialized", pThread, pThread->label);
    pThread++;
  }

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&(pServer->thread), &thattr, (void *)httpAcceptHttpConnection, (void *)(pServer)) != 0) {
    httpError("http server:%s, failed to create Http accept thread, reason:%s", pServer->label, strerror(errno));
    httpCleanUpConnect();
    return false;
  }
  pthread_attr_destroy(&thattr);

  httpDebug("http server:%s, initialized, ip:%s:%u, numOfThreads:%d", pServer->label, taosIpStr(pServer->serverIp),
            pServer->serverPort, pServer->numOfThreads);
  return true;
}
