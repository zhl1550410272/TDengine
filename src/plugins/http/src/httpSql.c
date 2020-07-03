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
#include "tnote.h"
#include "taos.h"
#include "tsclient.h"
#include "httpInt.h"
#include "httpContext.h"
#include "httpSql.h"
#include "httpResp.h"
#include "httpAuth.h"
#include "httpSession.h"

void *taos_connect_a(char *ip, char *user, char *pass, char *db, uint16_t port, void (*fp)(void *, TAOS_RES *, int),
                     void *param, void **taos);
void httpProcessMultiSql(HttpContext *pContext);

void httpProcessMultiSqlRetrieveCallBack(void *param, TAOS_RES *result, int numOfRows) {
  HttpContext *pContext = (HttpContext *)param;
  if (pContext == NULL) return;

  HttpSqlCmds *     multiCmds = pContext->multiCmds;
  HttpEncodeMethod *encode = pContext->encodeMethod;

  HttpSqlCmd *singleCmd = multiCmds->cmds + multiCmds->pos;
  char *      sql = httpGetCmdsString(pContext, singleCmd->sql);

  bool isContinue = false;

  if (numOfRows > 0) {
    if (singleCmd->cmdReturnType == HTTP_CMD_RETURN_TYPE_WITH_RETURN && encode->buildQueryJsonFp) {
      isContinue = (encode->buildQueryJsonFp)(pContext, singleCmd, result, numOfRows);
    }
  }

  if (isContinue) {
    // retrieve next batch of rows
    httpDebug("context:%p, fd:%d, ip:%s, user:%s, process pos:%d, continue retrieve, numOfRows:%d, sql:%s",
              pContext, pContext->fd, pContext->ipstr, pContext->user, multiCmds->pos, numOfRows, sql);
    taos_fetch_rows_a(result, httpProcessMultiSqlRetrieveCallBack, param);
  } else {
    httpDebug("context:%p, fd:%d, ip:%s, user:%s, process pos:%d, stop retrieve, numOfRows:%d, sql:%s",
              pContext, pContext->fd, pContext->ipstr, pContext->user, multiCmds->pos, numOfRows, sql);

    if (numOfRows < 0) {
      httpError("context:%p, fd:%d, ip:%s, user:%s, process pos:%d, retrieve failed code:%s, sql:%s",
                pContext, pContext->fd, pContext->ipstr, pContext->user, multiCmds->pos, tstrerror(numOfRows), sql);
    } 
    
    taos_free_result(result);
    
    if (singleCmd->cmdReturnType == HTTP_CMD_RETURN_TYPE_WITH_RETURN && encode->stopJsonFp) {
      (encode->stopJsonFp)(pContext, singleCmd);
    }
    multiCmds->pos++;
    httpProcessMultiSql(pContext);
  }
}

void httpProcessMultiSqlCallBack(void *param, TAOS_RES *result, int code) {
  HttpContext *pContext = (HttpContext *)param;
  if (pContext == NULL) return;

  HttpSqlCmds *     multiCmds = pContext->multiCmds;
  HttpEncodeMethod *encode = pContext->encodeMethod;

  HttpSqlCmd *singleCmd = multiCmds->cmds + multiCmds->pos;
  char *      sql = httpGetCmdsString(pContext, singleCmd->sql);

  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    httpWarn("context:%p, fd:%d, ip:%s, user:%s, process pos:%d, code:%s:inprogress, sql:%s",
             pContext, pContext->fd, pContext->ipstr, pContext->user, multiCmds->pos, tstrerror(code), sql);
    return;
  }

  if (code < 0) {
    if (encode->checkFinishedFp != NULL && !encode->checkFinishedFp(pContext, singleCmd, code)) {
      singleCmd->code = code;
      httpDebug("context:%p, fd:%d, ip:%s, user:%s, process pos jump to:%d, last code:%s, last sql:%s",
                pContext, pContext->fd, pContext->ipstr, pContext->user, multiCmds->pos + 1, tstrerror(code), sql);
    } else {
      singleCmd->code = code;
      httpError("context:%p, fd:%d, ip:%s, user:%s, process pos:%d, error code:%s, sql:%s",
                pContext, pContext->fd, pContext->ipstr, pContext->user, multiCmds->pos, tstrerror(code), sql);

      if (singleCmd->cmdReturnType == HTTP_CMD_RETURN_TYPE_WITH_RETURN) {
        if (encode->startJsonFp) (encode->startJsonFp)(pContext, singleCmd, result);
        if (encode->stopJsonFp) (encode->stopJsonFp)(pContext, singleCmd);
      }
    }
    multiCmds->pos++;
    httpProcessMultiSql(pContext);

    taos_free_result(result);
    return;
  }

  int num_fields = taos_field_count(result);
  if (num_fields == 0) {
    // not select or show commands
    int affectRows = taos_affected_rows(result);
    httpDebug("context:%p, fd:%d, ip:%s, user:%s, process pos:%d, affect rows:%d, sql:%s",
              pContext, pContext->fd, pContext->ipstr, pContext->user, multiCmds->pos, affectRows, sql);

    singleCmd->code = 0;

    if (singleCmd->cmdReturnType == HTTP_CMD_RETURN_TYPE_WITH_RETURN && encode->startJsonFp) {
      (encode->startJsonFp)(pContext, singleCmd, result);
    }

    if (singleCmd->cmdReturnType == HTTP_CMD_RETURN_TYPE_WITH_RETURN && encode->buildAffectRowJsonFp) {
      (encode->buildAffectRowJsonFp)(pContext, singleCmd, affectRows);
    }

    if (singleCmd->cmdReturnType == HTTP_CMD_RETURN_TYPE_WITH_RETURN && encode->stopJsonFp) {
      (encode->stopJsonFp)(pContext, singleCmd);
    }

    if (encode->setNextCmdFp) {
      (encode->setNextCmdFp)(pContext, singleCmd, code);
    } else {
      multiCmds->pos++;
    }

    taos_free_result(result);
    httpProcessMultiSql(pContext);
  } else {
    httpDebug("context:%p, fd:%d, ip:%s, user:%s, process pos:%d, start retrieve, sql:%s",
              pContext, pContext->fd, pContext->ipstr, pContext->user, multiCmds->pos, sql);

    if (singleCmd->cmdReturnType == HTTP_CMD_RETURN_TYPE_WITH_RETURN && encode->startJsonFp) {
      (encode->startJsonFp)(pContext, singleCmd, result);
    }
    taos_fetch_rows_a(result, httpProcessMultiSqlRetrieveCallBack, pContext);
  }
}

void httpProcessMultiSql(HttpContext *pContext) {
  HttpSqlCmds *     multiCmds = pContext->multiCmds;
  HttpEncodeMethod *encode = pContext->encodeMethod;

  if (multiCmds->pos >= multiCmds->size) {
    httpDebug("context:%p, fd:%d, ip:%s, user:%s, process pos:%d, size:%d, stop mulit-querys",
              pContext, pContext->fd, pContext->ipstr, pContext->user, multiCmds->pos, multiCmds->size);
    if (encode->cleanJsonFp) {
      (encode->cleanJsonFp)(pContext);
    }
    httpCloseContextByApp(pContext);
    return;
  }

  HttpSqlCmd *cmd = multiCmds->cmds + multiCmds->pos;

  char *sql = httpGetCmdsString(pContext, cmd->sql);
  httpTraceDump("context:%p, fd:%d, ip:%s, user:%s, process pos:%d, start query, sql:%s", pContext, pContext->fd,
           pContext->ipstr, pContext->user, multiCmds->pos, sql);
  taosNotePrintHttp(sql);
  taos_query_a(pContext->session->taos, sql, httpProcessMultiSqlCallBack, (void *)pContext);
}

void httpProcessMultiSqlCmd(HttpContext *pContext) {
  if (pContext == NULL) return;

  HttpSqlCmds *multiCmds = pContext->multiCmds;
  if (multiCmds == NULL || multiCmds->size <= 0 || multiCmds->pos >= multiCmds->size || multiCmds->pos < 0) {
    httpSendErrorResp(pContext, HTTP_INVALID_MULTI_REQUEST);
    return;
  }

  httpDebug("context:%p, fd:%d, ip:%s, user:%s, start multi-querys pos:%d, size:%d", pContext, pContext->fd,
            pContext->ipstr, pContext->user, multiCmds->pos, multiCmds->size);
  HttpEncodeMethod *encode = pContext->encodeMethod;
  if (encode->initJsonFp) {
    (encode->initJsonFp)(pContext);
  }

  httpProcessMultiSql(pContext);
}

void httpProcessSingleSqlRetrieveCallBack(void *param, TAOS_RES *result, int numOfRows) {
  HttpContext *pContext = (HttpContext *)param;
  if (pContext == NULL) return;

  HttpEncodeMethod *encode = pContext->encodeMethod;

  bool isContinue = false;

  if (numOfRows > 0) {
    if (encode->buildQueryJsonFp) {
      isContinue = (encode->buildQueryJsonFp)(pContext, &pContext->singleCmd, result, numOfRows);
    }
  }

  if (tscResultsetFetchCompleted(result)) {
    isContinue = false;
  }

  if (isContinue) {
    // retrieve next batch of rows
    httpDebug("context:%p, fd:%d, ip:%s, user:%s, continue retrieve, numOfRows:%d", pContext, pContext->fd,
              pContext->ipstr, pContext->user, numOfRows);
    taos_fetch_rows_a(result, httpProcessSingleSqlRetrieveCallBack, param);
  } else {
    httpDebug("context:%p, fd:%d, ip:%s, user:%s, stop retrieve, numOfRows:%d", pContext, pContext->fd, pContext->ipstr,
              pContext->user, numOfRows);

    if (numOfRows < 0) {
      httpError("context:%p, fd:%d, ip:%s, user:%s, retrieve failed, code:%s", pContext, pContext->fd, pContext->ipstr,
                pContext->user, tstrerror(numOfRows));
    } 
    
    taos_free_result(result);    

    if (encode->stopJsonFp) {
      (encode->stopJsonFp)(pContext, &pContext->singleCmd);
    }

    httpCloseContextByApp(pContext);
  }
}

void httpProcessSingleSqlCallBack(void *param, TAOS_RES *result, int code) {
  HttpContext *pContext = (HttpContext *)param;
  if (pContext == NULL) return;

  HttpEncodeMethod *encode = pContext->encodeMethod;

  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    httpError("context:%p, fd:%d, ip:%s, user:%s, query error, taos:%p, code:%s:inprogress, sqlObj:%p",
              pContext, pContext->fd, pContext->ipstr, pContext->user, pContext->session->taos, tstrerror(code), (SSqlObj *)result);
    return;
  }

  if (code < 0) {
    SSqlObj *pObj = (SSqlObj *)result;
    if (code == TSDB_CODE_TSC_INVALID_SQL) {
      httpError("context:%p, fd:%d, ip:%s, user:%s, query error, taos:%p, code:%s, sqlObj:%p, error:%s",
                pContext, pContext->fd, pContext->ipstr, pContext->user, pContext->session->taos, tstrerror(code), pObj, pObj->cmd.payload);
      httpSendTaosdInvalidSqlErrorResp(pContext, pObj->cmd.payload);
    } else {
      httpError("context:%p, fd:%d, ip:%s, user:%s, query error, taos:%p, code:%s, sqlObj:%p",
                pContext, pContext->fd, pContext->ipstr, pContext->user, pContext->session->taos, tstrerror(code), pObj);
      httpSendTaosdErrorResp(pContext, code);
    }
    taos_free_result(result);
    return;
  }

  int num_fields = taos_field_count(result);
  if (num_fields == 0) {
    // not select or show commands
    int affectRows = taos_affected_rows(result);

    httpDebug("context:%p, fd:%d, ip:%s, user:%s, affect rows:%d, stop query, sqlObj:%p",
              pContext, pContext->fd, pContext->ipstr, pContext->user, affectRows, result);

    if (encode->startJsonFp) {
      (encode->startJsonFp)(pContext, &pContext->singleCmd, result);
    }

    if (encode->buildAffectRowJsonFp) {
      (encode->buildAffectRowJsonFp)(pContext, &pContext->singleCmd, affectRows);
    }

    if (encode->stopJsonFp) {
      (encode->stopJsonFp)(pContext, &pContext->singleCmd);
    }

    taos_free_result(result);
    httpCloseContextByApp(pContext);
  } else {
    httpDebug("context:%p, fd:%d, ip:%s, user:%s, start retrieve", pContext, pContext->fd, pContext->ipstr,
              pContext->user);

    if (encode->startJsonFp) {
      (encode->startJsonFp)(pContext, &pContext->singleCmd, result);
    }

    taos_fetch_rows_a(result, httpProcessSingleSqlRetrieveCallBack, pContext);
  }
}

void httpProcessSingleSqlCmd(HttpContext *pContext) {
  HttpSqlCmd * cmd = &pContext->singleCmd;
  char *       sql = cmd->nativSql;
  HttpSession *pSession = pContext->session;

  if (sql == NULL || sql[0] == 0) {
    httpError("context:%p, fd:%d, ip:%s, user:%s, error:no sql input", pContext, pContext->fd, pContext->ipstr,
              pContext->user);
    httpSendErrorResp(pContext, HTTP_NO_SQL_INPUT);
    return;
  }

  httpTraceDump("context:%p, fd:%d, ip:%s, user:%s, start query, sql:%s", pContext, pContext->fd, pContext->ipstr,
           pContext->user, sql);
  taosNotePrintHttp(sql);
  taos_query_a(pSession->taos, sql, httpProcessSingleSqlCallBack, (void *)pContext);
}

void httpProcessLoginCmd(HttpContext *pContext) {
  char token[128] = {0};
  if (!httpGenTaosdAuthToken(pContext, token, 128)) {
    httpSendErrorResp(pContext, HTTP_GEN_TAOSD_TOKEN_ERR);
  } else {
    httpDebug("context:%p, fd:%d, ip:%s, user:%s, login via http, return token:%s",
              pContext, pContext->fd, pContext->ipstr, pContext->user, token);
    httpSendSuccResp(pContext, token);
  }
}

void httpProcessHeartBeatCmd(HttpContext *pContext) {
  HttpEncodeMethod *encode = pContext->encodeMethod;
  if (encode->startJsonFp) {
    (encode->startJsonFp)(pContext, &pContext->singleCmd, NULL);
  }
  if (encode->stopJsonFp) {
    (encode->stopJsonFp)(pContext, &pContext->singleCmd);
  }
  httpCloseContextByApp(pContext);
}

void httpExecCmd(HttpContext *pContext) {
  switch (pContext->reqType) {
    case HTTP_REQTYPE_LOGIN:
      httpProcessLoginCmd(pContext);
      break;
    case HTTP_REQTYPE_SINGLE_SQL:
      httpProcessSingleSqlCmd(pContext);
      break;
    case HTTP_REQTYPE_MULTI_SQL:
      httpProcessMultiSqlCmd(pContext);
      break;
    case HTTP_REQTYPE_HEARTBEAT:
      httpProcessHeartBeatCmd(pContext);
      break;
    case HTTP_REQTYPE_OTHERS:
      httpCloseContextByApp(pContext);
      break;
    default:
      httpCloseContextByApp(pContext);
      break;
  }
}

void httpProcessRequestCb(void *param, TAOS_RES *result, int code) {
  HttpContext *pContext = param;
  taos_free_result(result);
  
  if (pContext == NULL) return;

  if (code < 0) {
    httpError("context:%p, fd:%d, ip:%s, user:%s, login error, code:%s", pContext, pContext->fd, pContext->ipstr,
              pContext->user, tstrerror(code));
    httpSendTaosdErrorResp(pContext, code);
    return;
  }

  httpDebug("context:%p, fd:%d, ip:%s, user:%s, connect tdengine success, taos:%p", pContext, pContext->fd,
            pContext->ipstr, pContext->user, pContext->taos);
  if (pContext->taos == NULL) {
    httpError("context:%p, fd:%d, ip:%s, user:%s, login error, taos is empty", pContext, pContext->fd, pContext->ipstr,
              pContext->user);
    httpSendErrorResp(pContext, HTTP_NO_ENOUGH_SESSIONS);
    return;
  }

  httpCreateSession(pContext, pContext->taos);

  if (pContext->session == NULL) {
    httpSendErrorResp(pContext, HTTP_SESSION_FULL);
    httpCloseContextByApp(pContext);
  } else {
    httpExecCmd(pContext);
  }
}

void httpProcessRequest(HttpContext *pContext) {
  httpGetSession(pContext);

  if (pContext->session == NULL || pContext->reqType == HTTP_REQTYPE_LOGIN) {
    taos_connect_a(NULL, pContext->user, pContext->pass, "", 0, httpProcessRequestCb, (void *)pContext,
                   &(pContext->taos));
    httpDebug("context:%p, fd:%d, ip:%s, user:%s, try connect tdengine, taos:%p", pContext, pContext->fd,
              pContext->ipstr, pContext->user, pContext->taos);
  } else {
    httpExecCmd(pContext);
  }
}
