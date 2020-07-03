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
#include "taos.h"
#include "tutil.h"
#include "tconfig.h"
#include "tglobal.h"
#include "dnode.h"
#include "dnodeInt.h"
#include "dnodeMgmt.h"
#include "dnodePeer.h"
#include "dnodeModule.h"
#include "dnodeVRead.h"
#include "dnodeVWrite.h"
#include "dnodeMRead.h"
#include "dnodeMWrite.h"
#include "dnodeMPeer.h"
#include "dnodeShell.h"

static int32_t dnodeInitStorage();
static void dnodeCleanupStorage();
static void dnodeSetRunStatus(SDnodeRunStatus status);
static void dnodeCheckDataDirOpenned(char *dir);
static SDnodeRunStatus tsDnodeRunStatus = TSDB_DNODE_RUN_STATUS_STOPPED;
static int32_t dnodeInitComponents();
static void dnodeCleanupComponents(int32_t stepId);
static int dnodeCreateDir(const char *dir);

typedef struct {
  const char *const name;
  int               (*init)();
  void              (*cleanup)();
} SDnodeComponent;

static const SDnodeComponent tsDnodeComponents[] = {
  {"storage", dnodeInitStorage,    dnodeCleanupStorage},
  {"vread",   dnodeInitVnodeRead,  dnodeCleanupVnodeRead},
  {"vwrite",  dnodeInitVnodeWrite, dnodeCleanupVnodeWrite},
  {"mread",   dnodeInitMnodeRead,  dnodeCleanupMnodeRead},
  {"mwrite",  dnodeInitMnodeWrite, dnodeCleanupMnodeWrite},
  {"mpeer",   dnodeInitMnodePeer,  dnodeCleanupMnodePeer},  
  {"client",  dnodeInitClient,     dnodeCleanupClient},
  {"server",  dnodeInitServer,     dnodeCleanupServer},
  {"mgmt",    dnodeInitMgmt,       dnodeCleanupMgmt},
  {"modules", dnodeInitModules,    dnodeCleanupModules},
  {"shell",   dnodeInitShell,      dnodeCleanupShell}
};

static int dnodeCreateDir(const char *dir) {
  if (mkdir(dir, 0755) != 0 && errno != EEXIST) {
    return -1;
  }
  
  return 0;
}

static void dnodeCleanupComponents(int32_t stepId) {
  for (int32_t i = stepId; i >= 0; i--) {
    tsDnodeComponents[i].cleanup();
  }
}

static int32_t dnodeInitComponents() {
  int32_t code = 0;
  for (int32_t i = 0; i < sizeof(tsDnodeComponents) / sizeof(tsDnodeComponents[0]); i++) {
    if (tsDnodeComponents[i].init() != 0) {
      dnodeCleanupComponents(i);
      code = -1;
      break;
    }
  }
  return code;
}

int32_t dnodeInitSystem() {
  dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_INITIALIZE);
  tscEmbedded  = 1;
  taosBlockSIGPIPE();
  taosResolveCRC();
  taosInitGlobalCfg();
  taosReadGlobalLogCfg();
  taosSetCoreDump();
  signal(SIGPIPE, SIG_IGN);

  if (dnodeCreateDir(tsLogDir) < 0) {
   printf("failed to create dir: %s, reason: %s\n", tsLogDir, strerror(errno));
   return -1;
  }

  char temp[TSDB_FILENAME_LEN];
  sprintf(temp, "%s/taosdlog", tsLogDir);
  if (taosInitLog(temp, tsNumOfLogLines, 1) < 0) {
    printf("failed to init log file\n");
  }

  if (!taosReadGlobalCfg() || !taosCheckGlobalCfg()) {
    taosPrintGlobalCfg();
    dError("TDengine read global config failed");
    return -1;
  }
  taosPrintGlobalCfg();

  dInfo("start to initialize TDengine on %s", tsLocalEp);

  if (dnodeInitComponents() != 0) {
    return -1;
  }

  dnodeStartModules();
  dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_RUNING);
  dnodeStartStream();

  dInfo("TDengine is initialized successfully");

  return 0;
}

void dnodeCleanUpSystem() {
  if (dnodeGetRunStatus() != TSDB_DNODE_RUN_STATUS_STOPPED) {
    dnodeSetRunStatus(TSDB_DNODE_RUN_STATUS_STOPPED);
    dnodeCleanupComponents(sizeof(tsDnodeComponents) / sizeof(tsDnodeComponents[0]) - 1);
    taos_cleanup();
    taosCloseLog();
  }
}

SDnodeRunStatus dnodeGetRunStatus() {
  return tsDnodeRunStatus;
}

static void dnodeSetRunStatus(SDnodeRunStatus status) {
  tsDnodeRunStatus = status;
}

static void dnodeCheckDataDirOpenned(char *dir) {
  char filepath[256] = {0};
  sprintf(filepath, "%s/.running", dir);

  int fd = open(filepath, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    dError("failed to open lock file:%s, reason: %s, quit", filepath, strerror(errno));
    exit(0);
  }
  int32_t ret = flock(fd, LOCK_EX | LOCK_NB);
  if (ret != 0) {
    dError("failed to lock file:%s ret:%d, database may be running, quit", filepath, ret);
    close(fd);
    exit(0);
  }
}

static int32_t dnodeInitStorage() {
  if (dnodeCreateDir(tsDataDir) < 0) {
   dError("failed to create dir: %s, reason: %s", tsDataDir, strerror(errno));
   return -1;
  }
  sprintf(tsMnodeDir, "%s/mnode", tsDataDir);
  sprintf(tsVnodeDir, "%s/vnode", tsDataDir);
  sprintf(tsDnodeDir, "%s/dnode", tsDataDir);
  sprintf(tsVnodeBakDir, "%s/vnode_bak", tsDataDir);

  //TODO(dengyihao): no need to init here 
  if (dnodeCreateDir(tsMnodeDir) < 0) {
   dError("failed to create dir: %s, reason: %s", tsMnodeDir, strerror(errno));
   return -1;
  } 
  //TODO(dengyihao): no need to init here
  if (dnodeCreateDir(tsVnodeDir) < 0) {
   dError("failed to create dir: %s, reason: %s", tsVnodeDir, strerror(errno));
   return -1;
  }
  if (dnodeCreateDir(tsDnodeDir) < 0) {
   dError("failed to create dir: %s, reason: %s", tsDnodeDir, strerror(errno));
   return -1;
  } 
  if (dnodeCreateDir(tsVnodeBakDir) < 0) {
   dError("failed to create dir: %s, reason: %s", tsVnodeBakDir, strerror(errno));
   return -1;
  }

  dnodeCheckDataDirOpenned(tsDnodeDir);

  dInfo("storage directory is initialized");
  return 0;
}

static void dnodeCleanupStorage() {}

bool  dnodeIsFirstDeploy() {
  return strcmp(tsFirst, tsLocalEp) == 0;
}
