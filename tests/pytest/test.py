#!/usr/bin/python
###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################
# install pip
# pip install src/connector/python/linux/python2/

# -*- coding: utf-8 -*-
import sys
import getopt
import subprocess
from distutils.log import warn as printf

from util.log import *
from util.dnodes import *
from util.cases import *

import taos


if __name__ == "__main__":
    fileName = "all"
    deployPath = ""
    masterIp = ""
    testCluster = False
    valgrind = 0
    logSql = True
    stop = 0
    opts, args = getopt.gnu_getopt(sys.argv[1:], 'f:p:m:l:scgh', [
        'file=', 'path=', 'master', 'logSql', 'stop', 'cluster', 'valgrind', 'help'])
    for key, value in opts:
        if key in ['-h', '--help']:
            tdLog.printNoPrefix(
                'A collection of test cases written using Python')
            tdLog.printNoPrefix('-f Name of test case file written by Python')
            tdLog.printNoPrefix('-p Deploy Path for Simulator')
            tdLog.printNoPrefix('-m Master Ip for Simulator')
            tdLog.printNoPrefix('-l <True:False> logSql Flag')
            tdLog.printNoPrefix('-s stop All dnodes')
            tdLog.printNoPrefix('-c Test Cluster Flag')
            tdLog.printNoPrefix('-g valgrind Test Flag')
            sys.exit(0)

        if key in ['-f', '--file']:
            fileName = value

        if key in ['-p', '--path']:
            deployPath = value

        if key in ['-m', '--master']:
            masterIp = value

        if key in ['-l', '--logSql']:
            if (value.upper() == "TRUE"):
                logSql = True
            elif (value.upper() == "FALSE"):
                logSql = False
            else:
                tdLog.printNoPrefix("logSql value %s is invalid" % logSql)
                sys.exit(0)

        if key in ['-c', '--cluster']:
            testCluster = True

        if key in ['-g', '--valgrind']:
            valgrind = 1

        if key in ['-s', '--stop']:
            stop = 1

    if (stop != 0):
        if (valgrind == 0):
            toBeKilled = "taosd"
        else:
            toBeKilled = "valgrind.bin"

        killCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}' | xargs kill -HUP > /dev/null 2>&1" % toBeKilled

        psCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}'" % toBeKilled
        processID = subprocess.check_output(psCmd, shell=True)

        while(processID):
            os.system(killCmd)
            time.sleep(1)
            processID = subprocess.check_output(psCmd, shell=True)

        for port in range(6030, 6041):
            usePortPID = "lsof -i tcp:%d | grep LISTEn | awk '{print $2}'" % port
            processID = subprocess.check_output(usePortPID, shell=True)

            if processID:
                killCmd = "kill -9 %s" % processID
                os.system(killCmd)
            fuserCmd = "fuser -k -n tcp %d" % port
            os.system(fuserCmd)
        if valgrind:
            time.sleep(2)

        tdLog.info('stop All dnodes')
        sys.exit(0)

    tdDnodes.init(deployPath)
    tdDnodes.setTestCluster(testCluster)
    tdDnodes.setValgrind(valgrind)

    tdDnodes.stopAll()
    tdDnodes.deploy(1)
    tdDnodes.start(1)

    if masterIp == "":
        host = '127.0.0.1'
    else:
        host = masterIp

    tdLog.info("Procedures for tdengine deployed in %s" % (host))

    tdCases.logSql(logSql)

    if testCluster:
        tdLog.info("Procedures for testing cluster")
        if fileName == "all":
            tdCases.runAllCluster()
        else:
            tdCases.runOneCluster(fileName)
    else:
        tdLog.info("Procedures for testing self-deployment")
        conn = taos.connect(
            host,
            config=tdDnodes.getSimCfgPath())
        if fileName == "all":
            tdCases.runAllLinux(conn)
        else:
            tdCases.runOneLinux(conn, fileName)

    conn.close()
