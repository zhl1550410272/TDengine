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

# -*- coding: utf-8 -*-

import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()

        tdSql.execute(
            'create table cars (ts timestamp, speed int) tags(id int)')
        tdSql.execute("create table car0 using cars tags(0)")
        tdSql.execute("create table car1 using cars tags(1)")
        tdSql.execute("create table car2 using cars tags(2)")

        tdSql.execute("insert into car0 values(now - 2m, 10) (now - 1m, 20)")
        tdSql.execute("insert into car1 values(now - 1m, 100) (now, 110)")

        tdSql.query("select last_row(*) from car0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 20)

        tdSql.query("select last_row(*) from cars")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 110)

        tdSql.query("select last_row(*) from cars group by id")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 20)
        tdSql.checkData(1, 1, 110)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
