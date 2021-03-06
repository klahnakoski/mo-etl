# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import FirstOp as FirstOp_
from jx_sqlite.expressions._utils import SQLang, check
from mo_logs import Log


class FirstOp(FirstOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = SQLang[self.term].to_sql(schema, not_null=True)
        for c in value:
            for t, v in c.sql.items():
                if t == "j":
                    Log.error("can not handle")
        return value
