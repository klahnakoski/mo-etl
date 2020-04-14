# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from mo_dots import Null, Data
from mo_future import text
from mo_logs import Log


class Protect:
    """
    WHEN USING mozci: WEAR PROTECTION!
    """

    __slots__ = ["obj"]

    def __new__(cls, v):
        if isinstance(v, (int, float, text, Data, Protect)):
            return v
        return object.__new__(cls)

    def __init__(self, o):
        self.obj = o

    def __getattr__(self, item):
        try:
            result = getattr(self.obj, item)
        except AttributeError as ae:
            raise ae
        except Exception as e:
            Log.warning("Could not get {{attribute}}", attribute=item, cause=e)
            return Null

        if result == None:
            return Null
        return Protect(result)

    def __call__(self, *args, **kwargs):
        try:
            return Protect(self.obj(*args, **kwargs))
        except Exception as e:
            Log.warning("Could not call function", cause=e)
            return Null

    def __iter__(self):
        for i in self.obj:
            yield Protect(i)

    def __len__(self):
        return len(self.obj)

    def __data__(self):
        return self.obj
