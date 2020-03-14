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

from mo_http import http

ACTIVEDATA_URL = "https://activedata.allizom.org/query"

def process(source_key, source, destination, resources, please_stop=None):


    for task in source:


    # pull net new tasks to process


    # for each task

    # get the optimized_tasks.list

    tasks = http.get(ACTIVEDATA_URL, json={
        "from":"tasks",
        "where": {
            "gte":{"action.start_time":{"date":"today-month"}},
            "suffix":{"task.artifacts.name":"/optimized_tasks.list"}
        },
        "sort":{"action.start_time":"desc"},
        "limit":10000,
    })


    for t in tasks:

    destination.add(




        for t in tasks
    )

