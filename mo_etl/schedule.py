# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contect: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from mo_logs import Log

import mo_math
from mo_threads import Signal, Thread, Till, Process
from mo_times import Duration, Date


MAX_RUNTIME = "hour"
WAIT_FOR_SHUTDOWN = "5minute"
NO_JOB_WAITING_TIME = 60 # SECONDS TO WAIT IF THIS LIBRARY IS NOT USED
JOBS_WAITING_TIME = 30
schedules = []


class Schedule(object):

    def __init__(self, duration, starting, max_runtime=MAX_RUNTIME, wait_for_shutdown=WAIT_FOR_SHUTDOWN, name=None, params=None, cwd=None, env=None, debug=False, shell=False, bufsize=-1):
        self.duration = Duration(duration)
        self.starting = Date(starting)
        self.max_runtime = Duration(max_runtime)
        self.wait_for_shutdown = Duration(wait_for_shutdown)
        # Process parameters
        self.name = name
        self.params = params
        self.cwd = cwd
        self.env = env
        self.debug = debug
        self.shell = shell
        self.run_count = 0
        self.current = None

    def next_run(self):
        """
        :return: return signal for next
        """

        interval = mo_math.floor((Date.now() - self.starting)/self.duration)
        next_time = self.starting + (interval * self.duration)
        return Signal(next_time)

    def run_now(self):
        terminate = Till(seconds=self.max_runtime.seconds)

        self.current = Process(name=self.name, params=self.params, cwd=self.cwd, env=self.env, debug=self.debug)


        # after run is complete, or process crashes, schedule next

    def killer(self, please_stop):
        self.current.stop()
        (please_stop | self.current.service_stopped() | Till(seconds=self.wait_for_shutdown.seconds)).wait()
        if not self.current.service_stopped:



def monitor(please_stop=True):
    while not please_stop:
        if not schedules:
            (Till(seconds=NO_JOB_WAITING_TIME) | please_stop).wait()
            continue
        Log.note("Currently scheduled jobs: {{jobs}}", jobs=len(schedules))
        (Till(seconds=JOBS_WAITING_TIME) | please_stop).wait()

Log.alert("Job scheduler started...")
Thread.run("Monitor scheduled tasks", monitor)




