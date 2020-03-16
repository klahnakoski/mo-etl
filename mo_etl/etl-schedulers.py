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

import loguru

import adr
import jx_sqlite
import mo_math
from jx_bigquery import bigquery
from jx_python import jx
from mo_dots import Data, coalesce, listwrap, wrap
from mo_future import first
from mo_logs import startup, constants, Log, machine_metadata
from mo_threads import Queue, Thread, MAIN_THREAD
from mo_times import Date, Duration
from mozci.push import make_push_objects
from pyLibrary.env import git


def process(config, please_stop):
    config.range.min = Date(config.range.min)
    config.range.max = Date(config.range.max)
    config.start = Date(config.start)
    config.interval = Duration(config.interval)
    config.branches = listwrap(config.branches)
    config.schedulers = listwrap(config.schedulers)
    destination = bigquery.Dataset(config.destination).get_or_create_table(
        config.destination
    )

    todo = Queue("work", max=10000)

    etl_config_table = jx_sqlite.Container(config.config_db).get_or_create_facts(
        "etl-range"
    )
    data = wrap(etl_config_table.query()).data
    prev_done = data[0]
    done = Data(
        min=Date(coalesce(prev_done.min, config.start, "today-2day")),
        max=Date(coalesce(prev_done.max, config.start, "today-2day")),
    )
    if not len(data):
        etl_config_table.add(done)

    if done.max < config.range.max:
        # ADD WORK GOING FORWARDS
        start = Date.floor(done.max, config.interval)
        while start < config.range.max:
            end = start + config.interval
            for branch in config.branches:
                for sched in config.schedulers:
                    todo.add((start, end, branch, sched))
            start = end
    if config.range.min < done.min:
        # ADD WORK GOING BACKWARDS
        end = Date.ceiling(done.min, config.interval)
        while config.range.min < end:
            start = end - config.interval
            for branch in config.branches:
                for sched in config.schedulers:
                    todo.add((start, end, branch, sched))
            end = start

    def process_one(start, end, branch, scheduler):
        # ASSUME PREVIOUS WORK IS DONE
        # UPDATE THE DATABASE STATE
        done.min = mo_math.min(end, done.min)
        done.max = mo_math.max(start, done.max)
        etl_config_table.update({"set": done})

        # compute dates in range
        pushes = make_push_objects(
            from_date=start.format(), to_date=end.format(), branch=branch
        )

        Log.note(
            "Found {{num}} pushes for {{scheduler}} on {{branch}} in ({{start}}, {{end}})",
            num=len(pushes),
            start=start,
            end=end,
            scheduler=scheduler,
            branch=branch,
        )
        data = []
        for push in pushes:
            tasks = push.get_shadow_scheduler_tasks(scheduler)
            if push.backedout:
                if push.get_likely_regressions("label") & tasks:
                    backout = "primary"
                else:
                    backout = "secondary"
            else:
                backout = None

            data.append(
                {
                    "push": {
                        "id": push.id,
                        "date": push.date,
                        "changesets": push.changesets,
                    },
                    "tasks": jx.sort(tasks),
                    "scheduler": scheduler,
                    "branch": branch,
                    "backout": backout,
                    "etl": {"version": git.get_revision(), "timestamp": Date.now()},
                }
            )
        destination.extend(data)

    try:
        while not please_stop:
            try:
                start, end, branch, scheduler = todo.pop_one()
            except Exception:
                Log.note("no more work")
                break
            process_one(start, end, branch, scheduler)
    except Exception as e:
        Log.warning("Could not complete the etl", cause=e)



def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)

        adr.configure(config.adr)

        # SHUNT ADR LOGGING TO MAIN LOGGING
        # https://loguru.readthedocs.io/en/stable/api/logger.html#loguru._logger.Logger.add
        loguru.logger.remove()
        loguru.logger.add(
            _logging,
            level="DEBUG",
            format="{message}",
            filter=lambda r: True,
        )
        Log.start(config.debug)
        Thread.run("processing", process, config)
        MAIN_THREAD.wait_for_shutdown_signal(wait_forever=False)
    except Exception as e:
        Log.warning("Problem with etl! Shutting down.", cause=e)
    finally:
        Log.stop()


def _logging(message):
    # params = wrap(message.record)
    # Log.note(message, default_params=params)

    params = message.record
    params['machine'] = machine_metadata
    log_format = "{{machine.name}} (pid {{process}}) - {{time|datetime}} - {{thread}} - \"{{file}}:{{line}}\" - ({{function}}) - {{message}}"
    Log.main_log.write(log_format, params)


if __name__ == "__main__":
    main()
