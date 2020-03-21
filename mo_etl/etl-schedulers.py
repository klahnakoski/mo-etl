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

from copy import copy

import loguru

import adr
import jx_sqlite
import mo_math
from adr.errors import MissingDataError
from jx_bigquery import bigquery
from jx_python import jx
from mo_dots import Data, coalesce, listwrap, wrap
from mo_logs import startup, constants, Log, machine_metadata
from mo_threads import Queue, Thread, MAIN_THREAD, THREAD_STOP, Lock
from mo_times import Date, Duration
from mozci.push import make_push_objects
from mozci.task import Status
from pyLibrary.env import git

LOOK_BACK = 30
LOOK_FORWARD = 30


def normalize_config(config):
    config.range.min = Date(config.range.min)
    config.range.max = Date(config.range.max)
    config.start = Date(config.start)
    config.interval = Duration(config.interval)
    config.branches = listwrap(config.branches)
    config.schedulers = listwrap(config.schedulers)
    config._destination = bigquery.Dataset(config.destination).get_or_create_table(
        config.destination
    )
    return config


def process(config, please_stop):
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
        try:
            pushes = make_push_objects(
                from_date=start.format(), to_date=end.format(), branch=branch
            )
        except MissingDataError:
            pushes = []
        except Exception as e:
            raise Log.error("not expected", cause=e)

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
            likely_regressions = push.get_likely_regressions("label")
            backout_by = push.backedoutby()
            candidate_regressions = [
                {
                    "name": name,
                    "child_count": child_count,
                    "status": {
                        Status.PASS: "pass",
                        Status.FAIL: "fail",
                        Status.INTERMITTENT: "intermittent",
                    }[status],
                }
                for name, (child_count, status) in push.get_candidate_regressions(
                    "label"
                ).items()
            ]
            backout_type = None
            if backout_by:
                if likely_regressions & tasks:
                    backout_type = "primary"
                else:
                    backout_type = "secondary"

            data.append(
                {
                    "push": {
                        "id": push.id,
                        "date": push.date,
                        "changesets": push.revs,
                    },
                    "tasks": jx.sort(tasks),
                    "likely_regressions": jx.sort(likely_regressions),
                    "candidate_regressions": jx.sort(candidate_regressions, "name"),
                    "scheduler": scheduler,
                    "branch": branch,
                    "backout_type": backout_type,
                    "backout_by": backout_by,
                    "etl": {"version": git.get_revision(), "timestamp": Date.now()},
                }
            )
        Log.note("adding {{num}} records to bigquery", num=len(data))
        config._destination.extend(data)

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

    config._destination.merge_shards()


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)

        adr.config.update(config.adr)

        # SHUNT ADR LOGGING TO MAIN LOGGING
        # https://loguru.readthedocs.io/en/stable/api/logger.html#loguru._logger.Logger.add
        loguru.logger.remove()
        loguru.logger.add(
            _logging, level="DEBUG", format="{message}", filter=lambda r: True,
        )
        Log.start(config.debug)
        config = normalize_config(config)

        Thread.run("processing", process, config)
        MAIN_THREAD.wait_for_shutdown_signal(wait_forever=False)
    except Exception as e:
        Log.warning("Problem with etl! Shutting down.", cause=e)
    finally:
        Log.stop()


def all_pushes(config):
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
        try:
            pushes = make_push_objects(
                from_date=start.format(), to_date=end.format(), branch=branch
            )
        except MissingDataError:
            pushes = []
        except Exception as e:
            raise Log.error("not expected", cause=e)

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
            likely_regressions = push.get_likely_regressions("label")
            backout_by = push.backedoutby()
            candidate_regressions = [
                {
                    "name": name,
                    "child_count": child_count,
                    "status": {
                        Status.PASS: "pass",
                        Status.FAIL: "fail",
                        Status.INTERMITTENT: "intermittent",
                    }[status],
                }
                for name, (child_count, status) in push.get_candidate_regressions(
                    "label"
                ).items()
            ]
            backout_type = None
            if backout_by:
                if likely_regressions & tasks:
                    backout_type = "primary"
                else:
                    backout_type = "secondary"

            data.append(
                {
                    "push": {
                        "id": push.id,
                        "date": push.date,
                        "changesets": push.revs,
                    },
                    "tasks": jx.sort(tasks),
                    "likely_regressions": jx.sort(likely_regressions),
                    "candidate_regressions": jx.sort(candidate_regressions, "name"),
                    "scheduler": scheduler,
                    "branch": branch,
                    "backout_type": backout_type,
                    "backout_by": backout_by,
                    "etl": {"version": git.get_revision(), "timestamp": Date.now()},
                }
            )
        Log.note("adding {{num}} records to bigquery", num=len(data))
        config._destination.extend(data)

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

    config._destination.merge_shards()






    state = Data(locker=Lock(), labels={})
    backedout = Queue("backouts")

    def get_parents(from_date, start_push, please_stop):
        # ITERATE BACKWARDS, TRIGGERING LOCAL CACHE
        push = start_push
        i = 0
        while not please_stop and Date(push.date) > from_date:
            push.id
            push.revs
            push.backsoutnodes
            with state.locker:
                state.labels |= push.label_summaries().keys()
                for name, summary in push.label_summaries().items():
                    state.results[name][push.id][summary.status.name] += 1
            if push.backedoutby and i > LOOK_FORWARD:
                backedout.add(push)

        backedout.add(THREAD_STOP)

    worker = Thread.run("get_pushes", get_parents)

    # GET TASKS AND TASK RESULTS FOR PUSH
    # FOR EACH BACKED OUT PUSH
    # BETWEEN BACKEDOUT AND BACKOUT DETERMINE TASK STATUS
    while True:
        bad = backedout.pop()
        # ENSURE WE HAVE THE PUSH SEQUENCE COVERING (start, bad, backout, end)
        # sequence = before + during + after
        parts = Data()
        start = bad
        for i in range(LOOK_BACK):
            start = start.parent
            parts.before += [start.id]
        backout = bad.backedoutby
        end = bad
        while end.id < backout.id:
            parts.during += [end.id]
            end = end.child
        for i in range(LOOK_FORWARD):
            parts.after += [end.id]
            end = end.child

        with state.locker:
            labels = copy(state.labels)

        indicators = []
        for label in jx.sort(labels):
            pushes = state.results[label]

            about = Data()
            for p in ("before", "during", "after"):
                for s in Status:
                    about[p][s.name] = sum(pushes[i][s.name] for i in parts[p])

            # IS THIS LABEL AN INDICATOR?
            if (
                coalesce(about.before.PASS, 0) > coalesce(about.before.FAIL, 0)  # SUCCESS BEFORE BAD PUSH
                and coalesce(about.during.PASS, 0) < coalesce(about.during.FAIL, 0)  # FAILURE DURING BAD PUSH
                and coalesce(about.after.PASS, 0) > coalesce(about.after.FAIL, 0)  # SUCCESS AFTER BACKOUT
            ):
                indicators.add(label)


def _logging(message):
    # params = wrap(message.record)
    # Log.note(message, default_params=params)

    params = message.record
    params["machine"] = machine_metadata
    log_format = '{{machine.name}} (pid {{process}}) - {{time|datetime}} - {{thread}} - "{{file}}:{{line}}" - ({{function}}) - {{message}}'
    Log.main_log.write(log_format, params)


if __name__ == "__main__":
    main()
