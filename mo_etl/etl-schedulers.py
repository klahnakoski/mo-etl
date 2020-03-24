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

import os
from copy import copy

import loguru
from cachy import CacheManager
from cachy.stores import NullStore

import adr
import jx_sqlite
import mo_math
from adr.configuration import Configuration
from adr.errors import MissingDataError
from jx_bigquery import bigquery
from jx_python import jx
from mo_dots import Data, coalesce, listwrap, wrap, set_default
from mo_future import text
from mo_logs import startup, constants, Log, machine_metadata
from mo_threads import Queue
from mo_times import Date, Duration, Timer
from mozci.push import make_push_objects
from mozci.task import Status
from pyLibrary.env import git

DEFAULT_START = "today-2day"
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
                todo.add((start, end, branch))
            start = end
    if config.range.min < done.min:
        # ADD WORK GOING BACKWARDS
        end = Date.ceiling(done.min, config.interval)
        while config.range.min < end:
            start = end - config.interval
            for branch in config.branches:
                todo.add((start, end, branch))
            end = start

    def process_one(start, end, branch):
        # ASSUME PREVIOUS WORK IS DONE
        # UPDATE THE DATABASE STATE
        done.min = mo_math.min(end, done.min)
        done.max = mo_math.max(start, done.max)
        etl_config_table.update({"set": done})

        try:
            pushes = make_push_objects(
                from_date=start.format(),
                to_date=end.format(),
                branch=branch
            )
        except MissingDataError:
            pushes = []
        except Exception as e:
            raise Log.error("not expected", cause=e)

        Log.note(
            "Found {{num}} pushes on {{branch}} in ({{start}}, {{end}})",
            num=len(pushes),
            start=start,
            end=end,
            branch=branch,
        )

        for push in pushes:
            # RECORD THE PUSH
            backout_hash = push.backedoutby
            with Timer("get tasks for push {{push}}", {"push": push.id}):
                tasks = {}
                for s in config.schedulers:
                    try:
                        tasks[s] = jx.sort(push.get_shadow_scheduler_tasks(s))
                    except Exception:
                        pass

                regressed_labels = set()
                if backout_hash:
                    with Timer("find regressed labels for {{push}}", {"push": push.id}):
                        end = push
                        while backout_hash not in end.revs:
                            regressed_labels |= end.get_regressions("label").keys()
                            end = end.child

                data.append(
                    {
                        "push": {
                            "id": push.id,
                            "date": push.date,
                            "changesets": push.revs,
                        },
                        "tasks": tasks,
                        "indicators": [{"label": name} for name in jx.sort(regressed_labels)],
                        "branch": branch,
                        "etl": {"version": git.get_revision(), "timestamp": Date.now()},
                    }
                )

        Log.note("adding {{num}} records to bigquery", num=len(data))
        config._destination.extend(data)

    try:
        while True:
            try:
                start, end, branch = todo.pop_one()
            except Exception:
                Log.note("no more work")
                break
            process_one(start, end, branch)
    except Exception as e:
        Log.warning("Could not complete the etl", cause=e)
    else:
        config._destination.merge_shards()


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)

        # ADD CONFIGURATION INJECTOR
        def update(self, config):
            """
            Update the configuration object with new parameters
            :param config: dict of configuration
            """
            for k, v in config.items():
                if v != None:
                    self._config[k] = v

            self._config["sources"] = sorted(map(os.path.expanduser, set(self._config["sources"])))

            # Use the NullStore by default. This allows us to control whether
            # caching is enabled or not at runtime.
            self._config["cache"].setdefault("stores", {"null": {"driver": "null"}})
            object.__setattr__(self, "cache", CacheManager(self._config["cache"]))
            self.cache.extend("null", lambda driver: NullStore())
        setattr(Configuration, "update", update)

        adr.config.update(config.adr)

        # SHUNT ADR LOGGING TO MAIN LOGGING
        # https://loguru.readthedocs.io/en/stable/api/logger.html#loguru._logger.Logger.add
        loguru.logger.remove()
        loguru.logger.add(
            _logging, level="DEBUG", format="{message}", filter=lambda r: True,
        )
        Log.start(config.debug)
        config = normalize_config(config)
        all_pushes(config)
    except Exception as e:
        Log.warning("Problem with etl! Shutting down.", cause=e)
    finally:
        Log.stop()


def _logging(message):
    # params = wrap(message.record)
    # Log.note(message, default_params=params)

    params = message.record
    params["machine"] = machine_metadata
    log_format = '{{machine.name}} (pid {{process}}) - {{time|datetime}} - {{thread}} - "{{file}}:{{line}}" - ({{function}}) - {{message}}'
    Log.main_log.write(log_format, params)


if __name__ == "__main__":
    main()
