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

import loguru
from cachy import CacheManager
from cachy.stores import NullStore

import adr
import jx_sqlite
import mo_math
import mozci
from adr.configuration import Configuration
from adr.errors import MissingDataError
from jx_bigquery import bigquery
from jx_python import jx
from mo_dots import Data, coalesce, listwrap, wrap
from mo_logs import startup, constants, Log, machine_metadata
from mo_threads import Process
from mo_times import Date, Duration, Timer
from mozci.push import make_push_objects
from pyLibrary.env import git

DEFAULT_START = "today-2day"
LOOK_BACK = 30
LOOK_FORWARD = 30


class Schedulers:
    def __init__(self, config):
        self.config = config = wrap(config)
        config.range.min = Date(config.range.min)
        config.range.max = Date(config.range.max)
        config.start = Date(config.start)
        config.interval = Duration(config.interval)
        config.branches = listwrap(config.branches)
        config.schedulers = listwrap(config.schedulers)
        self.destination = bigquery.Dataset(config.destination).get_or_create_table(
            config.destination
        )

        # CALCULATE THE PREVIOUS RUN
        mozci_version = self.version("mozci")
        self.etl_config_table = jx_sqlite.Container(
            config.config_db
        ).get_or_create_facts("etl-range")
        done_result = wrap(self.etl_config_table.query()).data
        prev_done = done_result[0]
        if len(done_result) and prev_done.mozci_version == mozci_version:
            self.done = Data(
                mozci_version=mozci_version,
                min=Date(coalesce(prev_done.min, config.start, "today-2day")),
                max=Date(coalesce(prev_done.max, config.start, "today-2day")),
            )
        else:
            self.done = Data(
                mozci_version=mozci_version,
                min=Date(coalesce(config.start, "today-2day")),
                max=Date(coalesce(config.start, "today-2day")),
            )
            self.etl_config_table.add(self.done)

    def version(self, package):
        with Process("", ["pip", "show", package]) as p:
            for line in p.stdout:
                if line.lower().startswith("version: "):
                    return line[9:].strip()
            return None

    def process_one(self, start, end, branch):
        # ASSUME PREVIOUS WORK IS DONE
        # UPDATE THE DATABASE STATE
        self.done.min = mo_math.min(end, self.done.min)
        self.done.max = mo_math.max(start, self.done.max)
        self.etl_config_table.update({"set": self.done})

        try:
            pushes = make_push_objects(
                from_date=start.format(), to_date=end.format(), branch=branch
            )
        except MissingDataError:
            return
        except Exception as e:
            raise Log.error("not expected", cause=e)

        Log.note(
            "Found {{num}} pushes on {{branch}} in ({{start}}, {{end}})",
            num=len(pushes),
            start=start,
            end=end,
            branch=branch,
        )

        data = []
        try:
            for push in pushes:
                with Timer("get tasks for push {{push}}", {"push": push.id}):
                    tasks = {}
                    for s in self.config.schedulers:
                        try:
                            tasks[s] = jx.sort(push.get_shadow_scheduler_tasks(s))
                        except Exception:
                            pass

                regressions = push.get_regressions("label").keys()

                # RECORD THE PUSH
                data.append(
                    {
                        "push": {
                            "id": push.id,
                            "date": push.date,
                            "changesets": push.revs,
                            "backedoutby": push.backedoutby,
                        },
                        "tasks": tasks,
                        "indicators": [
                            {"label": name} for name in jx.sort(regressions)
                        ],
                        "branch": branch,
                        "etl": {
                            "revision": git.get_revision(),
                            "timestamp": Date.now(),
                        },
                    }
                )
        finally:
            # ADD WHATEVER WE HAVE
            with Timer("adding {{num}} records to bigquery", {"num": len(data)}):
                self.destination.extend(data)

    def process(self):
        done = self.done
        config = self.config

        # ADD CHUNKS OF WORK
        self.todo = []
        if done.max < config.range.max:
            # ADD WORK GOING FORWARDS
            start = Date.floor(done.max, config.interval)
            while start < config.range.max:
                end = start + config.interval
                for branch in config.branches:
                    self.todo.append((start, end, branch))
                start = end
        if config.range.min < done.min:
            # ADD WORK GOING BACKWARDS
            end = Date.ceiling(done.min, config.interval)
            while config.range.min < end:
                start = end - config.interval
                for branch in config.branches:
                    self.todo.append((start, end, branch))
                end = start

        try:
            for start, end, branch in self.todo:
                self.process_one(start, end, branch)
        except Exception as e:
            Log.warning("Could not complete the etl", cause=e)
        else:
            self.destination.merge_shards()


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)

        with Timer("Add update() method to Configuration class"):

            def update(self, config):
                """
                Update the configuration object with new parameters
                :param config: dict of configuration
                """
                for k, v in config.items():
                    if v != None:
                        self._config[k] = v

                self._config["sources"] = sorted(
                    map(os.path.expanduser, set(self._config["sources"]))
                )

                # Use the NullStore by default. This allows us to control whether
                # caching is enabled or not at runtime.
                self._config["cache"].setdefault("stores", {"null": {"driver": "null"}})
                object.__setattr__(self, "cache", CacheManager(self._config["cache"]))
                self.cache.extend("null", lambda driver: NullStore())

            setattr(Configuration, "update", update)

        # UPDATE ADR COFIGURATION
        adr.config.update(config.adr)

        Log.start(config.debug)

        # SHUNT ADR LOGGING TO MAIN LOGGING
        # https://loguru.readthedocs.io/en/stable/api/logger.html#loguru._logger.Logger.add
        loguru.logger.remove()
        loguru.logger.add(
            _logging, level="DEBUG", format="{message}", filter=lambda r: True,
        )

        Schedulers(config).process()
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
