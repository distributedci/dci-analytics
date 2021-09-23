#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) Red Hat, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from dci_analytics.engine.workers import task_duration_cumulated
import psycopg2
from psycopg2 import extras as pg_extras

import atexit
import datetime
import fcntl
import logging
import sys
import traceback


LOG = logging.getLogger(__name__)

formatter = logging.Formatter("%(levelname)s - %(message)s")
streamhandler = logging.StreamHandler(stream=sys.stdout)
streamhandler.setFormatter(formatter)
LOG.addHandler(streamhandler)
LOG.setLevel(logging.DEBUG)


def format_field(record, prefix):
    res = {}
    for k, v in record.items():
        k_split = k.split("_", 1)
        if k_split[0] == prefix:
            res[k_split[1]] = v
            if isinstance(v, datetime.datetime):
                res[k_split[1]] = v.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return res


def format(records):
    res = {}
    js = {}
    for r in records:
        if r["jobs_id"] not in res:
            res[r["jobs_id"]] = format_field(r, "jobs")
        if "jobstates" not in res[r["jobs_id"]]:
            res[r["jobs_id"]]["jobstates"] = []
        jobstate = format_field(r, "jobstates")
        if jobstate["id"] not in js:
            js[jobstate["id"]] = jobstate
            res[r["jobs_id"]]["jobstates"].append(jobstate)
        js_file = format_field(r, "files")
        if "files" not in js[jobstate["id"]]:
            js[jobstate["id"]]["files"] = []
        js[jobstate["id"]]["files"].append(js_file)
    return list(res.values())


def get_db_connection():
    return psycopg2.connect(
        user="dci", password="dci", host="127.0.0.1", port="5432", database="dci"
    )


def get_table_columns_names(db_conn, table_name):
    cursor = db_conn.cursor(cursor_factory=pg_extras.DictCursor)
    cursor.execute("SELECT * from %s;" % table_name)
    return [d.name for d in cursor.description]


def get_jobs(db_conn):
    jobs_columns_names = get_table_columns_names(db_conn, "jobs")
    jobs_aliases = ["jobs.%s as jobs_%s" % (n, n) for n in jobs_columns_names]
    jobstates_columns_names = get_table_columns_names(db_conn, "jobstates")
    jobstates_aliases = [
        "jobstates.%s as jobstates_%s" % (n, n) for n in jobstates_columns_names
    ]
    files_columns_names = get_table_columns_names(db_conn, "files")
    files_aliases = ["files.%s as files_%s" % (n, n) for n in files_columns_names]
    query = "SELECT %s, %s, %s from JOBS INNER JOIN JOBSTATES on (JOBSTATES.job_id = JOBS.id) INNER JOIN FILES on (FILES.jobstate_id = JOBSTATES.id) ORDER BY JOBSTATES.created_at ASC;"
    query = query % (
        ", ".join(jobs_aliases),
        ", ".join(jobstates_aliases),
        ", ".join(files_aliases),
    )
    cursor = db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query)
    jobs = [dict(j) for j in cursor.fetchall()]
    cursor.close()
    return format(jobs)


def main():
    db_connection = get_db_connection()
    jobs = get_jobs(db_connection)

    for job in jobs:
        LOG.info("process job %s" % job["id"])
        task_duration_cumulated.process(job)


if __name__ == "__main__":

    # acquire an exclusive file lock
    lock_file = open("/tmp/dci-poller.lock", "w")
    try:
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # release the lock on exit
        atexit.register(lock_file.close)
        atexit.register(fcntl.lockf, lock_file, fcntl.LOCK_UN)
    except IOError:
        LOG.warn("poller instance already running, exit(0)")
        sys.exit(0)

    try:
        main()
    except Exception:
        LOG.error(traceback.format_exc())
    sys.exit(0)
