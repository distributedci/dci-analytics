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


from lxml import etree

from dci.analytics import access_data_layer as a_d_l

from dci_analytics.engine import elasticsearch as es
from dci_analytics.engine import dci_db

from dciclient.v1.api import context
from dciclient.v1.api import file as dci_file

import logging


logger = logging.getLogger(__name__)


def junit_to_dict(junit):
    res = dict()
    try:
        root = etree.fromstring(junit)
        for testsuite in root.findall("testsuite"):
            for tc in testsuite:
                key = "%s/%s" % (tc.get("classname"), tc.get("name"))
                key = key.strip()
                key = key.replace(",", "_")
                if tc.get("time"):
                    res[key] = float(tc.get("time"))
                else:
                    res[key] = -1.0
    except etree.XMLSyntaxError as e:
        logger.error("XMLSyntaxError %s" % str(e))
        return {}
    return res


def get_file_content(api_conn, f):
    r = dci_file.content(api_conn, f["id"])
    r.text


def _process(job, api_conn):
    files = []
    for f in job["files"]:
        if f["mime"] == "application/junit":
            file_content = get_file_content(api_conn, f)
            f["junit_content"] = junit_to_dict(file_content)
            files.append(f)
    job["files"] = files
    es.push("tasks_junit", job, job["id"])


def _sync(unit, amount):
    session_db = dci_db.get_session_db()
    api_conn = context.build_dci_context()
    limit = 100
    offset = 0
    while True:
        jobs = a_d_l.get_jobs(session_db, offset, limit, unit=unit, amount=amount)
        if not jobs:
            break
        for job in jobs:
            logger.info("process job %s" % job["id"])
            _process(api_conn, job)
        offset += limit

    session_db.close()


def synchronize(_lock_synchronization):
    _sync("hours", 2)
    _lock_synchronization.release()


def full_synchronize(_lock_synchronization):
    _sync("weeks", 24)
    _lock_synchronization.release()
