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


from datetime import datetime as dt

from dci.analytics import access_data_layer as a_d_l
from dci_analytics.engine import elasticsearch as es
from dci_analytics.engine import dci_db
from dci_analytics import config
from dciclient.v1.api import context
from dciclient.v1.api import remoteci

import logging


logger = logging.getLogger(__name__)


def _get_job_type(job):
    for t in job["tags"]:
        if t.startswith("job-type:"):
            return t.split(":")[1]
    return "NA"


def _get_plugins(job):
    plugins_names = {"f5-bigip", "f5-spk", "f5-aspenmesh", "netapp-trident"}
    plugins = {}
    for p in job["components"]:
        if p["type"] in plugins_names:
            plugins[p["type"]] = {"job_id": job["id"],
                                  "status": job["status"],
                                  "name": _get_component_name_by_type(job, p["type"])}
    return plugins


def _get_pipeline_name(job):
    for t in job["tags"]:
        if t.startswith("pipeline:"):
            return t.split(":")[1]
    return "NA"


def _get_component_name_by_type(job, ctype):
    for c in job["components"]:
        if c["type"] == ctype:
            return c["canonical_project_name"]


def _process(job):
    if job["status"] not in {"success", "failure"}:
        return
    tags = job["tags"]
    if "daily" not in tags:
        logger.info("not a daily job")
        return

    # date-pipeline-name
    pipeline_name = _get_pipeline_name(job)
    _job_date = job["created_at"].split("T")[0]
    doc_id = "%s-%s" % (_job_date, pipeline_name)
    doc = es.get("telco_dashboard", doc_id)

    data = {}
    if doc:
        data = doc
    job_type = _get_job_type(job)
    if "fallback" in tags and job_type == "ocp":
        if 'ocp' in data:
            data["ocp"]["fallback"] = job["id"]
    elif job_type == "ocp":
        _ocp = {
                 "ocp_component": _get_component_name_by_type(job, "ocp"),
                 "plugins": _get_plugins(job)}
        data["ocp"] = _ocp
    elif job_type == "ocp-upgrade":
        data.update(
            {
                "upgrade": job["id"]
            }
        )
    elif job_type == "cnf":
        if "job:example-cnf" in job["tags"]:
            name = _get_component_name_by_type(job, "nfv-example-cnf-deploy")
            data["example-cnf"] = {"name": name, "job-id": job["id"], "status": job["status"]}
        elif "job:fake-cnf" in job["tags"]:
            data["webapp"] = {"name": job["status"], "job-id": job["id"], "status": job["status"]}
        elif "job:tnf-test-cnf" in job["tags"]:
            name = _get_component_name_by_type(job, "cnf-certification-test")
            data["test-net-function"] = {"name": name, "job-id": job["id"], "status": job["status"]}
        elif "job:preflight" in job["tags"]:
            data["preflight"] = {"name": job["status"], "job-id": job["id"], "status": job["status"]}
    else:
        logger.error("unknown job type: %s" % job_type)

    data["pipeline-name"] = pipeline_name
    data["pipeline-duration"] =  0

    logger.info("data %s" % str(data))

    if doc:
        es.update("telco_dashboard", data, doc_id)
    else:
        es.push("telco_dashboard", data, doc_id)


def _get_remoteci_id(name):
    _config = config.get_config()
    api_conn = context.build_dci_context(
        dci_login=_config["DCI_LOGIN"],
        dci_password=_config["DCI_PASSWORD"],
        dci_cs_url=_config["DCI_CS_URL"],
    )
    r = remoteci.list(api_conn, where="name:%s" % name)
    if r.status_code != 200:
        logger.error("could not find remoteci '%s'" % name)
        return
    return r.json()["remotecis"][0]["id"]


def _sync(unit, amount):
    session_db = dci_db.get_session_db()
    limit = 100
    offset = 0
    for remoteci_name in ("f5-dallas", "telco-rh-cnf", "rh-dallas"):
        remoteci_id = _get_remoteci_id(remoteci_name)
        if not remoteci_id:
            logger.error("could not find remoteci '%s'" % "rh-dallas")
            continue

        while True:
            jobs = a_d_l.get_jobs(
                session_db,
                offset,
                limit,
                unit=unit,
                amount=amount,
                remoteci_id=remoteci_id
            )
            if not jobs:
                break
            for job in jobs:
                logger.info("process job %s" % job["id"])
                try:
                    _process(job)
                except Exception as e:
                    logger.error(
                        "error while processing job '%s': %s" % (job["id"], str(e))
                    )
            offset += limit

    session_db.close()


def synchronize(_lock_synchronization):
    _sync("hours", 6)
    _lock_synchronization.release()


def full_synchronize(_lock_synchronization):
    _sync("weeks", 24)
    _lock_synchronization.release()
