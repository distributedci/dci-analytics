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

import flask

import json
import logging

from dci_analytics.api import api
from dci_analytics import dci_db
from dci.analytics import access_data_layer as a_d_l
from dci_analytics import elasticsearch as es
from datetime import datetime as dt

logger = logging.getLogger(__name__)


@api.route("/jobs", strict_slashes=False, methods=["GET"])
def get_jobs():
    latest_index_alias = es.get_latest_index_alias("jobs")
    if not latest_index_alias:
        return flask.Response(
            json.dumps({"message": "no alias for prefix index 'jobs' found"}),
            status=400,
            content_type="application/json",
        )
    values = flask.request.json
    _jobs = es.search_json(latest_index_alias, values)

    if "aggregations" not in _jobs:
        if "hits" not in _jobs:
            _jobs = {}
        elif "hits" not in _jobs["hits"]:
            _jobs = {}
        elif not _jobs["hits"]["hits"]:
            _jobs = {}

    _jobs["_meta"] = es.get_index_meta(latest_index_alias)
    return flask.Response(
        json.dumps(_jobs),
        status=200,
        content_type="application/json",
    )


@api.route("/jobs/syncstate", strict_slashes=False, methods=["GET"])
def get_syncstate():
    latest_index_alias = es.get_latest_index_alias("jobs")
    if not latest_index_alias:
        return flask.Response(
            json.dumps({"message": "no alias for prefix index 'jobs' found"}),
            status=400,
            content_type="application/json",
        )

    def _get_first_es_job_timestamp():
        first_es_job_query = {
            "size": 1,
            "fields": ["created_at"],
            "sort": [{"created_at": {"order": "asc"}}],
        }
        first_es_job = es.search_json(latest_index_alias, first_es_job_query)
        if not first_es_job or not first_es_job["hits"]["hits"]:
            return flask.Response(
                json.dumps({"message": "no jobs first es job found"}),
                status=400,
                content_type="application/json",
            )
        return dt.fromisoformat(
            first_es_job["hits"]["hits"][0]["_source"]["updated_at"]
        )

    first_es_job_timestamp = _get_first_es_job_timestamp()

    session_db = dci_db.get_session_db()
    limit = 1000
    offset = 0

    es_jobs_not_found = []
    jobs_ids_from_dci_db = set()
    while True:
        jobs_ids = a_d_l.get_jobs_ids_from_timestamp(
            session_db, offset, limit, first_es_job_timestamp
        )
        if not jobs_ids:
            break
        es_jobs = es.mget(latest_index_alias, jobs_ids)
        es_jobs_not_found.extend(
            [j["_id"] for j in es_jobs["docs"] if j["found"] is False]
        )

        jobs_ids_from_dci_db = jobs_ids_from_dci_db.union(set(jobs_ids))
        offset += limit

    return flask.Response(
        json.dumps(
            {
                "jobs_from_dci_db": {"length": len(jobs_ids_from_dci_db)},
                "es_jobs_not_found": es_jobs_not_found,
            }
        ),
        status=200,
        content_type="application/json",
    )


@api.route("/jobs/autocomplete", strict_slashes=False, methods=["GET"])
def get_jobs_autocompletion():
    latest_index_alias = es.get_latest_index_alias("jobs")
    if not latest_index_alias:
        return flask.Response(
            json.dumps({"message": "no alias for prefix index 'jobs' found"}),
            status=400,
            content_type="application/json",
        )
    values = flask.request.json
    if "field" not in values and "team_id" not in values:
        return flask.Response(
            json.dumps({"message": "'field' or 'team_id' parameters missing."}),
            status=400,
            content_type="application/json",
        )
    field = values["field"]
    team_id = values["team_id"]
    is_admin = values.get("is_admin", False)
    size = values.get("size", 10)

    autocompletion_values = es.get_autocompletion_values(
        latest_index_alias, team_id, field, is_admin, size
    )

    return flask.Response(
        json.dumps(autocompletion_values),
        status=200,
        content_type="application/json",
    )
