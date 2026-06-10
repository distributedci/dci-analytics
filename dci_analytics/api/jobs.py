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
import threading

from dci_analytics.api import api
from dci_analytics import dci_db
from dci.analytics import access_data_layer as a_d_l
from dci_analytics import elasticsearch as es
from datetime import datetime as dt

logger = logging.getLogger(__name__)


SYNC_STATE_LOCK = threading.Lock()


def lock_and_run(lock, func):
    if lock.acquire(blocking=False):
        threading.Thread(
            target=func,
            daemon=True,
            args=(lock,),
        ).start()
        return flask.Response(
            json.dumps(
                {
                    "message": "Run syncstate",
                }
            ),
            status=201,
            content_type="application/json",
        )
    else:
        return flask.Response(
            json.dumps(
                {
                    "message": "Already running syncstate, please try later",
                }
            ),
            status=400,
            content_type="application/json",
        )


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


def _get_syncstate(lock):
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
            "fields": ["updated_at"],
            "sort": [{"updated_at": {"order": "asc"}}],
        }
        first_es_job = es.search_json(latest_index_alias, first_es_job_query)
        if not first_es_job or not first_es_job["hits"]["hits"]:
            return None

        return dt.fromisoformat(
            first_es_job["hits"]["hits"][0]["_source"]["updated_at"]
        )

    first_es_job_timestamp = _get_first_es_job_timestamp()
    if not first_es_job_timestamp:
        return flask.Response(
            json.dumps({"message": "no jobs first es job found"}),
            status=400,
            content_type="application/json",
        )

    session_db = dci_db.get_session_db()
    limit = 1000
    offset = 0

    jobs_ids_from_dci_db = set()
    while True:
        jobs_ids = a_d_l.get_jobs_ids_from_timestamp(
            session_db, offset, limit, first_es_job_timestamp
        )
        if not jobs_ids:
            break
        jobs_ids_from_dci_db.update(set(jobs_ids))
        offset += limit

    jobs_ids_from_es = set()
    es_page_size = 100
    es_query = {
        "size": es_page_size,
        "_source": ["id"],
        "sort": [
            {
                "updated_at": {
                    "order": "asc",
                    "format": "strict_date_optional_time_nanos",
                }
            },
            {"id": {"order": "asc"}},
        ],
        "search_after": [first_es_job_timestamp.isoformat(), ""],
    }
    while True:
        res = es.search_json(latest_index_alias, es_query)
        hits = res.get("hits", {}).get("hits", [])
        if not hits:
            break
        jobs_ids_from_es.update({str(j["_source"]["id"]) for j in hits})
        es_query["search_after"] = hits[-1]["sort"]

    es_jobs_not_found = list(jobs_ids_from_dci_db - jobs_ids_from_es)
    db_jobs_not_found = list(jobs_ids_from_es - jobs_ids_from_dci_db)

    es.update(
        "syncstate",
        {
            "jobs_from_dci_db": {"length": len(jobs_ids_from_dci_db)},
            "jobs_from_es": {"length": len(jobs_ids_from_es)},
            "es_jobs_not_found": es_jobs_not_found,
            "db_jobs_not_found": db_jobs_not_found,
        },
        "syncstate",
    )


@api.route("/jobs/syncstate", strict_slashes=False, methods=["POST"])
def syncstate():
    try:
        return lock_and_run(SYNC_STATE_LOCK, _get_syncstate)
    except Exception as e:
        logger.error(f"Error getting syncstate: {e}")
        return flask.Response(
            json.dumps({"message": "Error getting syncstate"}),
            status=500,
            content_type="application/json",
        )
    finally:
        SYNC_STATE_LOCK.release()


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
