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
from dci_analytics import elasticsearch as es

logger = logging.getLogger(__name__)

_INDEX = "tasks_duration_cumulated"
_DEFAULT_OFFSET = 0
_DEFAULT_LIMIT = 20
_MAX_LIMIT = 200

_EMPTY_HITS = {"total": {"value": 0, "relation": "eq"}, "max_score": None, "hits": []}


def handle_pagination(values):
    offset = int(values.get("offset", _DEFAULT_OFFSET))
    limit = min(int(values.get("limit", _DEFAULT_LIMIT)), _MAX_LIMIT)
    return offset, limit


def build_query(topic_id, remoteci_id, offset, limit):
    return {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
                "must": [
                    {"term": {"topic_id": topic_id}},
                    {"term": {"remoteci_id": remoteci_id}},
                ]
            }
        },
        "sort": [{"created_at": {"order": "desc"}}],
    }


@api.route("/duration_cumulated", strict_slashes=False, methods=["GET"])
def get_duration_cumulated():
    values = flask.request.args
    if "topic_id" not in values or "remoteci_id" not in values:
        return flask.Response(
            json.dumps(
                {"message": "'topic_id' and 'remoteci_id' parameters are required."}
            ),
            status=400,
            content_type="application/json",
        )

    topic_id = values["topic_id"]
    remoteci_id = values["remoteci_id"]
    offset, limit = handle_pagination(values)

    query = build_query(topic_id, remoteci_id, offset, limit)
    res = es.search_json(_INDEX, query)
    hits = res.get("hits", _EMPTY_HITS)

    return flask.Response(
        json.dumps(hits),
        status=200,
        content_type="application/json",
    )
