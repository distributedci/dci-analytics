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

_INDEX = "tasks_components_coverage"
_DEFAULT_TEAM_ID = "red_hat"
_SIZE = 10000

_EMPTY_HITS = {"total": {"value": 0, "relation": "eq"}, "max_score": None, "hits": []}


def build_query(topic_id, team_id, types):
    query = {
        "size": _SIZE,
        "query": {
            "bool": {
                "must": [
                    {"term": {"topic_id": topic_id}},
                    {"term": {"team_id": team_id}},
                ]
            }
        },
        "sort": [
            {
                "released_at": {
                    "order": "desc",
                    "format": "strict_date_optional_time_nanos",
                }
            }
        ],
    }

    if types:
        bool_should = {"bool": {"should": []}}
        for t in types:
            bool_should["bool"]["should"].append({"term": {"type": t}})
        query["query"]["bool"]["must"].append(bool_should)
    else:
        # returns only one unique component for each type (with latest first)
        query["collapse"] = {"field": "type"}

    return query


@api.route("/components_coverage", strict_slashes=False, methods=["GET"])
def get_components_coverage():
    values = flask.request.args
    if "topic_id" not in values:
        return flask.Response(
            json.dumps({"message": "'topic_id' parameter is required."}),
            status=400,
            content_type="application/json",
        )

    topic_id = values["topic_id"]
    team_id = values.get("team_id") or _DEFAULT_TEAM_ID
    types = flask.request.args.getlist("types")

    query = build_query(topic_id, team_id, types)
    res = es.search_json(_INDEX, query)
    hits = res.get("hits", _EMPTY_HITS)

    return flask.Response(
        json.dumps(hits),
        status=200,
        content_type="application/json",
    )
