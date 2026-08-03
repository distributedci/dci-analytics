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

import mock
import pytest

from dci.auth import encode_service_jwt
from dci_analytics import config
from dci_analytics.api import components_coverage
from dci_analytics.app import app

JWT_SECRET = "test-analytics-jwt-secret"
JWT_AUDIENCE = "dci-analytics"
JWT_ISSUER = "dci-control-server"


@pytest.fixture
def client():
    return app.test_client()


@pytest.fixture
def jwt_config(monkeypatch):
    monkeypatch.setitem(config.CONFIG, "ANALYTICS_JWT_SECRET", JWT_SECRET)
    monkeypatch.setitem(config.CONFIG, "ANALYTICS_JWT_AUDIENCE", JWT_AUDIENCE)
    monkeypatch.setitem(config.CONFIG, "ANALYTICS_JWT_TTL_SECONDS", 300)


def _auth_headers():
    token = encode_service_jwt(JWT_SECRET, JWT_AUDIENCE, JWT_ISSUER, ttl_seconds=300)
    return {"Authorization": "Bearer %s" % token}


def test_build_query_without_types_collapses_on_type():
    query = components_coverage.build_query("topic-1", "red_hat", [])
    assert query == {
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {"term": {"topic_id": "topic-1"}},
                    {"term": {"team_id": "red_hat"}},
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
        "collapse": {"field": "type"},
    }


def test_build_query_with_types_adds_should_filter():
    query = components_coverage.build_query("topic-1", "red_hat", ["ocp", "f5-spk"])
    assert "collapse" not in query
    assert query["query"]["bool"]["must"][2] == {
        "bool": {
            "should": [
                {"term": {"type": "ocp"}},
                {"term": {"type": "f5-spk"}},
            ]
        }
    }


def test_get_components_coverage_missing_params(client, jwt_config):
    res = client.get("/components_coverage", headers=_auth_headers())
    assert res.status_code == 400


@mock.patch("dci_analytics.api.components_coverage.es.search_json")
def test_get_components_coverage(mock_search_json, client, jwt_config):
    mock_search_json.return_value = {
        "hits": {
            "total": {"value": 1, "relation": "eq"},
            "max_score": 1.0,
            "hits": [{"_id": "component-1", "_source": {"id": "component-1"}}],
        }
    }

    res = client.get(
        "/components_coverage?topic_id=topic-1",
        headers=_auth_headers(),
    )

    assert res.status_code == 200
    assert res.get_json()["hits"][0]["_id"] == "component-1"

    mock_search_json.assert_called_once_with(
        "tasks_components_coverage",
        components_coverage.build_query("topic-1", "red_hat", []),
    )


@mock.patch("dci_analytics.api.components_coverage.es.search_json")
def test_get_components_coverage_with_team_and_types(
    mock_search_json, client, jwt_config
):
    mock_search_json.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    res = client.get(
        "/components_coverage?topic_id=topic-1&team_id=team-1&types=ocp&types=f5-spk",
        headers=_auth_headers(),
    )

    assert res.status_code == 200
    mock_search_json.assert_called_once_with(
        "tasks_components_coverage",
        components_coverage.build_query("topic-1", "team-1", ["ocp", "f5-spk"]),
    )
