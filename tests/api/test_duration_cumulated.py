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
from dci_analytics.api import duration_cumulated
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


def test_handle_pagination_defaults():
    offset, limit = duration_cumulated.handle_pagination({})
    assert offset == 0
    assert limit == 20


def test_handle_pagination_caps_limit():
    offset, limit = duration_cumulated.handle_pagination(
        {"offset": "5", "limit": "500"}
    )
    assert offset == 5
    assert limit == 200


def test_build_query():
    query = duration_cumulated.build_query("topic-1", "remoteci-1", 0, 20)
    assert query == {
        "from": 0,
        "size": 20,
        "query": {
            "bool": {
                "must": [
                    {"term": {"topic_id": "topic-1"}},
                    {"term": {"remoteci_id": "remoteci-1"}},
                ]
            }
        },
        "sort": [{"created_at": {"order": "desc"}}],
    }


def test_get_duration_cumulated_missing_params(client, jwt_config):
    res = client.get("/duration_cumulated", headers=_auth_headers())
    assert res.status_code == 400


@mock.patch("dci_analytics.api.duration_cumulated.es.search_json")
def test_get_duration_cumulated(mock_search_json, client, jwt_config):
    mock_search_json.return_value = {
        "hits": {
            "total": {"value": 1, "relation": "eq"},
            "max_score": 1.0,
            "hits": [{"_id": "job-1", "_source": {"job_id": "job-1"}}],
        }
    }

    res = client.get(
        "/duration_cumulated?topic_id=topic-1&remoteci_id=remoteci-1",
        headers=_auth_headers(),
    )

    assert res.status_code == 200
    assert res.get_json()["hits"][0]["_id"] == "job-1"

    mock_search_json.assert_called_once_with(
        "tasks_duration_cumulated",
        duration_cumulated.build_query("topic-1", "remoteci-1", 0, 20),
    )


@mock.patch("dci_analytics.api.duration_cumulated.es.search_json")
def test_get_duration_cumulated_no_hits(mock_search_json, client, jwt_config):
    mock_search_json.return_value = {}

    res = client.get(
        "/duration_cumulated?topic_id=topic-1&remoteci_id=remoteci-1",
        headers=_auth_headers(),
    )

    assert res.status_code == 200
    assert res.get_json()["hits"] == []
