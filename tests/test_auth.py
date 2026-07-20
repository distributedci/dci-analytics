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

import datetime

import jwt
import pytest

from dci.auth import encode_service_jwt
from dci_analytics import config
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


def _auth_headers(token):
    return {"Authorization": "Bearer %s" % token}


def _valid_token():
    return encode_service_jwt(JWT_SECRET, JWT_AUDIENCE, JWT_ISSUER, ttl_seconds=300)


def test_ok_without_token(client, jwt_config):
    response = client.get("/ok")
    assert response.status_code == 200


def test_protected_endpoint_without_token(client, jwt_config):
    response = client.get("/jobs")
    assert response.status_code == 401
    assert response.get_json()["message"] == "Authorization header missing"


def test_protected_endpoint_with_invalid_token(client, jwt_config):
    response = client.get("/jobs", headers=_auth_headers("invalid-token"))
    assert response.status_code == 401
    assert response.get_json()["message"].startswith("JWT token decode error:")


def test_protected_endpoint_with_expired_token(client, jwt_config):
    now = datetime.datetime.now(datetime.timezone.utc)
    expired_token = jwt.encode(
        {
            "iat": now - datetime.timedelta(seconds=600),
            "exp": now - datetime.timedelta(seconds=300),
            "aud": JWT_AUDIENCE,
            "iss": JWT_ISSUER,
            "sub": "dci-control-server",
        },
        JWT_SECRET,
        algorithm="HS256",
    )
    response = client.get("/jobs", headers=_auth_headers(expired_token))
    assert response.status_code == 401
    assert response.get_json()["message"] == "JWT token expired, please refresh."


def test_protected_endpoint_with_valid_token(client, jwt_config):
    response = client.get("/jobs", headers=_auth_headers(_valid_token()))
    assert response.status_code != 401


def test_protected_endpoint_with_other_service_token(client, jwt_config):
    now = datetime.datetime.now(datetime.timezone.utc)
    token = jwt.encode(
        {
            "iat": now,
            "exp": now + datetime.timedelta(seconds=300),
            "aud": JWT_AUDIENCE,
            "iss": "other-service",
            "sub": "other-service",
        },
        JWT_SECRET,
        algorithm="HS256",
    )
    response = client.get("/jobs", headers=_auth_headers(token))
    assert response.status_code != 401
