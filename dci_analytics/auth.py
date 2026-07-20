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
import jwt
from jwt import exceptions as jwt_exc

from dci_analytics import config
from dci_analytics import exceptions


def decode_service_jwt(token):
    cfg = config.CONFIG
    return jwt.decode(
        token,
        cfg["ANALYTICS_JWT_SECRET"],
        algorithms=["HS256"],
        audience=cfg["ANALYTICS_JWT_AUDIENCE"],
        options={"verify_iss": False},
    )


def authenticate_request():
    cfg = config.CONFIG
    if not cfg["ANALYTICS_JWT_SECRET"]:
        raise exceptions.DCIException("JWT not configured", status_code=401)

    auth_header = flask.request.headers.get("Authorization")
    if not auth_header:
        raise exceptions.DCIException("Authorization header missing", status_code=401)

    parts = auth_header.split(" ", 1)
    if len(parts) != 2 or parts[0] != "Bearer":
        auth_type = parts[0] if parts else ""
        raise exceptions.DCIException(
            "Authorization scheme %s unknown" % auth_type, status_code=401
        )

    try:
        decode_service_jwt(parts[1])
    except jwt_exc.ExpiredSignatureError:
        raise exceptions.DCIException(
            "JWT token expired, please refresh.", status_code=401
        )
    except jwt_exc.InvalidAudienceError:
        raise exceptions.DCIException("JWT token audience invalid", status_code=401)
    except jwt_exc.DecodeError as e:
        raise exceptions.DCIException(
            "JWT token decode error: %s" % str(e), status_code=401
        )
    except jwt_exc.InvalidTokenError:
        raise exceptions.DCIException("JWT token invalid", status_code=401)
