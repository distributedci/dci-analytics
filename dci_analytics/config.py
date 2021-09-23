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

import logging
import yaml
import sys


def _get_log(LOG=None):
    if not LOG:
        LOG = logging.getLogger(__name__)
        formatter = logging.Formatter("%(levelname)s - %(message)s")
        streamhandler = logging.StreamHandler(stream=sys.stdout)
        streamhandler.setFormatter(formatter)
        LOG.addHandler(streamhandler)
        LOG.setLevel(logging.DEBUG)
    return LOG


def get_conf(LOG=None):

    LOG = _get_log(LOG)

    try:
        config_fd = open("/etc/dci-analytics.conf", "r")
        return yaml.load(config_fd)
    except IOError:
        LOG.error("Config file /etc/dci-elastic.conf not found.")
        sys.exit(1)
