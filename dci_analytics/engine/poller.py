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

from dci_analytics.engine.workers import task_duration_cumulated
from dciclient.v1.api import context
from dciclient.v1.api import job
from dciclient.v1.api import jobs_events as dci_jobs_events

import atexit
import fcntl
import logging
import sys
import traceback


LOG = logging.getLogger(__name__)

formatter = logging.Formatter('%(levelname)s - %(message)s')
streamhandler = logging.StreamHandler(stream=sys.stdout)
streamhandler.setFormatter(formatter)
LOG.addHandler(streamhandler)
LOG.setLevel(logging.DEBUG)


def get_current_sequence(dci_context):
    seq = dci_jobs_events.get_sequence(dci_context)
    if seq.status_code != 200:
        raise Exception('http error code=%s, message=%s, while getting '
                        'sequence number' % (seq.status_code, seq.text))
    return seq.json()['sequence']


def get_jobs_events(dci_context, sequence):
    je = dci_jobs_events.list(dci_context, sequence + 1)
    if je.status_code != 200:
        raise Exception('http error code=%s, message=%s, while getting '
                        'jobs events' % (je.status_code, je.text))
    return je.json()['jobs_events']


def get_job(dci_context, job_id):
    j = job.get(dci_context, job_id)
    if j.status_code != 200:
        raise Exception('http error code=%s, message=%s, while getting '
                        'job' % (j.status_code, j.text))
    return j.json()['job']


def main(dci_context):
    sequence = get_current_sequence(dci_context)
    jobs_events = get_jobs_events(dci_context, sequence['sequence'])
    last_job_event = None

    if not jobs_events:
        LOG.info('no new jobs found to process')

    for job_event in jobs_events:
        LOG.info('process job %s' % job_event['job_id'])
        current_job = get_job(dci_context, job_event['job_id'])
        last_job_event = job_event
        task_duration_cumulated.process(dci_context, current_job)

    if last_job_event:
        dci_jobs_events.update_sequence(dci_context,
                                        sequence['etag'],
                                        last_job_event['id'])


if __name__ == '__main__':
    dci_context = context.build_dci_context()

    # acquire an exclusive file lock
    lock_file = open('/tmp/dci-poller.lock', 'w')
    try:
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # release the lock on exit
        atexit.register(lock_file.close)
        atexit.register(fcntl.lockf, lock_file, fcntl.LOCK_UN)
    except IOError:
        LOG.warn('poller instance already running, exit(0)')
        sys.exit(0)

    try:
        main(dci_context)
    except Exception:
        LOG.error(traceback.format_exc())
    sys.exit(0)
