# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Routines for the MOAB environment."""

from __future__ import print_function
import getpass
import subprocess
import tempfile
import logging
import xml.etree.ElementTree as ET

from .manage import Scheduler
from .manage import ClusterJob, JobStatus


logger = logging.getLogger(__name__)


def _fetch(user=None):
    if user is None:
        user = getpass.getuser()
    cmd = "qstat -fx -u {user}".format(user=user)
    try:
        result = io.BytesIO(subprocess.check_output(cmd.split()))
    except FileNotFoundError:
        raise RuntimeError("Moab not available.")
    tree = ET.parse(source=result)
    return tree.getroot()

class MoabJob(ClusterJob):

    def __init__(self, node):
        self.node = node

    def _id(self):
        return self.node.find('Job_Id').text

    def __str__(self):
        return str(self._id())

    def name(self):
        return self.node.find('Job_Name').text

    def status(self):
        job_state = self.node.find('job_state').text
        if job_state == 'R':
            return JobStatus.active
        if job_state == 'Q':
            return JobStatus.queued
        if job_state == 'C':
            return JobStatus.inactive
        if job_state == 'H':
            return JobStatus.held
        return JobStatus.registered


class MoabScheduler(Scheduler):
    submit_cmd = ['qsub']

    def __init__(self, root=None, user=None):
        self.user = user
        self.root = root

    def jobs(self):
        self._prevent_dos()
        nodes = _fetch(user=self.user)
        for node in nodes.findall('Job'):
            yield MoabJob(node)

    def submit(self, script,
               resume=None, after=None, pretend=False, hold=False, *args, **kwargs):
        if pretend:
            print("#\n# Pretend to submit:\n")
            print(script, "\n")
        else:
            submit_cmd = self.submit_cmd
            if after is not None:
                submit_cmd.extend(
                    ['-W', 'depend="afterok:{}"'.format(after.split('.')[0])])
            if hold:
                submit_cmd += ['-h']
            with tempfile.NamedTemporaryFile() as tmp_submit_script:
                tmp_submit_script.write(script.encode('utf-8'))
                tmp_submit_script.flush()
                output = subprocess.check_output(
                    submit_cmd + [tmp_submit_script.name])
            jobsid = output.decode('utf-8').strip()
            return jobsid
