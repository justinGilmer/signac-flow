# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from __future__ import print_function
import logging

from .manage import Scheduler, JobStatus

logger = logging.getLogger(__name__)


class FakeScheduler(Scheduler):

    def jobs(self):
        return
        yield

    def submit(self, script, *args, **kwargs):
        print('submit', args)
        for key, value in kwargs.items():
            if value is not None:
                print("#FAKE {}={}".format(key, value))
        for line in script:
            print(line, end='')
        return JobStatus.submitted
