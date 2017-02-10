# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define environments to automate submission scripts.

Partially adapted from clusterutils package by
Matthew Spellings."""

from __future__ import print_function
import re
import socket
import logging
import io
import math
from signac.common.six import with_metaclass
from . import scheduler


logger = logging.getLogger(__name__)

def format_timedelta(delta):
    hours, r = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(r, 60)
    hours += delta.days * 24
    return "{:0>2}:{:0>2}:{:0>2}".format(hours, minutes, seconds)

class ComputeEnvironmentType(type):

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = dict()
        else:
            cls.registry[name] = cls
        return super(ComputeEnvironmentType, cls).__init__(name, bases, dct)


class ComputeEnvironment(with_metaclass(ComputeEnvironmentType)):
    hostname_pattern = None

    def __init__(self, mode=None):
        if mode == None:
            mode = type(self).modes['MODE_CPU']
        if mode not in self.modes.values():
            raise ValueError(mode)
        self.mode = mode

    @classmethod
    def is_present(cls):
        if cls.hostname_pattern is None:
            return False
        else:
            return re.match(
                cls.hostname_pattern, socket.gethostname()) is not None

    def submit(self, jobsid, np, walltime, script, nn = None, ppn = None, test = False, db = None,
               *args, **kwargs):
        submit_script = io.StringIO()
        if nn is not None and ppn is not None:
            num_nodes = int(np / ppn) # We divide rather than taking nn directly to allow for bundled jobs
            if (np / (nn*type(self).available_cores_per_node[self.mode])) < 0.9:
                logger.warning("Bad node utilization!")
        else:
            num_nodes = math.ceil(np / type(self).available_cores_per_node[self.mode])
            if (np / (num_nodes * type(self).available_cores_per_node[self.mode])) < 0.9:
                logger.warning("Bad node utilization!")

        submit_script.write(type(self).headers[self.mode].format(
            jobsid=jobsid, nn=num_nodes, walltime=format_timedelta(walltime)))
        submit_script.write('\n')
        submit_script.write(script.read())
        submit_script.seek(0)
        if nn is not None and ppn is not None:
            # If the ppn argument is specified, we modify the job script to explicitly specify how many processors we want for each node. This is a bit hackish, but since ppn is a feature attached to nodes on Moab schedulers this is a reasonable solution
            submit = submit_script.read().format(
                np="{num_nodes}:ppn={ppn}".format(num_nodes=num_nodes, ppn=ppn), 
                nn=num_nodes, walltime=format_timedelta(walltime), jobsid=jobsid)
        else:
            submit = submit_script.read().format(
                np=np, nn=num_nodes,
                walltime=format_timedelta(walltime), jobsid=jobsid)

        # Hand off the actual submission to the scheduler
        scheduler = type(self).get_scheduler(test, db)
        return scheduler.submit(submit, *args, **kwargs)

    @classmethod
    def get_scheduler(cls, test = False, db = None):
        if test:
            if db is None:
                db = signac.get_database(str(signac.get_project()))
            return scheduler.APScheduler(db = db)
        try:
            sched = getattr(cls, 'scheduler_type')()
            return sched
        except AttributeError:
            raise AttributeError("You must define a scheduler type for every environment")

class UnknownEnvironment(ComputeEnvironment):
    pass


class TestEnvironment(ComputeEnvironment):
    pass


class MoabEnvironment(ComputeEnvironment):
    scheduler_type = scheduler.MoabScheduler


class SlurmEnvironment(ComputeEnvironment):
    scheduler_type = scheduler.SlurmScheduler

'''
class CPUEnvironment(ComputeEnvironment):
    pass


class GPUEnvironment(ComputeEnvironment):
    pass

'''

def get_environment():
    for env_type in ComputeEnvironment.registry.values():
        if env_type.is_present():
            return env_type
    else:
        return UnknownEnvironment
