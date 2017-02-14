# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Define environments to automate submission scripts."""

from __future__ import print_function
import re
import socket
import logging
import io
import math
from signac.common.six import with_metaclass
from . import scheduler
from . import manage


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
    scheduler=None
    hostname_pattern = None

    class JobScript(io.StringIO):
        "Simple StringIO wrapper to implement cmd wrapping logic."

        def __init__(self, parent, _id, serial=False, eol='\n'):
            self._parent = parent
            self._serial = serial
            self._eol = eol
            super().__init__()

        def writeline(self, line):
            "Write one line to the job script."
            self.write(line + self._eol)

        def write_cmd(self, cmd, np=1):
            """Write a command to the jobscript.

            This command wrapper function is a convenience function, which
            adds mpi and other directives whenever necessary.

            :param cmd: The command to write to the jobscript.
            :type cmd: str
            :param np: The number of processors required for execution.
            :type np: int
            """
            if np > 1:
                cmd = self._parent.mpi_cmd(cmd, np=np)
            if not self._serial:
                cmd += ' &'
            self.writeline(cmd)

        def submit(self, *args, **kwargs):
            self.writeline('wait')
            self.seek(0)
            return self._parent.submit(self, *args, **kwargs)

    @classmethod
    def is_present(cls):
        if cls.hostname_pattern is None:
            return False
        else:
            return re.match(
                cls.hostname_pattern, socket.gethostname()) is not None

    @classmethod
    def get_scheduler(cls):
        try:
            return getattr(cls, 'scheduler_type')()
        except AttributeError:
            raise AttributeError("You must define a scheduler type for every environment")


    @classmethod
    def submit(cls, script, *args, **kwargs):
        # Hand off the actual submission to the scheduler
        return cls.get_scheduler().submit(script, *args, **kwargs)

    @classmethod
    def script(cls, _id, nn=None, np=None, ppn=None, serial=False, **kwargs):
        return cls.JobScript(cls, _id, serial=serial)

    @staticmethod
    def bg(cmd):
        return cmd + ' &'


class UnknownEnvironment(ComputeEnvironment):
    scheduler_type = scheduler.FakeScheduler


class TestEnvironment(ComputeEnvironment):
    scheduler_type = scheduler.FakeScheduler


class MoabEnvironment(ComputeEnvironment):
    scheduler_type = scheduler.MoabScheduler


class SlurmEnvironment(ComputeEnvironment):
    scheduler_type = scheduler.SlurmScheduler


class CPUEnvironment(ComputeEnvironment):
    pass


class GPUEnvironment(ComputeEnvironment):
    pass


def get_environment(test=False):
    if test:
        return TestEnvironment
    else:
        for env_type in ComputeEnvironment.registry.values():
            if env_type.is_present():
                return env_type
        else:
            return UnknownEnvironment
