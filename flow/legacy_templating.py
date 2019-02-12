# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Implements API and code paths for the legacy templating system.

Will be removed beginning version 0.8
"""
import warnings
import functools

# Global variable that is used internally to keep track of which
# FlowProject methods belong to the legacy templating system. Such
# a method is docorated with the _part_of_legacy_template_system()
# decorator and then registered in this variable.
_LEGACY_TEMPLATING_METHODS = set()


def _part_of_legacy_template_system(method):
    "Label a method to be part of the legacy templating system."
    _LEGACY_TEMPLATING_METHODS.add(method.__name__)
    method._legacy_intact = True
    return method


class FlowProjectLegacyTemplatingSystem(object):

    def __init__(self, *args, **kwargs):
        super(FlowProjectLegacyTemplatingSystem, self).__init__(*args, **kwargs)
        self._setup_legacy_templating()

    def _setup_legacy_templating(self):
        """This function identifies whether a subclass has implemented deprecated template
        functions.

        The legacy templating system is used to generate run and cluster submission scripts
        if that is the case. A warning is emitted to inform the user that they will not be
        able to use the standard templating system.

        The legacy templating functions are decorated with the _part_of_legacy_template_system()
        decorator.
        """
        self._legacy_templating = False
        legacy_methods = set()
        for method in _LEGACY_TEMPLATING_METHODS:
            if hasattr(self, method) and not hasattr(getattr(self, method), '_legacy_intact'):
                warnings.warn(
                    "The use of FlowProject method '{}' is deprecated!".format(method),
                    DeprecationWarning)
                legacy_methods.add(method)
        if legacy_methods:
            self._legacy_templating = True
            warnings.warn(
                "You are using the following deprecated templating methods: {}. Please remove "
                "those methods from your project class implementation to use the jinja2 templating "
                "system (version >= 0.6).".format(', '.join(legacy_methods)))

    @_part_of_legacy_template_system
    def write_script_header(self, script, **kwargs):
        """"Write the script header for the execution script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        # Add some whitespace
        script.writeline()
        # Don't use uninitialized environment variables.
        script.writeline('set -u')
        # Exit on errors.
        script.writeline('set -e')
        # Switch into the project root directory
        script.writeline('cd {}'.format(self.root_directory()))
        script.writeline()

    @_part_of_legacy_template_system
    def write_script_operations(self, script, operations, background=False, **kwargs):
        """Write the commands for the execution of operations as part of a script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        from .util.misc import write_human_readable_statepoint

        for op in operations:
            write_human_readable_statepoint(script, op.job)
            script.write_cmd(op.cmd.format(job=op.job), bg=background)
            script.writeline()

    @classmethod
    @_part_of_legacy_template_system
    def write_human_readable_statepoint(cls, script, job):
        """Write statepoint of job in human-readable format to script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        raise RuntimeError(
            "The write_human_readable_statepoint() function has been deprecated "
            "as of version 0.6 and been removed as of version 0.7.")

    @_part_of_legacy_template_system
    def write_script_footer(self, script, **kwargs):
        """"Write the script footer for the execution script.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.
        """
        # Wait until all processes have finished
        script.writeline('wait')

    @_part_of_legacy_template_system
    def write_script(self, script, operations, background=False, **kwargs):
        """Write a script for the execution of operations.

        .. deprecated:: 0.6
           Users should migrate to the new templating system.

        By default, this function will generate a script with the following components:

        .. code-block:: python

            write_script_header(script)
            write_script_operations(script, operations, background=background)
            write_script_footer(script)

        :param script:
            The script to write the commands to.
        :param operations:
            The operations to be written to the script.
        :type operations:
            A sequence of JobOperation
        :param background:
            Whether operations should be executed in the background;
            useful to parallelize execution.
        :type background:
            bool
        """
        self.write_script_header(script, **kwargs)
        self.write_script_operations(script, operations, background=background, **kwargs)
        self.write_script_footer(script, **kwargs)


def script_support_legacy_templating_system(func):

    @functools.wraps(func)
    def wrapper(self, operations, parallel=False, template='script.sh', *args, **kwargs):
        if self._legacy_templating:
            import os
            from .environment import TestEnvironment
            # We first check whether it appears that the user has provided a templating script
            # in which case we raise an exception to avoid highly unexpected behavior.
            fn_template = os.path.join(self.root_directory(), 'templates', template)
            if os.path.isfile(fn_template):
                raise RuntimeError(
                    "In legacy templating mode, unable to use template '{}'.".format(fn_template))
            script = TestEnvironment.script()
            self.write_script(script, operations, background=parallel)
            script.seek(0)
            return script.read()
        else:
            return func(self, operations=operations, parallel=parallel, template=template,
                        *args, **kwargs)

    return wrapper


def _generate_submit_script_support_legacy_templating_system(func):

    @functools.wraps(func)
    def wrapper(self, _id, operations, template, show_template_help, env, *args, **kwargs):
        import os
        if self._legacy_templating:
            fn_template = os.path.join(self._template_dir, template)
            if os.path.isfile(fn_template):
                raise RuntimeError(
                    "In legacy templating mode, unable to use template '{}'.".format(fn_template))

            # For maintaining backwards compatibility for Versions<0.7
            if kwargs['parallel']:
                np_total = sum(op.directives['np'] for op in operations)
            else:
                np_total = max(op.directives['np'] for op in operations)

            script = env.script(_id=_id, np_total=np_total, **kwargs)
            background = kwargs.pop('parallel', not kwargs.pop('serial', False))
            self.write_script(script=script, operations=operations, background=background, **kwargs)
            script.seek(0)
            return script.read()
        else:
            return func(self, _id, operations, template, show_template_help, env, *args, **kwargs)

    return wrapper
