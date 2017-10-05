import abc
import os
import sys
import yaml
from six.moves import configparser

__version__ = '0.2.0-incubating-rc3'

git_parser = configparser.ConfigParser()
git_parser.read(os.path.expanduser('~/.gitconfig'))


class HandlerError(Exception):
    def __init__(self, *args, **kwargs):
        inner_errors = kwargs.get('inner_errors', [])
        if inner_errors:
            message = 'Encountered the following errors: \r\n'
            for error in inner_errors:
                message += '{}: {}\r\n'.format(type(error).__name__, str(error))
            super(HandlerError, self).__init__(message)
        else:
            super(HandlerError, self).__init__(*args)


class ValidationError(Exception):
    pass


class BaseHandler(abc.ABC):
    """
    The CLI handlers detection -
    We offer our own set of handlers, but in case you'd like to extend the CLI for your own needs, we offer that as well.
    So this idea was basically taken from Django's management commands idea. All you need to do is subclass BaseHandler
    in one way or another.

    At runtime, we look for all the subclasses of BaseHandler and look for ones that implement the **handle** method.
    If the handler has implemented the **handle** method, we then proceed to get its parser.
    We mount all the parsers we find on the root Amaterasu parser defined in __main__.py

    TL;DR - To create a handler of your own:
     1. Subclass BaseHandler (or any of its subclasses)
     2. Implement handle
     3. Implement get_parser
    """

    def __init__(self, **args):
        self.args = args

    @abc.abstractmethod
    def handle(self):
        """
        This is where the magic happens. Write the handling logic here!
        :return:
        """
        pass


class BaseRepositoryHandler(BaseHandler):

    def __init__(self, **args):
        super(BaseRepositoryHandler, self).__init__(**args)
        path = args['path'] or os.getcwd()
        self.dir_path = path if os.path.isabs(path) else os.path.abspath(path)
        self._validate_path()

    def _validate_path(self):
        root_dir_exists = os.path.exists(self.dir_path)
        if not root_dir_exists:
            base_path = os.path.split(self.dir_path)[0]
            if not os.path.exists(base_path):
                raise HandlerError("The base path: \"{}\" doesn't exist!".format(base_path))


class MakiMixin(object):
    """
    A mixin that takes care of loading and validating a maki file.
    Multiple handlers require this logic, e.g. - UpdateRepositoryHandler, RunPipelineHandler
    and possibly any handler that has something to do with the maki file.
    Please add only shared maki related code here, it is not the place for handler specific code!
    I will personally kick your ass if you do.
    Regardless, Sasuke sucks.
    """

    @staticmethod
    def _validate_maki(maki):
        """
        A valid maki looks like the following:

                job-name:    amaterasu-test [REQUIRED]
                flow: [REQUIRED]
                --  - name: start [REQUIRED]
                |     runner: [REQUIRED]
                |         group: spark [REQUIRED]
       (1..n)  -|         type: scala [REQUIRED]
                |     file: file.scala [REQUIRED]
                |     exports: [OPTIONAL]
                --        odd: parquet
                    - name: step2
                      runner:
                          group: spark
                          type: scala
                      file: file2.scala
        :param maki:
        :return:
        """

        def str_ok(x):
            str_type = str if sys.version_info[0] > 2 else unicode
            return type(x) == str_type and len(x) > 0

        VALID_GROUPS = ['spark']
        VALID_TYPES = ['scala', 'sql', 'python', 'r']

        if not maki:
            raise HandlerError('Empty maki supplied')
        first_level_ok = 'job-name' in maki and 'flow' in maki
        if not first_level_ok:
            raise HandlerError('Invalid maki!')
        job_name_ok = str_ok(maki['job-name'])
        flow_ok = type(maki['flow']) == list and len(maki['flow']) > 0
        flow_steps_ok = True
        for step in maki['flow']:
            step_name_ok = lambda: 'name' in step and str_ok(step['name'])
            step_runner_ok = lambda: 'runner' in step and type(step['runner']) == dict \
                                     and 'group' in step['runner'] and str_ok(step['runner']['group']) \
                                     and step['runner']['group'] in VALID_GROUPS \
                                     and 'type' in step['runner'] and str_ok(step['runner']['type']) \
                                     and step['runner']['type'] in VALID_TYPES
            file_ok = lambda: 'file' in step and str_ok(step['file'])
            step_ok = type(step) == dict and step_name_ok() and step_runner_ok() and file_ok()
            if not step_ok:
                flow_steps_ok = False
                break
        return job_name_ok and flow_ok and flow_steps_ok

    @staticmethod
    def load_maki(maki_path):
        with open(maki_path, 'r') as f:
            maki = yaml.load(f)
        MakiMixin._validate_maki(maki)
        return maki


class ValidateRepositoryMixin(object):
    """
    We need valid repositories as inputs for the Amaterasu pipeline.
    A valid repository looks like this:

    /root_dir
    |__ /src ## This is where the source code resides
    |    |
    |    |__ task1.scala
    |    |
    |    |__ task2.py
    |    |
    |    |__ task3.sql
    |
    |__ /env ## This is a configuration directory for each environment the user defines, there should be a "default" env.
    |    |
    |    |__ /default
    |    |   |
    |    |   |__ job.yml
    |    |   |
    |    |   |__ spark.yml
    |    |
    |    |__ /test
    |    |
    |    |__ /<some other env>
    |
    |__ maki.yml ## The job definition
    """

    def _validate_repository(self):
        src_path = os.path.join(self.dir_path, 'src')
        env_path = os.path.join(self.dir_path, 'env')
        default_env_path = os.path.join(self.dir_path, 'env', 'default')
        errors = []
        print(src_path, env_path, default_env_path)
        if not os.path.exists(src_path):
            errors.append(ValidationError('Repository has no src directory'))
        if not os.path.exists(env_path):
            errors.append(ValidationError('Repository has no env directory'))
        if not os.path.exists(default_env_path):
            errors.append(ValidationError('Repository has no env/default directory'))
        if errors:
            raise HandlerError(inner_errors=errors)


class PropertiesFile(dict):

    def __init__(self, path, **kwargs) -> None:
        abs_path = os.path.expanduser(path) if path.startswith('~') else os.path.abspath(path)
        self.path = abs_path
        with open(abs_path, 'r') as f:
            for i, line in enumerate(f.read().splitlines()):
                try:
                    parts = line.split('=')
                    if len(parts) > 2:
                        var = parts[0]
                        value = '='.join(parts[1:])
                    else:
                        var, value = parts
                    self[var.strip()] = value.strip()
                except ValueError:
                    print('Improperly Configured: bad form of line {} in amaterasu.properties'.format(i))

        super().__init__(**kwargs)