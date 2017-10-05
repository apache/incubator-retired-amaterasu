"""
Run an Amaterasu pipeline.
You have to have Mesos installed on the same machine where Amaterasu is installed to use this command.
IMPORTANT:
In the future we plan to enable remote execution, hence you will be required to connect to a cluster prior to executing this command

Usage: ama run <repository_url> [-e <env>] [-r <report>] [-b <branch>] [-j <job-id>] [-n <name>] [-f]

Options:
    -h --help               Show this screen
    -e --env=<env>          The environment to use for running this job [default: default]
    -r --report=<report>    Verbosity, this controls how much of the job's logs is propagated to the CLI [default: none]
    -b --branch=<branch>    What branch to use when running this job [default: master]
    -j --job-id=<job-id>    Provide a job-id to resume a paused job.
    -n --name=<name>        Provider a name for the job.
    -f --force-bin          Force deleting and re-creating the HDFS amaterasu folder
"""
from .. import common, consts, compat
from .base import MakiMixin, ValidateRepositoryMixin, BaseHandler, HandlerError, \
    PropertiesFile
from . import setup as config_handlers
from string import Template
import netifaces
import abc
import os
import socket
import uuid
import git


__version__ = '0.2.0-incubating-rc3'


class BaseRunPipelineHandler(BaseHandler, MakiMixin):

    cluster_manager = None

    def __init__(self, **args):
        super(BaseRunPipelineHandler, self).__init__(**args)
        self.props = PropertiesFile('~/.amaterasu/amaterasu.properties')
        self.base_dir = '/tmp/amaterasu/repos'
        self.dir_path = '{}/{}'.format(self.base_dir, uuid.uuid4())
        self.amaterasu_root = self.props['amaterasu.home']

    def _validate_repository(self):
        super(BaseRunPipelineHandler, self)._validate_repository()
        BaseRunPipelineHandler.load_maki(
            os.path.join(self.dir_path, 'maki.yml'))

    @abc.abstractmethod
    def _get_command_params(self):
        pass

    def handle(self):
        try:
            git.Repo.clone_from(self.args['repository_url'], self.dir_path)
            self._validate_repository()
            command_params = self._get_command_params()
            os.environ.setdefault('AWS_ACCESS_KEY_ID', "0")
            os.environ.setdefault('AWS_SECRET_ACCESS_KEY', "0")
            os.environ.setdefault('AMA_NODE', socket.gethostname())
            compat.run_subprocess(command_params, cwd=self.amaterasu_root)
            print('W00t amaterasu job is finished!!!')
        except git.GitError as e:
            raise HandlerError(inner_errors=[e])


class RunMesosPipelineHandler(BaseRunPipelineHandler, MakiMixin,  ValidateRepositoryMixin):
    """
    This handler takes care of starting up Amaterasu Scala process.
    First, we validate the inputs we get. The user is expected to pass at least the repository URL.
    We inspect the submitted repository and validate that it exists and fits the structure of a valid Amaterasu job repository
    If all validations are passed, we invoke the Scala runtime.
    """

    cluster_manager = 'mesos'

    def _get_command_params(self):
        command_params = [
            'java',
            '-cp',
            '{}/bin/leader-{}-all.jar'.format(self.amaterasu_root, __version__),
            "-Djava.library.path=/usr/lib",
            "org.apache.amaterasu.leader.mesos.MesosJobLauncher",
            "--home",
            self.amaterasu_root,
            "--repo",
            self.args['repository_url'],
            "--env",
            self.args.get('env', 'default'),
            "--report",
            self.args.get('report', 'code'),
            "--branch",
            self.args.get('branch', 'master'),
            "--config-home",
            self.args.get('config_home', os.path.expanduser("~/.amaterasu"))
        ]
        if self.args.get('job_id'):
            command_params.extend(["--job-id", self.args['job_id']])
        if self.args.get('name'):
            command_params.extend(["--name", self.args['name']])
        return command_params


class RunYarnPipelineHandler(BaseRunPipelineHandler, MakiMixin,  ValidateRepositoryMixin):

    cluster_manager = 'yarn'

    def _get_command_params(self):
        """
        yarn jar ${BASEDIR}/bin/leader-0.2.0-incubating-all.jar org.apache.amaterasu.leader.yarn.Client --home ${BASEDIR}
        :return:
        """
        command_params = [
            'yarn',
            'jar',
            '{}/bin/leader-{}-all.jar'.format(self.amaterasu_root, __version__),
            'org.apache.amaterasu.leader.yarn.Client',
            "--home",
            self.amaterasu_root,
            "--repo",
            self.args['repository_url'],
            "--env",
            self.args.get('env', 'default'),
            "--report",
            self.args.get('report', 'code'),
            "--branch",
            self.args.get('branch', 'master'),
            "--config-home",
            os.path.expanduser("~/.amaterasu")
        ]
        if self.args.get('job_id'):
            command_params.extend(["--job-id", self.args['job_id']])
        if self.args.get('name'):
            command_params.extend(["--name", self.args['name']])
        return command_params

    def handle(self):
        if self.args.get('force-bin', False):
            compat.run_subprocess(['hdfs', 'dfs', '-rm', '-R', '-skipTrash', self.props['yarn.jarspath']])
        return super().handle()


def _check_amaterasu_properties():
    supported_cluster_managers = ['mesos', 'yarn']
    if not os.path.exists(
        os.path.expanduser('~/.amaterasu/amaterasu.properties')):
        print('Amaterasu hasn\'t been configured yet, please fill in the following details:')
        cluster_manager = None
        while not cluster_manager:
            cluster_manager = input('Choose cluster manager [{}]:'.format(', '.join(supported_cluster_managers)))
            if cluster_manager.strip() not in supported_cluster_managers:
                print('Invalid cluster manager: {}'.format(cluster_manager))
                cluster_manager = None
        kwargs = {cluster_manager: True}
        handler = config_handlers.get_handler(**kwargs)
        handler()


def get_handler(**kwargs):
    try:
        props = PropertiesFile('~/.amaterasu/amaterasu.properties')
        cluster_manager = props['cluster.manager']
        if cluster_manager == 'mesos':
            return RunMesosPipelineHandler
        elif cluster_manager == 'yarn':
            return RunYarnPipelineHandler
        else:
            raise NotImplemented('Unsupported cluster manager: {}'.format(cluster_manager))
    except KeyError:
        raise HandlerError('cluster.manager is missing from configuration! Please run ama setup and try again.')
