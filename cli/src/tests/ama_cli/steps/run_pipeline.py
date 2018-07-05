from functools import partial
from unittest import mock
from behave import *
from hamcrest import *

from amaterasu.cli import consts
from amaterasu.cli.handlers.base import HandlerError
from amaterasu.cli.handlers.run import RunMesosPipelineHandler, RunYarnPipelineHandler
from uuid import uuid4

import os
import git



def mock_git_clone(uid, context, url, dest_dir):
    repository_dest = os.path.abspath('/tmp/amaterasu/repos/{}'.format(uid))
    if url == 'http://git.sunagakure.com/ama-job-non-exist.git':
        raise git.GitError("failed to send request: The server name or address could not be resolved")
    elif url == "http://git.sunagakure.com/ama-job-valid.git":
        os.makedirs(repository_dest, exist_ok=True)
        os.makedirs(os.path.join(repository_dest, 'src'), exist_ok=True)
        os.makedirs(os.path.join(repository_dest, 'env'), exist_ok=True)
        os.makedirs(os.path.join(repository_dest, 'env', 'default'), exist_ok=True)
        with open(os.path.join(repository_dest, 'maki.yml'), 'w') as maki:
            maki.write(context.test_resources['maki_valid.yml'])
        with open(os.path.join(repository_dest, 'env', 'default', consts.SPARK_CONF), 'w') as spark:
            spark.write(context.test_resources[consts.SPARK_CONF])
        with open(os.path.join(repository_dest, 'env', 'default', consts.JOB_FILE), 'w') as job:
            job.write(context.test_resources[consts.JOB_FILE])

    elif url == 'http://git.sunagakure.com/some-repo.git':
        os.makedirs(repository_dest)
        os.mkdir(os.path.join(repository_dest, 'sasuke'))
        os.mkdir(os.path.join(repository_dest, 'sasuke', 'is'))
        os.mkdir(os.path.join(repository_dest, 'sasuke', 'is', 'lame'))  # (NOT) TODO: Na nach nachman me oman
    else:
        raise NotImplementedError()


def mock_subprocess_run(context, cmd, cwd):
    context.command = cmd


@given("A valid repository")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.repository_uri = 'http://git.sunagakure.com/ama-job-valid.git'


@when("Running a pipeline on Mesos with the given repository")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    uid = uuid4()
    with mock.patch('git.Repo.clone_from', partial(mock_git_clone, uid, context)), \
         mock.patch('uuid.uuid4', lambda: uid), \
         mock.patch('amaterasu.cli.compat.run_subprocess', partial(mock_subprocess_run, context)):
        handler = RunMesosPipelineHandler(repository_url=context.repository_uri, env='default', name=None, report='code', branch='master', job_id=None, config_home='/tmp/amaterasu')
        handler.amaterasu_root = '/tmp/amaterasu/assets'
        os.makedirs(handler.amaterasu_root, exist_ok=True)
        try:
            handler.handle()
        except HandlerError as ex:
            context.ex = ex


@given("A valid file URI repository")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.repository_uri = 'http://git.sunagakure.com/ama-job-valid.git'


@given("A repository that doesn't exist")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.repository_uri = 'http://git.sunagakure.com/ama-job-non-exist.git'


@given("A repository that is not Amaterasu compliant")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.repository_uri = 'http://git.sunagakure.com/some-repo.git'


@then("Amaterasu should run")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    pass


@given("It is the first time the user runs a pipeline")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.first_run = True


@step("The resulting command looks like this")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    command = ' '.join(context.command)
    assert_that(command, is_(equal_to(context.text)))


@when("Running a pipeline on YARN with the given repository")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    uid = uuid4()
    with mock.patch('git.Repo.clone_from',
                    partial(mock_git_clone, uid, context)), \
         mock.patch('uuid.uuid4', lambda: uid), \
         mock.patch('amaterasu.cli.compat.run_subprocess',
                    partial(mock_subprocess_run, context)):
        handler = RunYarnPipelineHandler(repository_url=context.repository_uri,
                                          env='default', name=None,
                                          report='code', branch='master',
                                          job_id=None,
                                          config_home='/tmp/amaterasu')
        handler.amaterasu_root = '/tmp/amaterasu/assets'
        os.makedirs(handler.amaterasu_root, exist_ok=True)
        try:
            handler.handle()
        except HandlerError as ex:
            context.ex = ex