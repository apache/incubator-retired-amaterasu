import os
import sys

from behave import *
from hamcrest import *
from unittest import mock
from amaterasu.cli import common
from amaterasu.cli.handlers.base import HandlerError
from amaterasu.cli.handlers.init import InitRepositoryHandler
from tests.utils import collect_stats
import git

@given('The relative path "{path}"')
def step_impl(context, path):
    """
    :type context: behave.runner.Context
    """
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        os.makedirs(abs_path, exist_ok=True)
    context.given_path = abs_path


@given('The absolute path "{path}"')
def step_impl(context, path):
    """
    :type context: behave.runner.Context
    """
    context.given_path = path


@when("InitRepository handler is invoked with the given path")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    try:
        with mock.patch('amaterasu.cli.handlers.init.InitRepositoryHandler._config_user', return_value=common.User('Naruto Uzumaki', 'naruto@konoha.village')):
            handler = InitRepositoryHandler(path=context.given_path)
            handler.handle()
        collect_stats(context, context.given_path)
    except HandlerError as ex:
        context.ex = ex


@then('A directory with path "{expected_path}" should be created')
def step_impl(context, expected_path):
    """
    :type context: behave.runner.Context
    """
    abs_path = os.path.abspath(expected_path)
    path_exists = os.path.exists(abs_path)
    assert_that(path_exists, is_(True))


@step('The directory in path "{expected_repo_path}" should be a git repository')
def step_impl(context, expected_repo_path):
    """
    :type context: behave.runner.Context
    """
    abs_path = os.path.abspath(expected_repo_path)
    git_meta_path = os.path.join(abs_path, '.git')
    repo_exists = os.path.exists(git_meta_path)
    assert_that(repo_exists, is_(True))


@step('The "{expected_path}" directory should have a "{expected_file}" file')
def step_impl(context, expected_path, expected_file):
    """
    :type context: behave.runner.Context
    """
    abs_path = os.path.abspath(expected_path)
    file_path = os.path.join(abs_path, expected_file)
    file_exists = os.path.exists(file_path)
    assert_that(file_exists, is_(True))


@step('The "{expected_path}" directory should have a "{expected_subdir}" subdirectory')
def step_impl(context, expected_path, expected_subdir):
    """
    :type context: behave.runner.Context
    """
    abs_path = os.path.abspath(expected_path)
    subdir_path = os.path.join(abs_path, expected_subdir)
    subdir_exists = os.path.exists(subdir_path)
    assert_that(subdir_exists, is_(True))


@then("An HandlerError should be raised")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    assert_that(context, has_property('ex', instance_of(HandlerError)))


@given("The path is a repository")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    git.Repo.init(context.given_path)


@given('The "{given_path}" directory has a "{given_subdir}" subdirectory')
def step_impl(context, given_path, given_subdir):
    """
    :type context: behave.runner.Context
    """
    abs_path = os.path.abspath(given_path)
    subdir_path = os.path.join(abs_path, given_subdir)
    os.makedirs(subdir_path, exist_ok=True)


@given('The "{given_path}" directory has a "{given_file}" file')
def step_impl(context, given_path, given_file):
    """
    :type context: behave.runner.Context
    """
    abs_path = os.path.abspath(given_path)
    file_path = os.path.join(abs_path, given_file)
    with open(file_path, 'w'):
        pass
    stat = os.lstat(file_path)
    context.stats_before[file_path] = stat

@given('The invalid absolute path "{given_path}"')
def step_impl(context, given_path):
    """
    :type context: behave.runner.Context
    """
    if sys.platform == 'win32':
        given_path = 'xxxzzz:\\{}'.format(given_path)
    context.given_path = given_path


@step('Only "{expected_changed_files_str}" should have changed')
def step_impl(context, expected_changed_files_str):
    """
    :type context: behave.runner.Context
    """
    expected_changed_files = [fname.strip() for fname in expected_changed_files_str.split(',')]
    for fname in expected_changed_files:
        path = os.path.abspath(os.path.join(context.given_path, fname))
        after = context.stats_after[path]
        before = context.stats_before.get(path, None)
        if before:
            assert_that(before.st_mtime, is_not(equal_to(after.st_mtime)))

    expected_unchanged_files = set(context.stats_before) - set(expected_changed_files)
    for fname in expected_unchanged_files:
        path = os.path.abspath(os.path.join(context.given_path, fname))
        after = context.stats_after[path]
        before = context.stats_before.get(path, None)
        assert_that(before.st_mtime, is_(equal_to(after.st_mtime)), path)
