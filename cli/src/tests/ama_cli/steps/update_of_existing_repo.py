import os

import yaml
from behave import *
from hamcrest import *
from unittest import mock

from amaterasu.cli.handlers.base import HandlerError
from amaterasu.cli.handlers.update import UpdateRepositoryHandler
from tests.utils import collect_stats


@when("Updating the repository using the maki file")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    try:
        handler = UpdateRepositoryHandler(path=context.given_path)
        handler.handle()
        collect_stats(context, context.given_path)
    except HandlerError as ex:
        context.ex = ex


@given('The "{directory}" directory has an empty maki file')
def step_impl(context, directory):
    """
    :type context: behave.runner.Context
    """
    maki_path = os.path.join(context.given_path, 'maki.yml')
    with open(maki_path, 'w'):
        pass


@given('The "{directory}" directory has an invalid maki file')
def step_impl(context, directory):
    """
    :type context: behave.runner.Context
    """
    maki_path = os.path.join(context.given_path, 'maki.yml')
    resources = context.test_resources
    with open(maki_path, 'w') as f:
        f.write(resources['maki_invalid.yml'])


@given('The "{directory}" directory has another invalid maki file')
def step_impl(context, directory):
    """
    :type context: behave.runner.Context
    """
    maki_path = os.path.join(context.given_path, 'maki.yml')
    resources = context.test_resources
    with open(maki_path, 'w') as f:
        f.write(resources['maki_invalid2.yml'])


@given('The "{directory}" directory has a valid maki file')
def step_impl(context, directory):
    """
    :type context: behave.runner.Context
    """
    maki_path = os.path.join(context.given_path, 'maki.yml')
    resources = context.test_resources
    with open(maki_path, 'w') as f:
        f.write(resources['maki_valid.yml'])


@then('"{filename}" should be added to the maki file')
def step_impl(context, filename):
    """
    :type context: behave.runner.Context
    """
    maki_path = os.path.join(context.given_path, 'maki.yml')
    with open(maki_path, 'r') as f:
        maki = yaml.load(f)
    source_files = [step['file'] for step in maki['flow']]
    assert_that(source_files, contains(filename))


@step('The "{directory}" shouldn\'t have a "{filename}" file')
def step_impl(context, directory, filename):
    """
    :type context: behave.runner.Context
    """
    full_file_path = os.path.abspath(os.path.join(directory, filename))
    assert_that(os.path.exists(full_file_path), is_not(True))


@then("An HandlerError should not be raised")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    assert_that(context, is_not(has_property('ex')), "An exception was raised while invoking the handler")


@when("Updating the repository using the maki file, with user keeping source files that are not in the maki")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    try:
        with mock.patch('amaterasu.cli.handlers.update.UpdateRepositoryHandler._get_user_input_for_source_not_on_maki', return_value='kA'):
            handler = UpdateRepositoryHandler(path=context.given_path)
            handler.handle()
        collect_stats(context, context.given_path)
    except HandlerError as ex:
        context.ex = ex


@when("Updating the repository using the maki file, with user not keeping source files that are not in the maki")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    try:
        with mock.patch('amaterasu.cli.handlers.update.UpdateRepositoryHandler._get_user_input_for_source_not_on_maki', return_value='dA'):
            handler = UpdateRepositoryHandler(path=context.given_path)
            handler.handle()
        collect_stats(context, context.given_path)
    except HandlerError as ex:
        context.ex = ex


@step('The "{directory}" directory shouldn\'t have a "{filename}" file')
def step_impl(context, directory, filename):
    """
    :type context: behave.runner.Context
    """
    file_path = os.path.join(os.path.abspath(directory), filename)
    assert_that(os.path.exists(file_path), is_(False))



@when('Updating the repository using the maki file, with user not keeping "{file_to_delete}" and is keeping "{file_to_keep}"')
def step_impl(context, file_to_delete, file_to_keep):
    """
    :type context: behave.runner.Context
    """

    def mock_user_input(handler, source):
        if source == file_to_delete:
            return 'd'
        else:
            return 'k'

    try:
        with mock.patch('amaterasu.cli.handlers.update.UpdateRepositoryHandler._get_user_input_for_source_not_on_maki', new=mock_user_input):
            handler = UpdateRepositoryHandler(path=context.given_path)
            handler.handle()
        collect_stats(context, context.given_path)
    except HandlerError as ex:
        context.ex = ex