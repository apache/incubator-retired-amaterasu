"""
Start a new Amaterasu repository at the given path
By default, uses PWD.

Usage:
    ama init [<path>]

Options:
    -h --help       Show this screen.

"""

from .base import BaseRepositoryHandler, git_parser, HandlerError
from .. import common
from ..repository import AmaRepository


class InitRepositoryHandler(BaseRepositoryHandler):
    """
    A handler for creating a new Amaterasu repository
    We generate the following structure:
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
    |
    |__ maki.yml ## The job definition
    """

    @staticmethod
    def _config_user():
        """
        First we try to get the user details from the global .gitconfig
        If we fail at that, then we will ask the user for his credentials
        :return:
        """
        try:
            username = git_parser.get('user', 'name')
        except KeyError:
            username = ''
        try:
            email = git_parser.get('user', 'email')
        except KeyError:
            email = ''

        new_name = input("Your name [{}]: ".format(username))
        if new_name == username == '':
            raise HandlerError('Username is required!')
        elif new_name == '':
            new_name = username

        new_email = input("Your email [{}]:".format(email))
        if new_email == email == '':
            raise HandlerError('Email is required!')
        elif new_email == '':
            new_email = email

        return common.User(new_name, new_email)

    def handle(self):
        print("Setting up an Amaterasu job repository at {}".format(self.dir_path))
        repo = AmaRepository(self.dir_path)
        repo.init_repo()
        repo.commit()
        print("Amaterasu job repository set up successfully")


def get_handler(**kwargs):
    return InitRepositoryHandler