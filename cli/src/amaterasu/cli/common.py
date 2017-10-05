import os
from argparse import ArgumentParser, _SubParsersAction
from multipledispatch import dispatch


class User:
    name = None
    email = None

    def __init__(self, name, email):
        self.name = name
        self.email = email


class AmaterasuArgumentParser(ArgumentParser):
    """
    We use our own parser here so our handlers can expose a cleaner registration API.
    """

    def __init__(self, name=None, **kwargs):
        if not name:
            raise TypeError("Missing keyword argument 'name'")
        self.name = name
        self.arguments = []
        self.kwargs = kwargs
        super(AmaterasuArgumentParser, self).__init__(**kwargs)


    def add_argument(self, *args, **kwargs):
        self.arguments.append((args, kwargs))
        return super(AmaterasuArgumentParser, self).add_argument(*args, **kwargs)

    def add_subparsers(self, **kwargs):
        return super(AmaterasuArgumentParser, self).add_subparsers(action=AmaterasuSubParsers, **kwargs)


class AmaterasuSubParsers(_SubParsersAction):

    @dispatch(object, object)
    def add_parser(self, arg, **kwargs):
        super(AmaterasuSubParsers, self).add_parser(arg, **kwargs)

    @dispatch(AmaterasuArgumentParser)
    def add_parser(self, ama_parser):
        self._name_parser_map[ama_parser.name] = ama_parser


class Resources(dict):
    BASE_DIR = '{}/resources'.format(os.path.dirname(__file__))

    def __init__(self, path=None):
        super(Resources, self).__init__()
        if path:
            self.BASE_DIR = '{}/resources'.format(os.path.abspath(path))
        for (_, _, files) in os.walk(self.BASE_DIR):
            for f in files:
                with open('{}/{}'.format(self.BASE_DIR, f), 'r') as fd:
                    if f != 'banner2.txt':
                        self[f] = fd.read()
                    else:
                        self[f] = fd.readlines()


RESOURCES = Resources()
