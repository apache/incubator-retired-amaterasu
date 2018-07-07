"""
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
__doc__ = """
{amaterasu_logo}

Usage: ama [--verbose] <command> [<args>...]

Builtin commands:
    init        Start a new Amaterasu compliant repository
    setup       Initial setup for Amaterasu
    update      Update an existing Amaterasu repository based on a maki file
    run         Run an Amaterasu job

Options:
    -V --verbose    Enable verbose output.

See 'ama <command> --help' for more detailed information.
"""
__version__ = '0.2.0-incubating-rc4'

import colorama
import pkgutil
import importlib
import logging
import logging.config
import os
import yaml
from .cli import common, consts, handlers
from docopt import docopt

ama_package_path = os.path.abspath(os.path.dirname(__file__))
logging_config_path = '{}/cli/resources/logging.yml'.format(ama_package_path)
with open(logging_config_path) as fp:
    logging_cfg = yaml.load(fp)
logging.config.dictConfig(logging_cfg)
logger = logging.getLogger(__name__)


colorama.init()
lines = []
for idx, line in enumerate(common.RESOURCES[consts.AMATERASU_LOGO]):
    if idx <= 7:
        lines.append("\033[38;5;202m" + line)
    elif 7 < idx < 14:
        lines.append("\033[38;5;214m" + line)
    else:
        lines.append("\033[38;5;220m" + line)
desc = ''.join(lines)
desc += colorama.Fore.RESET + '\n\n'
desc += common.RESOURCES[consts.APACHE_LOGO]
desc += common.RESOURCES[consts.AMATERASU_TXT]


def load_handlers():
    return {
        name.split('.')[-1]: importlib.import_module(name)
        for _, name, _
        in pkgutil.iter_modules(handlers.__path__, handlers.__name__ + ".")
        if not name.endswith('base')
    }


def extract_args(args):
    """
    Cleans docopt's output, collects the <arg> arguments, strips the "<" ">" and returns an equivalent dictionary
    :param args: docopt result arguments
    :type args: dict
    :return:
    """
    kwargs = {}
    for k,v in args.items():
        if k.startswith('--'):
            key = k.lstrip('--')
        elif k.startswith('<') and k.endswith('>'):
            key = k.strip('<').strip('>')
        else:
            key = k
        kwargs[key] = v
    return kwargs


def find_handler(handler_module, **kwargs):
    """
    Looks for a handler class that inherits from BaseHandler. We assume that the class with the longest MRO is
    the one we look for
    :param handler_module: A handler module loaded by importlib
    :return:
    """
    try:
        return handler_module.get_handler(**kwargs)
    except AttributeError:
        raise AttributeError("Module {} does not define a get_handler function".format(handler_module.__name__))


def main():
    doc = __doc__.format(amaterasu_logo=desc, additional_commands='')  # TODO: implement additional_commands
    root_args = docopt(doc, version=__version__, options_first=True)
    handler_modules = load_handlers()
    command = root_args['<command>']
    if root_args['--verbose']:
        logging.basicConfig(level=logging.DEBUG)

    logger.debug('CLI Started. Received the following arguments: {}'.format(root_args))
    if command in handler_modules:
        handler_vars = vars(handler_modules[command])
        cmd_args = docopt(handler_vars['__doc__'], version=handler_vars.get('__version__', __version__))
        handler = find_handler(handler_modules[command], **cmd_args)
        handler(**extract_args(cmd_args)).handle()
    else:
        logger.debug("Unrecognized command received: {}".format(command))
        print(doc)

if __name__ == '__main__':
    main()