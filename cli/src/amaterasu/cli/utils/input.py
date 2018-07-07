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

import readline


def default_input(prompt, default=''):
    """
    This prints the default value next to the prompt and the value is editable.
    Important note!
    The default value cannot be displayed in the Mac OSX default shell.
    You can bypass this by installing zsh on Mac OSX.
    As far as tested on
    :param prompt:
    :param default:
    :return:
    """
    readline.set_startup_hook(lambda: readline.insert_text(default))
    try:
        return input(prompt)
    finally:
        readline.set_startup_hook()