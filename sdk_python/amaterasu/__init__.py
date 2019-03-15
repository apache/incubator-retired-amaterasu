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
import pkg_resources
import sys

from .runtime import BaseAmaContext, conf, ImproperlyConfiguredError


class PluginProxy:

    def __init__(self, entry_point):
        self.entry_point = entry_point
        self.module = None

    def __getattr__(self, item):
        module = super(PluginProxy, self).__getattribute__('module')
        if not module:
            entry_point = super(PluginProxy, self).__getattribute__('entry_point')
            module = entry_point.load()
            super(PluginProxy, self).__setattr__('module', module)
        return getattr(module, item)



plugins = {
    entry_point.name: PluginProxy(entry_point)
    for entry_point
    in pkg_resources.iter_entry_points('amaterasu.plugins')
}


__all__ = ['BaseAmaContext', 'conf', 'ImproperlyConfiguredError']

thismodule = sys.modules[__name__]
for plugin_name, plugin_proxy in plugins.items():
    setattr(thismodule, plugin_name, plugin_proxy)
    module_name = 'amaterasu.{}'.format(plugin_name)
    sys.modules[module_name] = plugin_proxy
    __all__.append(plugin_name)