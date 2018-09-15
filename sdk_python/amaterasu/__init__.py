import pkg_resources
import sys

from .runtime import BaseAmaContext, conf, notifier, ImproperlyConfiguredError, _LazyProxy


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


__all__ = ['BaseAmaContext', 'conf', 'notifier', 'ImproperlyConfiguredError']

thismodule = sys.modules[__name__]
for plugin_name, plugin_proxy in plugins.items():
    setattr(thismodule, plugin_name, plugin_proxy)
    module_name = 'amaterasu.{}'.format(plugin_name)
    sys.modules[module_name] = plugin_proxy
    __all__.append(plugin_name)