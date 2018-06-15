"""
Create or change Amaterasu's configuration.

Usage:
    ama [-V] setup ( mesos | yarn )

"""
from string import Template
from typing import Any

from ..utils.input import default_input
from .base import BaseHandler, ConfigurationFile
import netifaces
import os
import abc
import socket
import getpass
import wget
import colorama
import logging
import pyarrow

logger = logging.getLogger(__name__)

__version__ = '0.2.0-incubating-rc3'

def get_current_ip():
    logger.debug("Trying to get the current IP")
    default_gateway = netifaces.gateways()['default']
    if default_gateway:
        netface_name = default_gateway[netifaces.AF_INET][1]
        ip = netifaces.ifaddresses(netface_name)[netifaces.AF_INET][0][
            'addr']
    else:
        ip = '127.0.0.1'
    logging.debug("Resulting IP is: {}".format(ip))
    return ip

def get_zookeeper_ip():
    logger.debug('Trying to retrieve the cluster\'s ZK instance IP')
    try:
        zoo_cfg = ConfigurationFile('/etc/zookeeper/conf/zoo.conf')
    except IOError:
        zoo_cfg = ConfigurationFile('/usr/local/etc/zookeeper/zoo.cfg') ### Mac OSX
    finally:
        logger.debug('ZK configuration: {}'.format(zoo_cfg.items()))
        try:
            server_addr = zoo_cfg.startswith('server')[0][1] ## Because we may have multiple, we will take the first one
            return server_addr.split(':')[0]
        except IndexError:
            return 'localhost'

class ValidationError(Exception):
    pass


class ConfigurationField(metaclass=abc.ABCMeta):

    def __init__(self, required=False, input_text=None, default=None, name=None) -> None:
        self.required = required
        self.input_text = input_text
        self._default = default
        self.name = name
        self._handler = None
        self._varname = None

    def _find_field_name_on_handler(self):
        if self._varname:
            return self._varname
        else:
            for k, v in self._handler._fields.items():
                if v == self:
                    self._varname = k
                    return k

    def clean(self, value: Any) -> Any:
        logger.debug('Field: \'{}\' received value of {}'.format(self._find_field_name_on_handler(), value))
        cleaned_value = value
        if not cleaned_value and cleaned_value != 0 and self._default is not None:
            try:
                default_func = getattr(self._handler, str(self.default))
                cleaned_value = default_func()
            except AttributeError:
                cleaned_value = self.default
                logger.debug('Field: \'{}\' used default value'.format(
                    self._find_field_name_on_handler()))
        if self.required and cleaned_value is None:
            logger.error('\'{}\' is a required field and no value could be determined'.format(self._find_field_name_on_handler()))
            raise ValidationError('This field is required')
        cleaned_value = cleaned_value or value
        logger.debug('Field: \'{}\' cleaned value is {}'.format(
            self._find_field_name_on_handler(), cleaned_value))
        return cleaned_value

    @property
    def default(self):
        if callable(self._default):
            value = self._default()
        else:
            value = self._default
        return value


class TextField(ConfigurationField):

    def clean(self, value):
        cleaned_value = super().clean(value)
        if str not in type(cleaned_value).mro():
            return str(cleaned_value)
        else:
            return cleaned_value


class NumericField(ConfigurationField):

    def clean(self, value):
        cleaned_value = super().clean(value)
        if int not in type(cleaned_value).mro() and float not in type(cleaned_value).mro():
            try:
                int_val = int(cleaned_value)
                float_val = float(cleaned_value)
                if int_val != float_val:
                    return float_val
                else:
                    return int_val
            except:
                raise ValidationError("Value must be a number")
        else:
            return cleaned_value


class IPField(TextField):

    def clean(self, value):
        cleaned_value = super().clean(value)
        try:
            socket.inet_aton(cleaned_value)
            return cleaned_value
        except:
            raise ValidationError('Value must be a valid IP address')


class PathField(TextField):



    def __init__(self, required=False, input_text=None, default=None,
                 name=None, auto_create=False) -> None:
        self.auto_create = auto_create
        super().__init__(required, input_text, default, name)

    def clean(self, value):
        cleaned_value = super().clean(value)
        if not os.path.isabs(cleaned_value):
            cleaned_value = os.path.expanduser(cleaned_value) if cleaned_value.startswith('~') else os.path.abspath(cleaned_value)
        if os.path.exists(cleaned_value):
            return cleaned_value
        elif self.auto_create:
            os.makedirs(os.path.abspath(os.path.expanduser(cleaned_value)))
            return cleaned_value
        else:
            raise ValidationError(
                'Path "{}" does not exist'.format(cleaned_value))


class ConfigurationMeta(abc.ABCMeta):

    @staticmethod
    def find_fields_for_cls(fields):
        vars_map = {}
        for var, value in fields.items():
            if ConfigurationField in value.__class__.mro():
                vars_map[var] = value
        return vars_map

    def __new__(mcls, name, bases, namespace, **kwargs):
        vars_map = {}
        cls = super().__new__(mcls, name, bases, namespace, **kwargs)
        vars_map.update(ConfigurationMeta.find_fields_for_cls(vars(cls)))
        for base in bases:
            vars_map.update(ConfigurationMeta.find_fields_for_cls(vars(base).get('_fields', {})))
        setattr(cls, '_fields', vars_map)
        for var in vars_map:
            setattr(cls, var, None)
        return cls


class BaseConfigurationHandler(BaseHandler, metaclass=ConfigurationMeta):

    cluster_manager = None
    amaterasu_home = PathField(input_text='Amaterasu home directory', default='/ama', name='amaterasu.home', auto_create=True)
    zk = TextField(required=True, input_text='Zookeeper IP', default=get_zookeeper_ip)

    user = TextField(required=True, default=getpass.getuser())
    spark_version = TextField(default='2.2.1-bin-hadoop2.7',
                              input_text='Spark version', name='spark.version')
    spark_home = TextField(default='spark_home_default', input_text='Path to spark\'s distributable', name='spark.home')

    @property
    @abc.abstractmethod
    def spark_home_default(self):
        pass

    def __new__(cls, *args, **kwargs) -> Any:
        instance = super().__new__(cls)
        for field in instance._fields.values():
            field._handler = instance
        return instance

    def __init__(self, **args):
        if os.path.exists(self.CONFIGURATION_PATH):
            prop_file = ConfigurationFile(self.CONFIGURATION_PATH)
            for var_name, field in self._fields.items():
                field_name = field.name if field.name else var_name
                setattr(self, var_name, prop_file.get(field_name))

        super().__init__(**args)

    def _get_user_input_for_field(self, var_name: str, field: ConfigurationField):
        input_tpl = Template('$input_text $default:')
        valid = False
        while not valid:
            if field.input_text:
                input_string = Template(input_tpl.safe_substitute(input_text=field.input_text))
            else:
                input_string = Template(input_tpl.safe_substitute(input_text=var_name))
            if field.default:
                try:
                    default_func = getattr(self, str(field.default))
                    default_val = default_func()
                except AttributeError:
                    default_val = field.default
                input_string = input_string.safe_substitute(
                    default='[{}]'.format(default_val))
            else:
                input_string = input_string.safe_substitute(default='')
            if getattr(self, var_name):
                value = default_input(input_string, getattr(self, var_name))
            else:
                value = input(input_string)
            try:
                cleaned_value = field.clean(value)
                valid = True
            except ValidationError as e:
                print('{}. Please try again'.format(e))
        return cleaned_value

    def _collect_user_input(self):
        for var_name, field in self._fields.items():
            value = self._get_user_input_for_field(var_name, field)
            setattr(self, var_name, value)

    def _render_configuration_file(self):
        config_file = ConfigurationFile(self.CONFIGURATION_PATH)
        config_file['cluster.manager'] = self.cluster_manager
        config_file['version'] = __version__
        for var_name, field_cls in self._fields.items():
            field_name = field_cls.name if field_cls.name else var_name
            value = str(getattr(self, var_name)).strip('"')
            if '=' in value:
                value = '"{}"'.format(value)
            config_file[field_name] = value
        config_file.save()
        logger.info("Successfully created Apache Amaterasu configuration file")

    def _download_dependencies(self):
        miniconda_dist_path = os.path.join(self.amaterasu_home, 'dist', 'Miniconda2-latest-Linux-x86_64.sh')
        if not os.path.exists(miniconda_dist_path):
            print('\n', colorama.Style.BRIGHT, 'Fetching Miniconda distributable', colorama.Style.RESET_ALL)
            wget.download(
                'https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh',
                out=miniconda_dist_path
            )


    def handle(self):
        self._collect_user_input()
        self._render_configuration_file()
        self._download_dependencies()


class MesosConfigurationHandler(BaseConfigurationHandler):
    master = IPField(required=True, input_text='Mesos master IP',
                     default=get_current_ip)
    amaterasu_port = NumericField(default=8000, input_text='Amaterasu server port', name='webserver.port')
    amaterasu_root = TextField(default='dist', input_text='Amaterasu server root path', name='webserver.root')
    cluster_manager = 'mesos'

    def spark_home_default(self):
        return './spark-{}'.format(self.spark_version)

    def _download_dependencies(self):
        super()._download_dependencies()
        spark_dist_path = os.path.join(self.amaterasu_home, 'dist',
                                       'spark-{}.tgz'.format(
                                           self.spark_version))
        if not os.path.exists(spark_dist_path):
            print(colorama.Style.BRIGHT, 'Fetching Spark distributable', colorama.Style.RESET_ALL)
            spark_url = 'http://apache.mirror.digitalpacific.com.au/spark/spark-{}/spark-{}.tgz'.format(self.spark_version.split('-')[0], self.spark_version)
            wget.download(
                spark_url,
                out=spark_dist_path
            )


class YarnConfigurationHandler(BaseConfigurationHandler):

    yarn_queue = TextField(default='default', input_text='YARN queue name', name='yarn.queue')
    yarn_jarspath = TextField(default='/apps/amaterasu', name='yarn.jarspath')
    spark_home = TextField(default='/usr/hdp/current/spark2-client', name='spark.home')
    yarn_homedir = TextField(default='/etc/hadoop', name='yarn.hadoop.home.dir')
    spark_yarn_java_opts = TextField(default='-Dhdp.version=2.6.1.0-129', name='spark.opts.spark.yarn.am.extraJavaOptions')
    spark_driver_java_opts = TextField(default='-Dhdp.version=2.6.1.0-129', name='spark.opts.spark.driver.extraJavaOptions')
    cluster_manager = 'yarn'

    def spark_home_default(self):
        if os.path.exists('/usr/hdp'):
            # Running in Hortonworks HDP
            return '/usr/hdp/current/spark2-client'
        else:
            return '/usr/lib/spark'

    def _copy_to_HDFS(self):
        fs = pyarrow.hdfs.connect()
        logger.info('Uploading Amaterasu executor to HDFS')
        with open('{}/dist/executor-{}-all.jar'.format(self.amaterasu_home, __version__), 'rb') as f:
            fs.upload('{}/executor.jar'.format(self.yarn_jarspath), f)
        with open('/etc/amaterasu/amaterasu.conf', 'rb') as f:
            fs.upload('{}/amaterasu.conf'.format(self.yarn_jarspath), f)
        logger.info('Uploading Miniconda to HDFS')
        miniconda_dist_path = os.path.join(self.amaterasu_home, 'dist',
                                           'Miniconda2-latest-Linux-x86_64.sh')
        with open(miniconda_dist_path) as f:
            fs.upload("{}/{}".format(self.yarn_jarspath, 'miniconda.sh'), f)
        logger.info('Uploading Spark-Client to HDFS')
        for root, _, files in os.walk(self.spark_home):
            remote_path_dir = root.split(self.spark_home)[1]
            for file_name in files:
                logger.debug('Uploading: "{}" to HDFS at: {}'.format(local_path, remote_path))
                local_path = '{}/{}'.format(root, file_name)
                remote_path = '{}/{}/{}'.format(self.yarn_jarspath, remote_path_dir, file_name)
                with open(local_path, 'rb') as f:
                    fs.upload(remote_path, f)



    def handle(self):
        super().handle()
        self._copy_to_HDFS()


def get_handler(**kwargs):
    if kwargs['mesos']:
        return MesosConfigurationHandler
    elif kwargs['yarn']:
        return YarnConfigurationHandler
    else:
        raise ValueError('Could not find a handler for the given arguments')