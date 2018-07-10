"""
Create or change Amaterasu's configuration.

Usage:
    ama [-V] setup ( mesos | yarn [-f] )

Options:
    -f --force-bin  YARN-only - remove all existing Amaterasu HDFS assets
"""
import shutil
from .base import BaseHandler
from ..compat import run_subprocess
from ..utils import input
import os
import wget
import colorama
import logging
import subprocess
from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)

__version__ = '0.2.0-incubating-rc4'
THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class BaseConfigurationHandler(BaseHandler):

    TEMPLATE_NAME = None

    def __init__(self, **args):
        self.jinja_env = Environment(
            loader=FileSystemLoader(os.path.normpath(os.path.join(THIS_DIR, os.path.pardir, 'conf')))
        )
        super().__init__(**args)

    def _render_configuration_file(self):
        if os.path.exists(self.CONFIGURATION_PATH):
            answer = input.default_input("An Apache Amaterasu configuration file exists, do you want to overwrite (Yn)?", "n")
            generate_new_configuration = answer.lower() == 'y'
        else:
            generate_new_configuration = True
        if generate_new_configuration:
            self.jinja_env.get_template(self.TEMPLATE_NAME).stream().dump(self.CONFIGURATION_PATH)
        logger.info("Successfully created Apache Amaterasu configuration file")

    def _download_dependencies(self):
        miniconda_dist_path = os.path.join(self.AMATERASU_HOME, 'dist', 'Miniconda2-latest-Linux-x86_64.sh')
        if not os.path.exists(miniconda_dist_path):
            print('\n', colorama.Style.BRIGHT, 'Fetching Miniconda distributable', colorama.Style.RESET_ALL)
            wget.download(
                'https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh',
                out=miniconda_dist_path
            )

    def handle(self):
        self._render_configuration_file()
        self._download_dependencies()


class MesosConfigurationHandler(BaseConfigurationHandler):

    TEMPLATE_NAME = "ama-mesos.conf"

    def _download_dependencies(self):
        super()._download_dependencies()
        spark_dist_path = os.path.join(self.AMATERASU_HOME, 'dist',
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

    TEMPLATE_NAME = "ama-yarn.conf"

    def _hdfs_directory_exists(self, dir_name):
        try:
            run_subprocess([
                "su",
                "hadoop",
                "-c",
                "hdfs dfs -test -e {}".format(dir_name)
            ])
            amaterasu_hdfs_dir_exists = True
        except subprocess.CalledProcessError as e:
            print(e.returncode)
            if e.returncode == 1:
                amaterasu_hdfs_dir_exists = False
            else:
                raise
        return amaterasu_hdfs_dir_exists

    def _remove_amaterasu_HDFS_assets(self):
        run_subprocess([
            "su",
            self.user,
            "-c",
            "hdfs dfs -rm -r -skipTrash /apps/amaterasu"
        ])

    def handle(self):
        super().handle()
        amaterasu_dir_exists = lambda: self._hdfs_directory_exists("/apps/amaterasu")

        if self.args.get('force-bin', False) and amaterasu_dir_exists():
            self._remove_amaterasu_HDFS_assets()


def get_handler(**kwargs):
    if kwargs['mesos']:
        return MesosConfigurationHandler
    elif kwargs['yarn']:
        return YarnConfigurationHandler
    else:
        raise ValueError('Could not find a handler for the given arguments')