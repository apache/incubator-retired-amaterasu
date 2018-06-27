from setuptools import setup, find_packages
from setuptools.command.install import install
import os

class PostInstallCommand(install):

    AMATERASU_LOG_DIR = '/var/log/amaterasu'
    AMATERASU_CONFIG_DIR = '/etc/amaterasu'

    def run(self):
        old_umask = os.umask(000)
        if not os.path.exists(self.AMATERASU_LOG_DIR):
            os.mkdir(self.AMATERASU_LOG_DIR)
            os.chmod(self.AMATERASU_LOG_DIR, 777)
        if not os.path.exists(self.AMATERASU_CONFIG_DIR):
            os.mkdir(self.AMATERASU_CONFIG_DIR)
        os.umask(old_umask)
        return super().run()


setup(
    name='amaterasu',
    version='0.2.1',
    packages=find_packages(),
    url='https://github.com/apache/incubator-amaterasu',
    license='Apache License 2.0 ',
    author='Apache Amaterasu (incubating)',
    author_email="dev@amaterasu.incubator.apache.org",
    description='Apache Amaterasu (incubating) is an open source, configuration managment and deployment framework for big data pipelines',
    install_requires=['colorama', 'GitPython', 'six', 'PyYAML', 'netifaces', 'multipledispatch', 'docopt', 'paramiko', 'wget'],
    tests_require=['behave'],
    python_requires='!=3.0.*, !=3.1.*, !=3.2.*, <4',
    entry_points={
        'console_scripts': [
            'ama=amaterasu.__main__:main'
        ]
    },
    include_package_data=True,
    package_data={
        'amaterasu.cli.resources': ['*']
    },
    cmdclass={
        'install': PostInstallCommand
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Java',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Scientific/Engineering'
    ]
)
