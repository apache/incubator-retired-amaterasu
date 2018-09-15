from setuptools import setup, find_packages

setup(
    name='amaterasu_pyspark',
    version='0.2.0-incubating-rc4',
    packages=find_packages(),
    url='https://github.com/apache/incubator-amaterasu',
    license='Apache License 2.0 ',
    author='Apache Amaterasu (incubating)',
    author_email="dev@amaterasu.incubator.apache.org",
    description='Apache Amaterasu (incubating) is an open source, configuration managment and deployment framework for big data pipelines',
    python_requires='!=3.0.*, !=3.1.*, !=3.2.*, <4',
    install_requires=['amaterasu-sdk==0.2.0-incubating-rc4', 'pyspark >= 2'],
    test_requires=['virtualenv'],
    entry_points={'amaterasu.plugins': 'pyspark=amaterasu_pyspark'},
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