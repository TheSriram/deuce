# -*- coding: utf-8 -*-
import sys
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages
if sys.version_info[0] == 3:
    REQUIRES = ['six', 'pecan', 'setuptools >= 1.1.6',
    'cassandra-driver', 'pymongo', 'msgpack-python', 'asyncio',
    'aiohttp', 'python-swiftclient']
else:
    REQUIRES = ['six', 'pecan', 'setuptools >= 1.1.6',
    'cassandra-driver', 'pymongo', 'msgpack-python', 'gevent',
    'python-swiftclient']
setup(
    name='deuce',
    version='0.1',
    description='Deuce - Block-level de-duplication as-a-service',
    author='Rackspace',
    author_email='',
    install_requires=REQUIRES,
    test_suite='deuce',
    zip_safe=False,
    include_package_data=True,
    packages=find_packages(exclude=['tests'])
)
