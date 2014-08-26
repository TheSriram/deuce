# -*- coding: utf-8 -*-
import sys
try:
    from distutils.core import setup
    from setuptools import find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages
else:
    REQUIRES = ['six', 'pecan', 'setuptools >= 1.1.6',
                'cassandra-driver', 'pymongo', 'msgpack-python',
                'python-swiftclient']

    extras_require = {
        'python_version>="3.3"': ['asyncio', 'aiohttp'],
        'python_version=="2.7"': ['gevent'],
        '3.4': ['asyncio', 'aiohttp'],
        '3.3': ['asyncio','aiohttp'],
        '2.7': ['gevent']
    }
    setup(
        name='deuce',
        version='0.1',
        description='Deuce - Block-level de-duplication as-a-service',
        license='Apache License 2.0',
        url='github.com/rackerlabs/deuce',
        author='Rackspace',
        author_email='',
        include_package_data=True,
        install_requires=REQUIRES,
        extras_require=extras_require,
        test_suite='deuce',
        zip_safe=False,
        data_files=[('bin', ['config.py'])],
        packages=find_packages(exclude=['tests'])
    )
