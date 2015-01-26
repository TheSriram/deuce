from deuce.drivers.cassandra import CassandraStorageDriver
from deuce.tests.test_sqlite_storage_driver import SqliteStorageDriverTest
from deuce import conf

from mock import patch

import unittest

# Explanation:
#   - The SqliteStorageDriver is the reference metadata driver. All
# other drivers should conform to the same interface, therefore
# we simply extend the SqliteStorageTest and run the sqlite driver tests
# against the Cassandra driver. The sqlite tests simply exercise the
# interface.

cassandra_mock = conf.metadata_driver.cassandra.testing.is_mocking
ssl_enabled = conf.metadata_driver.cassandra.ssl_enabled


class CassandraStorageDriverTest(SqliteStorageDriverTest):

    def create_driver(self):
        return CassandraStorageDriver()

    @unittest.skipIf(cassandra_mock is False
       and ssl_enabled is False,
       "Don't run the test if we are running without SSL")
    def test_create_driver_auth_ssl(self):
        with patch.object(conf.metadata_driver.cassandra, 'ssl_enabled',
                          return_value=True):
            with patch.object(conf.metadata_driver.cassandra, 'auth_enabled',
                              return_value=True):
                return CassandraStorageDriver()

    @unittest.skipIf(cassandra_mock is False,
    "Dont run the test if we are against non-mocked Cassandra")
    def test_create_driver_consistency_from_conf(self):
        contact_points = ['127.0.0.1', '127.0.0.2', '127.0.0.3']
        conf.metadata_driver.cassandra.cluster = contact_points
        return CassandraStorageDriver()
