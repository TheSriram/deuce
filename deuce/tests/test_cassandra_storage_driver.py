import importlib
import unittest

import ddt
from mock import patch, MagicMock

from deuce.drivers.cassandra import CassandraStorageDriver
from deuce.tests.test_sqlite_storage_driver import SqliteStorageDriverTest
from deuce import conf

# Explanation:
#   - The SqliteStorageDriver is the reference metadata driver. All
# other drivers should conform to the same interface, therefore
# we simply extend the SqliteStorageTest and run the sqlite driver tests
# against the Cassandra driver. The sqlite tests simply exercise the
# interface.

cassandra_mock = conf.metadata_driver.cassandra.testing.is_mocking
ssl_enabled = conf.metadata_driver.cassandra.ssl_enabled


@ddt.ddt
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

    # (BenjamenMeyer) Covers the contact_point switching when
    # the ~/.deuce/config.ini is not available
    @unittest.skipIf(cassandra_mock is False,
    "Dont run the test if we are against non-mocked Cassandra")
    def test_create_driver_consistency_from_conf(self):
        contact_points = ['127.0.0.1', '127.0.0.2', '127.0.0.3']
        conf.metadata_driver.cassandra.cluster = contact_points
        return CassandraStorageDriver()

    # (BenjamenMeyer) Covers the contact_point switching when
    # the ~/.deuce/config.ini is available
    @ddt.data(['127.0.0.1', '127.0.0.2', '127.0.0.3'], ['127.0.0.1'])
    def test_create_driver_mocked_consistency(self, contact_points):
        # We want the actual imports for 3 of the 4 imports
        cassandra_driver = importlib.import_module(
            conf.metadata_driver.cassandra.db_module)
        cassandra_auth = importlib.import_module(
            '{0}.auth'.format(conf.metadata_driver.cassandra.db_module))
        cassandra_query = importlib.import_module(
            '{0}.query'.format(conf.metadata_driver.cassandra.db_module))

        # Mock importlib so we can control the cassandra cluster import
        with patch('importlib.import_module') as mock_importlib:
            mock_importlib.side_effect = [
                cassandra_driver,
                MagicMock(),
                cassandra_auth,
                cassandra_query
            ]
            # override the connect method so it doesn't actually do anything
            mock_importlib.return_value[1].connect = MagicMock()
            mock_importlib.return_value[1].contact_points = contact_points

            conf.metadata_driver.cassandra.cluster = contact_points
            return CassandraStorageDriver()
