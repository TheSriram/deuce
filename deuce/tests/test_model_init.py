from deuce.tests import *
from deuce.model import (
    init_model,
    _load_storage_driver,
    _load_metadata_driver,
    BadStorageDriverException,
    BadMetadataDriverException
)


class TestModelInit(V1Base):

    def setUp(self):
        super(TestModelInit, self).setUp()
        import deuce
        deuce.metadata_driver = None
        deuce.storage_driver = None

    def tearDown(self):
        super(TestModelInit, self).tearDown()
        import deuce
        deuce.metadata_driver = None
        deuce.storage_driver = None

    def test_load_drivers(self):
        init_model()

    def test_load_bad_metadata_driver(self):
        with self.assertRaises(BadMetadataDriverException):
            _load_metadata_driver('bad metadata driver')

    def test_load_bad_storage_driver(self):
        with self.assertRaises(BadStorageDriverException):
            _load_storage_driver('bad storage driver')
