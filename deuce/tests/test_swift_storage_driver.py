import six
from deuce.drivers.blockstoragedriver import BlockStorageDriver
if six.PY2:
    from deuce.drivers.swift2k import SwiftStorageDriver
else:
    from deuce.drivers.swift3k import SwiftStorageDriver
from deuce.tests.test_disk_storage_driver import DiskStorageDriverTest

# Users need take care of authenticate themselves and
# have the token ready for each query.

from swiftclient import client as Conn
from swiftclient.exceptions import ClientException

from pecan import conf

import sys

# TODO: Make this test generic -- it should not konw
# which particular driver it is testing.


class SwiftStorageDriverTest(DiskStorageDriverTest):

    def setUp(self):
        super(SwiftStorageDriverTest, self).setUp()

        auth_url = conf.block_storage_driver.swift.auth_url
        username = conf.block_storage_driver.swift.username
        password = conf.block_storage_driver.swift.password

        os_options = dict()

        self.mocking = False
        try:
            if conf.block_storage_driver.swift.is_mocking:
                self.mocking = True
        except:
            self.mocking = False

        if not self.mocking:
            try:
                self.storage_url, self.token = \
                    Conn.get_keystoneclient_2_0(
                        auth_url=auth_url,
                        user=username,
                        key=password,
                        os_options=os_options)
            except ClientException as e:
                sys.exit(str(e))

        else:
            self.storage_url = 'mocking_url'
            self.token = 'mocking_token'
        self.project_id = self.storage_url[self.storage_url.rfind("/") + 1:]

    def test_basic_construction(self):
        driver = SwiftStorageDriver(self.storage_url,
            self.token, self.project_id)

    def create_driver(self):
        return SwiftStorageDriver(self.storage_url,
            self.token, self.project_id)


    def test_ancestry(self):

        driver = SwiftStorageDriver(self.storage_url,
            self.token, self.project_id)
        assert isinstance(driver, SwiftStorageDriver)
        assert isinstance(driver, object)

        # Test all exceptions
        driver = SwiftStorageDriver(
            self.storage_url,
            self.token + '1',
            self.project_id)
        projectid = 'notmatter'
        vaultid = 'notmatter'
        blockid = 'notmatter'
        driver.create_vault(projectid, vaultid)
        driver.vault_exists(projectid, vaultid)

        driver.store_block(projectid, vaultid, blockid,
            str('test').encode('utf-8'))
        driver.block_exists(projectid, vaultid, blockid)
        driver.get_block_obj(projectid, vaultid, blockid)
        driver.delete_block(projectid, vaultid, blockid)
        # assert None in driver.get_block_obj(projectid, vaultid, blockid)
        driver.delete_vault(projectid, vaultid)
