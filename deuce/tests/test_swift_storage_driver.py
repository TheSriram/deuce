
from deuce.drivers.blockstoragedriver import BlockStorageDriver
from deuce.drivers.swift import SwiftStorageDriver
from deuce.tests.test_disk_storage_driver import DiskStorageDriverTest

# Users need take care of authenticate themselves and
# have the token ready for each query.

from swiftclient import client as Conn
from swiftclient.exceptions import ClientException

from pecan import conf

import sys
import os
import json


# TODO: Make this test generic -- it should not konw
# which particular driver it is testing.


class SwiftStorageDriverTest(DiskStorageDriverTest):

    def get_Auth_Token(self):

        auth_url = str(conf.block_storage_driver.swift.auth_url)

        username = 'User Name'
        password = 'Password'

        self.mocking = False
        try:
            if conf.block_storage_driver.swift.is_mocking:
                self.mocking = True
        except:
            self.mocking = False

        if not self.mocking:
            try:
                os_options = dict()
                storage_url, token = \
                    Conn.get_keystoneclient_2_0(
                        auth_url=auth_url,
                        user=username,
                        key=password,
                        os_options=os_options)
            except ClientException as e:
                sys.exit(str(e))

        else:
            storage_url = conf.block_storage_driver.swift.storage_url
            token = 'mocking_token'

        self._hdrs = {"x-project-id": 'testswfitstoragedrv',
            "x-auth-token": token}
        return storage_url, token

    def test_basic_construction(self):
        storage_url, token = self.get_Auth_Token()
        project_id = storage_url[storage_url.rfind("/") + 1:]
        driver = SwiftStorageDriver(storage_url,
            token, project_id)

    def create_driver(self):
        storage_url, token = self.get_Auth_Token()
        project_id = storage_url[storage_url.rfind("/") + 1:]
        return SwiftStorageDriver(storage_url,
            token, project_id)

    def test_ancestry(self):
        storage_url, token = self.get_Auth_Token()
        project_id = storage_url[storage_url.rfind("/") + 1:]

        driver = SwiftStorageDriver(storage_url,
            token, project_id)
        assert isinstance(driver, SwiftStorageDriver)
        assert isinstance(driver, object)

        # Test all exceptions
        failed_token = token + '1'
        driver = SwiftStorageDriver(
            storage_url,
            token + '1',
            project_id)
        projectid = 'notmatter'
        vaultid = 'notmatter'
        blockid = 'notmatter'
        driver.create_vault(projectid, vaultid, failed_token)
        driver.vault_exists(projectid, vaultid, failed_token)
        driver.delete_vault(projectid, vaultid, failed_token)
        driver.store_block(projectid, vaultid, blockid,
            str('').encode('utf-8'), failed_token)
        driver.block_exists(projectid, vaultid, blockid,
            failed_token)
        driver.delete_block(projectid, vaultid, blockid,
            failed_token)
        driver.get_block_obj(projectid, vaultid, blockid,
            failed_token)

    def test_network_drops(self):
        """
        This is only to exercise code that relies on network errors to occur
        """
        self.mocking = False
        try:
            if conf.block_storage_driver.swift.is_mocking:
                self.mocking = True
        except:
            self.mocking = False

        if self.mocking:

            storage_url, token = self.get_Auth_Token()
            project_id = 'notmatter'
            vault_id = 'notmatter'
            block_id = 'notmatter'

            driver = SwiftStorageDriver(storage_url, token, project_id)
            assert isinstance(driver, SwiftStorageDriver)
            assert isinstance(driver, object)

            # simulate swiftclient tossing exceptions
            driver.Conn.mock_drop_connections(True)

            self.assertFalse(driver.create_vault(project_id, vault_id, token))

            self.assertFalse(driver.vault_exists(project_id, vault_id, token))

            self.assertFalse(driver.delete_vault(project_id, vault_id, token))

            self.assertFalse(driver.store_block(project_id, vault_id, block_id,
                str('').encode('utf-8'), token))

            self.assertFalse(driver.block_exists(project_id, vault_id,
                block_id, token))

            self.assertFalse(driver.delete_block(project_id, vault_id,
                block_id, token))

            self.assertIsNone(driver.get_block_obj(project_id, vault_id,
                block_id, token))

            # Stats should come back as zero even though the connection "dropped"
            bad_vault_stats = driver.get_vault_statistics(project_id, vault_id,
                token))
            main_keys = ('total-size', 'block-count')
            for key in main_keys:
                assert key in statistics.keys()
                assert statistics[key] == 0


            # simulate swiftclient tossing exceptions
            driver.Conn.mock_drop_connections(False)
