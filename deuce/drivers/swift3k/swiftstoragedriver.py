from pecan import conf

from deuce.drivers.blockstoragedriver import BlockStorageDriver

import hashlib
import importlib


from deuce.util import log

logger = log.getLogger(__name__)
from swiftclient.exceptions import ClientException

from six import BytesIO


class SwiftStorageDriver(BlockStorageDriver):

    def __init__(self, storage_url, auth_token, project_id):
        self._storage_url = storage_url
        self._token = auth_token
        self._project_id = project_id
        self.headers = {'X-Auth-Token': auth_token}
        self.lib_pack = importlib.import_module(
            conf.block_storage_driver.swift3k.swift_module)
        self.Conn = getattr(self.lib_pack, 'client')

    # =========== VAULTS ===============================

    def create_vault(self, project_id, vault_id, auth_token=None):
        try:
            response = dict()
            self.Conn.put_container(
                self._storage_url,
                auth_token,
                vault_id,
                response_dict=response)
            return response['status'] == 201
        except ClientException:
            return False

    def vault_exists(self, project_id, vault_id, auth_token=None):
        try:

            response = self.Conn.head_container(
                self._storage_url,
                auth_token,
                vault_id)
            return True if response else False

        except ClientException:
            return False

    def delete_vault(self, project_id, vault_id, auth_token=None):
        try:
            response = dict()
            self.Conn.delete_container(
                self._storage_url,
                auth_token,
                vault_id,
                response)
            return response['status'] >= 200 and response['status'] < 300
        except ClientException:
            return False

    # =========== BLOCKS ===============================

    def store_block(self, project_id, vault_id, block_ids,
                    blockdatas, auth_token=None):
        try:
            response = dict()
            mdhash = hashlib.md5()

            mdhash.update(blockdatas)
            mdetag = mdhash.hexdigest()
            ret = self.Conn.put_object(self._storage_url, auth_token, vault_id,
                                       block_ids, blockdatas,
                                       str(len(blockdatas)),
                                       response, etag=mdetag)
            return response['status'] == 201 and ret == mdetag
        except ClientException:
            return False

    def block_exists(self, project_id, vault_id, block_id, auth_token=None):

        try:
            response = self.Conn.head_object(
                self._storage_url,
                auth_token,
                vault_id,
                str(block_id))

            return True if response else False

        except ClientException:
            return False

    def delete_block(self, project_id, vault_id, block_id, auth_token=None):

        response = dict()

        try:
            self.Conn.delete_object(
                self._storage_url,
                auth_token,
                vault_id,
                str(block_id),
                response)
            return response['status'] >= 200 and response['status'] < 300
        except ClientException:
            return False

    def get_block_obj(self, project_id, vault_id, block_id, auth_token=None):

        try:
            buff = BytesIO()
            response = dict()
            block = self.Conn.get_object(
                self._storage_url,
                auth_token,
                vault_id,
                str(block_id),
                response)

            if block[1]:
                buff.write(block[1])
                buff.seek(0)
                return buff
            else:
                return None
        except ClientException:
            return None

    def create_blocks_generator(
            self, project_id, vault_id, block_gen, auth_token=None):
        """Returns a generator of file-like objects that are
        ready to read. These objects will get closed
        individually."""
        return (self.get_block_obj(project_id, vault_id, block_id, auth_token)
                for block_id in block_gen)
