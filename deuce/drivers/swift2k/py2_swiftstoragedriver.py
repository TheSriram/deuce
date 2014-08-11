from gevent import monkey

monkey.patch_all()
import gevent
from pecan import conf

from deuce.drivers.blockstoragedriver import BlockStorageDriver

import os
import io
import shutil

import importlib
import hashlib

from swiftclient.exceptions import ClientException

from six import BytesIO


class PY2_SwiftStorageDriver(BlockStorageDriver):

    def __init__(self, storage_url, auth_token, project_id):
        self._storage_url = storage_url
        self._token = auth_token
        self._project_id = project_id

        self.lib_pack = importlib.import_module(
            conf.block_storage_driver.swift2k.swift_module)
        self.Conn = getattr(self.lib_pack, 'client')

    # =========== VAULTS ===============================
    def create_vault(self, project_id, vault_id, auth_token=None):
        response = dict()

        green_thread = gevent.spawn(self.Conn.put_container,
                                    url=self._storage_url,
                                    token=auth_token,
                                    container=vault_id,
                                    response_dict=response)
        gevent.joinall([green_thread])

        if isinstance(green_thread.exception, ClientException):
            return False
        else:
            return response['status'] == 201

    def vault_exists(self, project_id, vault_id, auth_token=None):

        green_thread = gevent.spawn(self.Conn.head_container,
                                    url=self._storage_url,
                                    token=auth_token,
                                    container=vault_id)
        gevent.joinall([green_thread])
        if isinstance(green_thread.exception, ClientException):
            return False
        else:
            return True if any(green_thread.value) else False

    def delete_vault(self, project_id, vault_id, auth_token=None):
        response = dict()

        green_thread = gevent.spawn(self.Conn.delete_container,
                                    url=self._storage_url,
                                    token=auth_token,
                                    container=vault_id,
                                    response_dict=response)
        gevent.joinall([green_thread])
        if isinstance(green_thread.exception, ClientException):
            return False
        else:
            return response['status'] >= 200 and response['status'] < 300

    # =========== BLOCKS ===============================
    def store_block(
            self, project_id, vault_id, block_id, blockdata, auth_token=None):
        response = dict()
        mdhash = hashlib.md5()
        mdhash.update(blockdata)
        mdetag = mdhash.hexdigest()
        green_thread = gevent.spawn(self.Conn.put_object,
                                    url=self._storage_url,
                                    token=auth_token,
                                    container=vault_id,
                                    name='blocks_' + str(block_id),
                                    contents=blockdata,
                                    content_length=len(blockdata),
                                    etag=mdetag,
                                    response_dict=response)
        gevent.joinall([green_thread])
        if isinstance(green_thread.exception, ClientException):
            return False

        else:
            ret_etag = green_thread.value
            return response['status'] == 201 and ret_etag[0] == mdetag

    def block_exists(self, project_id, vault_id, block_id, auth_token=None):
        green_thread = gevent.spawn(self.Conn.head_object,
                                    url=self._storage_url,
                                    token=auth_token,
                                    container=vault_id,
                                    name='blocks_' + str(block_id))
        gevent.joinall([green_thread])
        if isinstance(green_thread.exception, ClientException):
            return False
        else:

            return True if any(green_thread.value) else False

    def delete_block(self, project_id, vault_id, block_id, auth_token=None):
        response = dict()

        green_thread = gevent.spawn(self.Conn.delete_object,
                                    url=self._storage_url,
                                    token=auth_token,
                                    container=vault_id,
                                    name='blocks_' + str(block_id),
                                    response_dict=response)
        gevent.joinall([green_thread])
        if isinstance(green_thread.exception, ClientException):
            return False
        else:
            return response['status'] >= 200 and response['status'] < 300

    def get_block_obj(self, project_id, vault_id, block_id, auth_token=None):
        response = dict()
        buff = BytesIO()
        green_thread = gevent.spawn(self.Conn.get_object,
                                    url=self._storage_url,
                                    token=auth_token,
                                    container=vault_id,
                                    name='blocks_' + str(block_id),
                                    response_dict=response)
        gevent.joinall([green_thread])
        if isinstance(green_thread.exception, ClientException):
            return None
        else:
            ret = green_thread.value
            buff.write(ret[1])
            buff.seek(0)
            return buff

    def create_blocks_generator(
            self, project_id, vault_id, block_gen, auth_token=None):
        """Returns a generator of file-like objects that are
        ready to read. These objects will get closed
        individually."""
        return (self.get_block_obj(project_id, vault_id, block_id, auth_token)
                for block_id in block_gen)
