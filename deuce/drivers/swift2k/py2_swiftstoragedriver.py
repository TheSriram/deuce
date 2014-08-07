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
            conf.block_storage_driver.swift.swift_module)
        self.Conn = getattr(self.lib_pack, 'client')

    # =========== VAULTS ===============================
    def create_vault(self, project_id, vault_id):
        response = dict()

        # try:
        gevent.joinall([gevent.spawn(self.Conn.put_container,
            url=self._storage_url,
            token=self._token,
            container=vault_id,
            response_dict=response)])
        return response['status'] == 201
        # except ClientException as e:
        #     return False

    def vault_exists(self, project_id, vault_id):
        # try:
        green_threads = [gevent.spawn(self.Conn.head_container,
            url=self._storage_url,
            token=self._token,
            container=vault_id)]
        gevent.joinall(green_threads)
        return True if any([thread.value for thread in green_threads]) else False
        # except ClientException as e:
        #     return False

    def delete_vault(self, project_id, vault_id):
        response = dict()
        # try:
        gevent.joinall([gevent.spawn(self.Conn.delete_container,
            url=self._storage_url,
            token=self._token,
            container=vault_id,
            response_dict=response)])
        return response['status'] >= 200 and response['status'] < 300
        # except ClientException as e:
        #     return False

    # =========== BLOCKS ===============================
    def store_block(self, project_id, vault_id, block_id, blockdata):
        response = dict()
        # try:
        mdhash = hashlib.md5()
        mdhash.update(blockdata)
        mdetag = mdhash.hexdigest()
        green_threads=[gevent.spawn(self.Conn.put_object,
            url=self._storage_url,
            token=self._token,
            container=vault_id,
            name='blocks/' + str(block_id),
            contents=blockdata,
            content_length=len(blockdata),
            etag=mdetag,
            response_dict=response)]
        gevent.joinall(green_threads)
        # from nose.tools import set_trace
        # set_trace()
        ret_etag = [thread.value for thread in green_threads]
        return response['status'] == 201 and ret_etag[0] == mdetag
        # except ClientException as e:
        #     return False

    def block_exists(self, project_id, vault_id, block_id):
        # try:
        green_threads= [gevent.spawn(self.Conn.head_object,
            url=self._storage_url,
            token=self._token,
            container=vault_id,
            name='blocks/' + str(block_id))]
        gevent.joinall(green_threads)
        ret = [thread.value for thread in green_threads]
        return True if any(ret) else False
        # except ClientException as e:
        #     return False

    def delete_block(self, project_id, vault_id, block_id):
        response = dict()
        # try:
        gevent.joinall([gevent.spawn(self.Conn.delete_object,
            url=self._storage_url,
            token=self._token,
            container=vault_id,
            name='blocks/' + str(block_id),
            response_dict=response)])
        return response['status'] >= 200 and response['status'] < 300
        # except ClientException as e:
        #     return False

    def get_block_obj(self, project_id, vault_id, block_id):
        response = dict()
        buff = BytesIO()
        # try:
            # green_threads = [gevent.spawn]
            # ret_hdr, ret_obj_body = \
        green_threads = [gevent.spawn(self.Conn.get_object,
                url=self._storage_url,
                token=self._token,
                container=vault_id,
                name='blocks/' + str(block_id),
                response_dict=response)]
        gevent.joinall(green_threads)

        ret = [thread.value for thread in green_threads]
        # if None in ret:
        #     return None
        # from nose.tools import set_trace
        # set_trace()
        # else:
        if any(ret[0]):
            buff.write(ret[0][1])
            buff.seek(0)
            return buff
        else:
            return None
        # except ClientException as e:
        #     return None

    def create_blocks_generator(self, project_id, vault_id, block_gen):
        """Returns a generator of file-like objects that are
        ready to read. These objects will get closed
        individually."""
        return (self.get_block_obj(project_id, vault_id, block_id)
            for block_id in block_gen)
