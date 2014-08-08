from pecan import conf

from deuce.drivers.blockstoragedriver import BlockStorageDriver

import hashlib
import importlib
import asyncio
import aiohttp

from deuce.util.event_loop import get_event_loop
from deuce.util import log
import deuce.util.p3k_swiftclient as p3k_swiftclient
logger = log.getLogger(__name__)
from swiftclient.exceptions import ClientException

from six import BytesIO


# NOTE(TheSriram) : changed to inherit from object for testing purposes
class SwiftStorageDriver(BlockStorageDriver):
    def __init__(self, storage_url, auth_token, project_id):
        self._storage_url = storage_url
        self._token = auth_token
        self._project_id = project_id
        self.headers = {'X-Auth-Token': auth_token}
        # import ipdb
        # ipdb.set_trace()
        self.lib_pack = importlib.import_module(conf.block_storage_driver.swift.swift_module)
        self.Conn = getattr(self.lib_pack, 'p3k_swiftclient')

    # @get_event_loop
    # def _request(self, method, url, headers, data = None):
    #
    #     response = yield from aiohttp.request(method = method, url = url, headers = headers, data = data)
    #     return response
    #
    # # NOTE(TheSriram) : This will be used for storing blocks, as multiple blocks PUT will need to
    # # share the same event loop. This can be made more elegant
    # def _noloop_request(self, method, url, headers, data = None):
    #
    #     response = yield from aiohttp.request(method = method, url = url, headers = headers, data = data)
    #     return response

    # =========== VAULTS ===============================
    # @get_event_loop
    def create_vault(self, project_id, vault_id):

        response = dict()
        # import pdb
        # pdb.set_trace()
        # from nose.tools import set_trace
        # set_trace()
        self.Conn.put_container(self._storage_url,self._token, vault_id,response_dict=response)
        return response['status'] == 201
        # self._request('PUT', self._storage_url + '/' + vault_id, headers = self.headers)

    # @get_event_loop
    def vault_exists(self, project_id, vault_id):

        response = dict()
        self.Conn.head_container(self._storage_url,self._token, vault_id,response)
        # import ipdb
        # ipdb.set_trace()
        return response['status']>=200 and response['status']<300
    # @get_event_loop
    def delete_vault(self, project_id, vault_id):
        response = dict()
        self.Conn.delete_container(self._storage_url,self._token, vault_id,response)
        return response['status']>=200 and response['status']<300


    # =========== BLOCKS ===============================
    # @get_event_loop
    def store_block(self, project_id, vault_id, block_ids, blockdatas):
        # # import ipdb
        # # ipdb.set_trace()
        # try:
        #     # NOTE(TheSriram) : Have not tested this yet.
        #     if isinstance(blockdatas, list) and isinstance(block_ids,list):
        #         tasks = []
        #         for block_id, blockdata in zip(block_ids,blockdatas):
        #             # mdhash = hashlib.md5()
        #             # mdhash.update(blockdata)
        #             mdetag = hashlib.md5(blockdata._content).hexdigest()
        #             # mdetag = hashlib.md5()
        #             # headers = self.headers.copy()
        #             # headers.update({'Etag': mdetag, 'Content-Length': str(len(blockdata._content))})
        #             # headers.update({'Content-Length': str(len(blockdata))})
        #             import ipdb
        #             ipdb.set_trace()
        #             tasks.append(asyncio.Task(self.Conn.put_object(self._storage_url,self._token,vault_id,block_id,blockdata._content,str(len(blockdata._content)),dict(),etag=mdetag)))
        #             # tasks.append(asyncio.Task(self._noloop_request('PUT', self._storage_url +'/'+vault_id+'/blocks_' + str(block_id), headers=headers,data=blockdata._content)))
        #         total_responses = yield from asyncio.gather(*tasks)
        #         # return response['status'] == 201 and ret_etag == mdetag


            # else:
        response = dict()
        mdhash = hashlib.md5()
        # import ipdb
        # ipdb.set_trace()
        mdhash.update(blockdatas)
        mdetag = mdhash.hexdigest()
        # headers = self.headers.copy()
        # headers.update({'Etag': mdetag, 'Content-Length': str(len(blockdatas._content))}
        ret = self.Conn.put_object(self._storage_url,self._token,vault_id,block_ids,blockdatas,str(len(blockdatas)),response,etag=mdetag)
        return response['status'] == 201 and ret == mdetag



        # except Exception as ex:
        #     # logger.error(ex)
        #     print (ex)

    def block_exists(self, project_id, vault_id, block_id):
        response = dict()
        self.Conn.head_object(self._storage_url,self._token,vault_id,str(block_id),response)
        # response = self._request('HEAD', self._storage_url + '/' + vault_id + '/blocks_' + str(block_id),
        #                          headers = self.headers)
        return response['status'] >= 200 and response['status'] < 300


    def delete_block(self, project_id, vault_id, block_id):
        # try:
        response = dict()
        # return self._request('DELETE', self._storage_url + '/' + vault_id + '/blocks_' + str(block_id),
        #                      headers = self.headers)
        self.Conn.delete_object(self._storage_url,self._token,vault_id,str(block_id),response)
        return response['status'] >= 200 and response['status'] < 300



    # @get_event_loop
    def get_block_obj(self, project_id, vault_id, block_id):
        # try:
        buff = BytesIO()
        response = dict()
        block = self.Conn.get_object(self._storage_url,self._token,vault_id,str(block_id),response)
        # response = yield from aiohttp.request('GET',
        #                                       self._storage_url + '/' + vault_id + '/blocks_' + str(block_id),
        #                                       headers = self.headers)
        # block = yield from response.content.read()
        # from nose.tools import set_trace
        # set_trace()
        if block[1]:
            buff.write(block[1])
            buff.seek(0)
            return buff
        else:
            return None


    def create_blocks_generator(self, project_id, vault_id, block_gen):
        """Returns a generator of file-like objects that are
        ready to read. These objects will get closed
        individually."""
        return (self.get_block_obj(project_id, vault_id, block_id) for block_id in block_gen)