from pecan import conf

from deuce.drivers.blockstoragedriver import BlockStorageDriver

import hashlib

import asyncio
import aiohttp

from deuce.util.event_loop import get_event_loop
from deuce.util import log
logger = log.getLogger(__name__)
from swiftclient.exceptions import ClientException

from six import BytesIO


# NOTE(TheSriram) : changed to inherit from object for testing purposes
class SwiftStorageDriver(object):
    def __init__(self, storage_url, auth_token, project_id):
        self._storage_url = storage_url
        self._token = auth_token
        self._project_id = project_id
        self.headers = {'X-Auth-Token': auth_token}

    @get_event_loop
    def _request(self, method, url, headers, data = None):

        response = yield from aiohttp.request(method = method, url = url, headers = headers, data = data)
        return response

    # NOTE(TheSriram) : This will be used for storing blocks, as multiple blocks PUT will need to
    # share the same event loop. This can be made more elegant
    def _noloop_request(self, method, url, headers, data = None):

        response = yield from aiohttp.request(method = method, url = url, headers = headers, data = data)
        return response

    # =========== VAULTS ===============================
    def create_vault(self, project_id, vault_id):

        try:
            return self._request('PUT', self._storage_url + '/' + vault_id, headers = self.headers)
        except Exception as ex:
            logger.error(ex)
            return None

    def vault_exists(self, project_id, vault_id):
        try:
            return self._request('HEAD', self._storage_url + '/' + vault_id, headers = self.headers)
        except Exception as ex:
            logger.error(ex)
            return None

    def delete_vault(self, project_id, vault_id):

        try:

            return self._request('DELETE', self._storage_url + '/' + vault_id, headers = self.headers)
        except Exception as ex:
            logger.error(ex)
            return None

    # =========== BLOCKS ===============================
    @get_event_loop
    def store_block(self, project_id, vault_id, block_ids, blockdatas):
        # import ipdb
        # ipdb.set_trace()
        try:
            # NOTE(TheSriram) : Have not tested this yet.
            if isinstance(blockdatas, list) and isinstance(block_ids,list):
                tasks = []
                for block_id, blockdata in zip(block_ids,blockdatas):
                    # mdhash = hashlib.md5()
                    # mdhash.update(blockdata)
                    mdetag = hashlib.md5(blockdata._content).hexdigest()
                    # mdetag = hashlib.md5()
                    headers = self.headers
                    headers.update({'Etag': mdetag, 'Content-Length': str(len(blockdata._content))})
                    # headers.update({'Content-Length': str(len(blockdata))})
                    tasks.append(asyncio.Task(self._noloop_request('PUT', self._storage_url +'/'+vault_id+'/blocks_' + str(block_id), headers=headers,data=blockdata._content)))
                total_responses = yield from asyncio.gather(*tasks)
                return total_responses


            else:
                mdhash = hashlib.md5()
                mdhash.update(blockdatas._content)
                mdetag = mdhash.hexdigest()
                headers = self.headers
                headers.update({'Etag': mdetag, 'Content-Length': str(len(blockdatas._content))})

                response = yield from self._noloop_request('PUT', self._storage_url + '/' + vault_id + '/blocks_' + str(block_ids),
                                                           headers = headers, data = blockdatas._content)
                return response


        except Exception as ex:
            # logger.error(ex)
            return None

    def block_exists(self, project_id, vault_id, block_id):
        try:

            response = self._request('HEAD', self._storage_url + '/' + vault_id + '/blocks_' + str(block_id),
                                     headers = self.headers)
            return True if response else False
        except Exception as ex:
            logger.error(ex)
            return None

    def delete_block(self, project_id, vault_id, block_id):
        try:

            return self._request('DELETE', self._storage_url + '/' + vault_id + '/blocks_' + str(block_id),
                                 headers = self.headers)

        except Exception as ex:
            logger.error(ex)
            return None


    @get_event_loop
    def get_block_obj(self, project_id, vault_id, block_id):
        try:
            response = yield from aiohttp.request('GET',
                                                  self._storage_url + '/' + vault_id + '/blocks_' + str(block_id),
                                                  headers = self.headers)
            block = yield from response.content.read()

            return block
        except Exception as ex:
            logger.error(ex)
            return None

    def create_blocks_generator(self, project_id, vault_id, block_gen):
        """Returns a generator of file-like objects that are
        ready to read. These objects will get closed
        individually."""
        return (self.get_block_obj(project_id, vault_id, block_id)
                for block_id in block_gen)
