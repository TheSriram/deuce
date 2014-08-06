

import os
import io
import shutil
import asyncio
from swiftclient.exceptions import ClientException
from deuce.util.event_loop import get_event_loop
import hashlib

container_path = '/tmp/swift_mocking'


def _get_vault_path(vault_id):
    return os.path.join(container_path, vault_id)


def _get_block_path(vault_id, block_id):
    vault_path = _get_vault_path(vault_id)
    return os.path.join(vault_path,'blocks', str(block_id))


# Create Vault
# @get_event_loop

# @get_event_loop
# def put_container():
# @get_event_loop
# @asyncio.coroutine
def put_container(url,
            token,
            container,
            response_dict):
    # from nose.tools import set_trace
    # set_trace()
    # from nose.tools import set_trace
    # set_trace()
    # import ipdb
    # ipdb.set_trace()
    path =  _get_vault_path(container)
    if not os.path.exists(path):
        shutil.os.makedirs(path)
        block_path = os.path.join(path, 'blocks')
        if not os.path.exists(block_path):
            shutil.os.makedirs(block_path)
            response_dict['status'] = 201
    else:
        response_dict['status'] = 202




# Check Vault
# @get_event_loop
# @asyncio.coroutine
def head_container(url,
            token,
            container,response_dict):
    path = _get_vault_path(container)
    if os.path.exists(path):
        response_dict['status']=204
        return 'mocking_ret'

    else:
        response_dict['status']=404
        return None



# Delete Vault
# @get_event_loop
# @asyncio.coroutine
def delete_container(url,
            token,
            container,
            response_dict):
    # try:
        # from nose.tools import set_trace
        # set_trace()

    path = _get_vault_path(container)
    blockpath = os.path.join(path, 'blocks')
    # from nose.tools import set_trace
    # set_trace()
    if os.path.exists(path) or os.path.exists(blockpath):
        # from nose.tools import set_trace
        # set_trace()
        if os.listdir(blockpath) == []:
            shutil.rmtree(path)
            response_dict['status'] = 204
        else:
            response_dict['status'] = 404
    else:
        response_dict['status'] = 404
        # raise ClientException('mocking')
    # except:
    #     raise ClientException('mocking')


# Store Block
# @get_event_loop
# @asyncio.coroutine
def put_object(url,
            token,
            container,
            name,
            contents,
            content_length,
            response_dict,
            etag=None):

    blocks_path =  os.path.join(_get_vault_path(container), 'blocks')
    if not os.path.exists(blocks_path):
        response_dict['status'] = 404
        return None

    path = _get_block_path(container, name)

    with open(path, 'wb') as outfile:
        outfile.write(contents)

    mdhash = hashlib.md5()
    mdhash.update(contents)
    response_dict['status'] = 201
    return mdhash.hexdigest()


# Check Block
# @get_event_loop
# @asyncio.coroutine
def head_object(url,
            token,
            container,
            name,response_dict):

    path = _get_block_path(container, name)
    if not os.path.exists(path):
        response_dict['status']=404
        return None
    response_dict['status']=200
    return 'mocking_ret'


# Delete Block
# @get_event_loop
# @asyncio.coroutine
def delete_object(url,
            token,
            container,
            name,
            response_dict):
    # import ipdb
    # ipdb.set_trace()
    # import pdb
    # pdb.set_trace()
    # from nose.tools import set_trace
    # set_trace()
    path = _get_block_path(container, name)
    if os.path.exists(path):
        os.remove(path)
        response_dict['status'] = 201
    else:
        response_dict['status'] = 404


# Get Block
# @get_event_loop
# @asyncio.coroutine
def get_object(url,
            token,
            container,
            name,
            response_dict):
    # import ipdb
    # ipdb.set_trace()
    path = _get_block_path(container, name)

    if not os.path.exists(path):
        response_dict['status'] = 404
        return (None,None)

    buff = ""
    with open(path, 'rb') as infile:
        buff = infile.read()
    response_dict['status'] = 200
    return dict(), buff


#
# import pymongo
# import sys
# connection = pymongo.MongoClient(host=sys.argv[1], port=27017)
