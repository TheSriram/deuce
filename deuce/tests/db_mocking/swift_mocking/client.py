

import os
import io
import shutil
from swiftclient.exceptions import ClientException
import hashlib

container_path = '/tmp/swift_mocking'


def _get_vault_path(vault_id):
    return os.path.join(container_path, vault_id)


def _get_block_path(vault_id, block_id):
    vault_path = _get_vault_path(vault_id)
    return os.path.join(vault_path, str(block_id))


# Create Vault
def put_container(url,
            token,
            container,
            response_dict):
    # from nose.tools import set_trace
    # set_trace()
    path = _get_vault_path(container)
    if not os.path.exists(path):
        shutil.os.makedirs(path)
        block_path = os.path.join(path, 'blocks')
        if not os.path.exists(block_path):
            shutil.os.makedirs(block_path)
            response_dict['status'] = 201
    else:
        response_dict['status'] = 202




# Check Vault
def head_container(url,
            token,
            container):
    path = _get_vault_path(container)
    if os.path.exists(path):
        return 'mocking_ret'
    else:
        return None


# Delete Vault
def delete_container(url,
            token,
            container,
            response_dict):
    # try:
        # from nose.tools import set_trace
        # set_trace()

    path = _get_vault_path(container)
    blockpath = os.path.join(path, 'blocks')
    if os.path.exists(path) or os.path.exists(blockpath):
        if os.listdir(path) == [] or os.listdir(blockpath) == []:
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
def put_object(url,
            token,
            container,
            name,
            contents,
            content_length,
            response_dict,
            etag=None):

    blocks_path = os.path.join(_get_vault_path(container), 'blocks')
    if not os.path.exists(blocks_path):
        response_dict['status'] = 404
        return

    path = _get_block_path(container, name)

    with open(path, 'wb') as outfile:
        outfile.write(contents)

    mdhash = hashlib.md5()
    mdhash.update(contents)
    response_dict['status'] = 201
    return mdhash.hexdigest()


# Check Block
def head_object(url,
            token,
            container,
            name):

    path = _get_block_path(container, name)
    if not os.path.exists(path):
        return None
    return 'mocking_ret'


# Delete Block
def delete_object(url,
            token,
            container,
            name,
            response_dict):
    path = _get_block_path(container, name)
    if os.path.exists(path):
        os.remove(path)
        response_dict['status'] = 201
    else:
        response_dict['status'] = 404


# Get Block
def get_object(url,
            token,
            container,
            name,
            response_dict):
    path = _get_block_path(container, name)

    if not os.path.exists(path):
        response_dict['status'] = 404
        return (None,None)

    buff = ""
    with open(path, 'rb') as infile:
        buff = infile.read()

    return dict(), buff
