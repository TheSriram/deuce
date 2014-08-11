

import os
import io
import shutil
from swiftclient.exceptions import ClientException
import hashlib
import uuid

import datetime
import atexit

container_path = '/tmp/swift_mocking'
mock_drop_connection_status = False


def mock_drop_connections(drop_status):
    global mock_drop_connection_status
    mock_drop_connection_status = drop_status


def _clean_up_mocking():
    import os.path

    if os.path.exists(container_path):
        shutil.rmtree(container_path)

atexit.register(_clean_up_mocking)


def _get_vault_path(vault_id):
    return os.path.join(container_path, vault_id)


def _get_block_path(vault_id, block_id):
    vault_path = _get_vault_path(vault_id)
    return os.path.join(vault_path, 'blocks', str(block_id))


# Create Vault
def put_container(url,
                  token,
                  container,
                  response_dict):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    path = _get_vault_path(container)
    if not os.path.exists(path):
        shutil.os.makedirs(path)
    else:
        raise ClientException('mocking')

    block_path = os.path.join(path, 'blocks')
    if not os.path.exists(block_path):
        shutil.os.makedirs(block_path)
    response_dict['status'] = 201


# Check Vault
def head_container(url,
                   token,
                   container):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    path = _get_vault_path(container)
    if os.path.exists(path):
        return 'mocking_ret'
    else:
        raise ClientException('mocking')


# Delete Vault
def delete_container(url,
                     token,
                     container,
                     response_dict):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    try:
        # Basic response requirements
        response_dict['content-length'] = 0
        response_dict['content-type'] = 'text/html; charset=UTF-8'
        response_dict['x-transaction-id'] = 'req-' + str(uuid.uuid4())
        # Thu, 16 Jan 2014 18:04:04 GMT
        response_dict['date'] = datetime.datetime.utcnow().strftime(
            "%a, %d %b %Y %H:%M:%S %Z")

        path = _get_vault_path(container)
        blockpath = os.path.join(path, 'blocks')

        response_dict['x-vault-path'] = path
        response_dict['x-block-path'] = blockpath

        if os.path.exists(path):

            if os.path.exists(blockpath):
                if os.listdir(blockpath) != []:
                    # container not empty
                    response_dict['x-error-message'] = 'container not empty'
                    response_dict['status'] = 409
                else:
                    # no blocks we're okay

                    # container exists and is empty (no blocks)
                    shutil.rmtree(path)

                    response_dict['status'] = 204

            elif os.listdir(path) == []:
                # container exists and is empty (no blocks)
                shutil.rmtree(path)

                response_dict['status'] = 204

            else:
                # else there is some other issue
                response_dict['x-error-message'] = 'mocking: listing directory'
                response_dict['status'] = 500
                assert False

        else:
            # Container does not exist
            response_dict['x-error-message'] = 'vault does not exist'
            response_dict['status'] = 404

    except Exception as ex:
        response_dict['x-error-message'] = 'mocking error: {0:}'.format(ex)
        response_dict['status'] = 500
        raise ClientException('mocking error: {0:}'.format(ex))


# Store Block
def put_object(url,
               token,
               container,
               name,
               contents,
               content_length,
               response_dict,
               etag=None):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    blocks_path = os.path.join(_get_vault_path(container), 'blocks')
    if not os.path.exists(blocks_path):
        raise ClientException('mocking')

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
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    path = _get_block_path(container, name)
    if not os.path.exists(path):
        raise ClientException('mocking')
    return 'mocking_ret'


# Delete Block
def delete_object(url,
                  token,
                  container,
                  name,
                  response_dict):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    path = _get_block_path(container, name)
    if os.path.exists(path):
        os.remove(path)
        response_dict['status'] = 201
    else:
        raise ClientException('mocking')


# Get Block
def get_object(url,
               token,
               container,
               name,
               response_dict):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    path = _get_block_path(container, name)

    if not os.path.exists(path):
        raise ClientException('mocking')

    buff = ""
    with open(path, 'rb') as infile:
        buff = infile.read()

    return dict(), buff


def get_keystoneclient_2_0(auth_url,
                           user,
                           key,
                           os_options):
    if user == 'failing_auth_hook':
        raise ClientException('mocking auth failure')
    return 'mocking_project_id', 'mocking_project_token'
