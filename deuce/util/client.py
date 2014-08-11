import aiohttp
import hashlib
from deuce.util.event_loop import get_event_loop

# NOTE (TheSriram) : must include exception handling


@get_event_loop
def _request(method, url, headers, data=None):

    response = yield from aiohttp.request(method=method, url=url,
                                          headers=headers, data=data)
    return response


@get_event_loop
def _noloop_request_getobj(method, url, headers, data=None):
    response = yield from aiohttp.request(method=method, url=url,
                                          headers=headers, data=data)

    block = yield from response.content.read()
    return block

# Create vault


def put_container(url, token, container, response_dict):

    headers = {'X-Auth-Token': token}
    response = _request('PUT', url + '/' + container, headers=headers)
    response_dict['status'] = response.status


# Check Vault
def head_container(url, token, container, response_dict):
    headers = {'X-Auth-Token': token}
    response = _request('HEAD', url + '/' + container, headers=headers)
    response_dict['status'] = response.status


# Delete Vault
def delete_container(url, token, container, response_dict):
    headers = {'X-Auth-Token': token}
    response = _request('DELETE', url + '/' + container, headers=headers)
    response_dict['status'] = response.status


# Store Block

def put_object(url, token, container, name, contents,
               content_length, response_dict, etag=None):
    headers = {'X-Auth-Token': token}
    headers = headers.copy()
    if etag:
        headers.update({'Etag': etag, 'Content-Length': content_length})
    else:
        headers.update({'Content-Length': content_length})
    response = _request('PUT', url + '/' + container + '/blocks_' + str(name),
                        headers=headers, data=contents)

    response_dict['status'] = response.status

# Check Block


def head_object(url, token, container, name, response_dict):
    headers = {'X-Auth-Token': token}
    response = _request(
        'HEAD',
        url +
        '/' +
        container +
        '/blocks_' +
        str(name),
        headers=headers)

    response_dict['status'] = response.status


# Delete Block
def delete_object(url, token, container, name, response_dict):
    headers = {'X-Auth-Token': token}
    response = _request(
        'DELETE',
        url +
        '/' +
        container +
        '/blocks_' +
        str(name),
        headers=headers)

    response_dict['status'] = response.status


# Get Block

def get_object(url, token, container, name, response_dict):
    headers = {'X-Auth-Token': token}
    response = _noloop_request_getobj(
        'GET',
        url +
        '/' +
        container +
        '/blocks_' +
        str(name),
        headers=headers)

    return response
