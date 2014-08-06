import aiohttp
import hashlib
from deuce.util.event_loop import get_event_loop
# create vault

@get_event_loop
def _request(method, url, headers, data = None):
    # import ipdb
    # ipdb.set_trace()
    response = yield from aiohttp.request(method = method, url = url, headers = headers, data = data)
    return response
# NOTE(TheSriram) : This will be used for storing blocks, as multiple blocks PUT will need to
# share the same event loop. This can be made more elegant
@get_event_loop
def _noloop_request_getobj(method, url, headers, data = None):
    response = yield from aiohttp.request(method = method, url = url, headers = headers, data = data)
    # import ipdb
    # ipdb.set_trace()
    # obj_contents = response.content
    block = yield from response.content.read()
    return block


def put_container(url,token,container,response_dict):
    # from nose.tools import set_trace
    # set_trace()
    headers = {'X-Auth-Token': token}
    response = _request('PUT', url + '/' + container, headers = headers)
    response_dict['status'] = response.status



# Check Vault
def head_container(url,token,container,response_dict):
    headers = {'X-Auth-Token': token}
    response = _request('HEAD', url + '/' + container, headers = headers)
    response_dict['status'] = response.status


# Delete Vault
def delete_container(url,token,container, response_dict):
    headers = {'X-Auth-Token': token}
    response = _request('DELETE', url + '/' + container, headers = headers)
    response_dict['status'] = response.status



# Store Block
# @get_event_loop
def put_object(url,token,container,name,contents,content_length,response_dict,etag=None):
    headers = {'X-Auth-Token': token}
    # mdhash = hashlib.md5()
    # mdhash.update(contents)
    # mdetag = mdhash.hexdigest()
    headers = headers.copy()
    if etag:
        headers.update({'Etag': etag, 'Content-Length': content_length})
    else:
        headers.update({'Content-Length': content_length})
    response = _request('PUT', url + '/' + container + '/blocks_' + str(name),
                                                           headers = headers, data = contents)
    # return response
    response_dict['status'] = response.status

# Check Block
def head_object(url,token,container,name,response_dict):
    headers = {'X-Auth-Token': token}
    response = _request('HEAD', url + '/' + container + '/blocks_' + str(name), headers = headers)
    # return response
    response_dict['status'] = response.status


# Delete Block
def delete_object(url,token,container,name,response_dict):
    headers = {'X-Auth-Token': token}
    response = _request('DELETE', url + '/' + container + '/blocks_' + str(name), headers = headers)
    # return response
    response_dict['status'] = response.status



# Get Block
# @get_event_loop
def get_object(url,token,container,name,response_dict):
    headers = {'X-Auth-Token': token}
    response = _noloop_request_getobj('GET',url + '/' + container + '/blocks_' + str(name),headers = headers)
    # block = yield from response.content.read()

    return response
