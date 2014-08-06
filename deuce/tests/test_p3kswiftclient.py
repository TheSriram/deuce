import deuce.util.p3k_swiftclient as p3k_swiftclient
from deuce.tests.util.mockfile import MockFile

from unittest import TestCase
import mock
import asyncio


class Response(object):

    def __init__(self,status,content=None):
        self.status = status
        self.content = content
        if content:
            fut = asyncio.Future(loop=None)
            fut.set_result(content)
            self.content.read = mock.Mock(return_value=fut)

class Test_P3k_SwiftClient(TestCase):

    def setUp(self):
        self.storage_url = 'http://mock_storage_url.com'
        self.token = 'mock_token'
        self.vault = 'mock_vault'
        self.block = 'mock'
        self.response_dict = dict()

    def test_put_container(self):
        res = Response(201)
        fut = asyncio.Future(loop=None)
        fut.set_result(res)
        p3k_swiftclient.aiohttp.request = mock.Mock(return_value=fut)
        p3k_swiftclient.put_container(self.storage_url,self.token,self.vault,self.response_dict)
        self.assertEqual(self.response_dict['status'],201)

    def test_head_container(self):
        res = Response(200)
        fut = asyncio.Future(loop=None)
        fut.set_result(res)
        p3k_swiftclient.aiohttp.request = mock.Mock(return_value=fut)
        p3k_swiftclient.head_container(self.storage_url,self.token,self.vault,self.response_dict)
        self.assertEqual(self.response_dict['status'],200)

    def test_delete_container(self):
        res = Response(204)
        fut = asyncio.Future(loop=None)
        fut.set_result(res)
        p3k_swiftclient.aiohttp.request = mock.Mock(return_value=fut)
        p3k_swiftclient.delete_container(self.storage_url,self.token,self.vault,self.response_dict)
        self.assertEqual(self.response_dict['status'],204)

    def test_put_object(self):
        res = Response(204)
        fut = asyncio.Future(loop=None)
        fut.set_result(res)
        p3k_swiftclient.aiohttp.request = mock.Mock(return_value=fut)
        p3k_swiftclient.put_object(self.storage_url,self.token,self.vault,self.block,'mock','4',self.response_dict,etag=None)
        self.assertEqual(self.response_dict['status'],204)
        p3k_swiftclient.put_object(self.storage_url,self.token,self.vault,'mock','mock','4',self.response_dict,etag='mock')
        self.assertEqual(self.response_dict['status'],204)

    def test_head_object(self):
        res = Response(204)
        fut = asyncio.Future(loop=None)
        fut.set_result(res)
        p3k_swiftclient.aiohttp.request = mock.Mock(return_value=fut)
        p3k_swiftclient.head_object(self.storage_url,self.token,self.vault,'mock',self.response_dict)
        self.assertEqual(self.response_dict['status'],204)

    def test_get_object(self):
        file = MockFile(10)
        r = Response(200,file)
        fut1 = asyncio.Future(loop=None)
        fut1.set_result(r)
        p3k_swiftclient.aiohttp.request = mock.Mock(return_value=fut1)

        response = p3k_swiftclient.get_object(self.storage_url,self.token,self.vault,self.block,self.response_dict)
        self.assertEqual(file,response)

    def test_delete_object(self):
        r = Response(204)
        fut1 = asyncio.Future(loop=None)
        fut1.set_result(r)
        p3k_swiftclient.aiohttp.request = mock.Mock(return_value=fut1)

        p3k_swiftclient.delete_object(self.storage_url,self.token,self.vault,self.block,self.response_dict)
        self.assertEqual(self.response_dict['status'],204)


