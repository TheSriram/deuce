from unittest import TestCase

import ddt
from mock import patch
from wsgiref import simple_server

import deuce
from deuce.transport.wsgi.driver import Driver
import deuce.util.log as logging


@ddt.ddt
class TestDriver(TestCase):

    def test_driver(self):
        class MockServer(object):

            def serve_forever(self):
                LOG = logging.getLogger(__name__)
                LOG.info("Mock Server - Started")

        mock_server_object = MockServer()
        with patch.object(simple_server,
                          'make_server',
                          return_value=mock_server_object):
                app_container = Driver()
                app_container.listen()

    @ddt.data('disk', 'swift')
    def test_hooks(self, hook_set):
        class DummyWsgi(object):

            def __init__(self, *args, **kwargs):
                pass

            def set_header(self, *args, **kwargs):
                pass

        from deuce import conf
        conf.block_storage_driver.driver = hook_set
        dummy_wsgi = DummyWsgi()
        Driver.before_hooks(dummy_wsgi)
