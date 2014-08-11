from deuce.util.event_loop import get_event_loop

from unittest import TestCase

import asyncio


class TestEvent(TestCase):

    def test_eventloop(self):
        @get_event_loop
        def check():
            response = yield from asyncio.sleep(0.1)
            return True
        self.assertTrue(check())
