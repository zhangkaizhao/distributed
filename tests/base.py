import asyncio
import unittest


class AioTestCase(unittest.TestCase):
    def setUp(self):
        # via https://stackoverflow.com/a/23642269/3449199
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
