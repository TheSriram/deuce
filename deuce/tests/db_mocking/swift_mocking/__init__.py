import os
from unittest import TestCase
from pecan import set_config
import pecan

import os
import shutil
import six
if six.PY2:
    import deuce.tests.db_mocking.swift_mocking.client
else:
    import deuce.tests.db_mocking.swift_mocking.p3k_swiftclient
