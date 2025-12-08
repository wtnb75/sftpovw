import unittest
import tempfile
from pathlib import Path
from sftpovw.fs import FS


class Test1(unittest.TestCase):
    def test_1(self):
        tf = tempfile.NamedTemporaryFile("r+b")
        tf.write(b"hello world")
        tf.flush()
        res = FS.hash_local([Path(tf.name)])
        self.assertEqual(["2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"], list(res.values()))
