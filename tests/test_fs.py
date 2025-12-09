import unittest
import tempfile
import io
from testcontainers.sftp import SFTPContainer
from pathlib import Path
from sftpovw.fs import FS


class TestFS(unittest.TestCase):
    def test_hash_local(self):
        tf = tempfile.NamedTemporaryFile("r+b")
        tf.write(b"hello world")
        tf.flush()
        res = FS.hash_local([Path(tf.name)])
        self.assertEqual(["2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"], list(res.values()))

    def test_put_safeN(self):
        with SFTPContainer() as sftp:
            fs = FS(
                hostname=sftp.get_container_host_ip(),
                port=sftp.get_exposed_sftp_port(),
                username="basic",
                password="password",
            )
            for level in (0, 1, 2, 3):
                fp = io.BytesIO(b"hello world\n")
                opath = Path("upload/hello.txt")
                fs.put(fp, opath, level=level)
                with tempfile.NamedTemporaryFile("r+") as tmpf:
                    fs.get(opath, Path(tmpf.name), level=level)
                    self.assertEqual("hello world\n", Path(tmpf.name).read_text())
