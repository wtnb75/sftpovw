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
            for level in (0, 1, 2, 3, 4):
                fp = io.BytesIO(b"hello world\n")
                filelen = fp.getbuffer().nbytes
                opath = Path("upload/hello.txt")
                putlen = fs.put(fp, opath, level=level)
                self.assertEqual(filelen, putlen)
                with tempfile.NamedTemporaryFile("r+") as tmpf:
                    getlen = fs.get(opath, Path(tmpf.name), level=level)
                    self.assertEqual("hello world\n", Path(tmpf.name).read_text())
                    self.assertEqual(filelen, getlen)

    def test_isdir(self):
        with SFTPContainer() as sftp:
            fs = FS(
                hostname=sftp.get_container_host_ip(),
                port=sftp.get_exposed_sftp_port(),
                username="keypair",
                key_filename=sftp.users[1].private_key_file,
            )
            self.assertTrue(fs.is_dir(Path("upload")))
            self.assertFalse(fs.is_dir(Path("notfound.txt")))
            fs.put(io.BytesIO(b"hello\n"), Path("upload/test.txt"))
            self.assertFalse(fs.is_dir(Path("upload/test.txt")))
