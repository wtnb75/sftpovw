from logging import getLogger
from typing import Iterable, BinaryIO
from pathlib import Path
import os
import time
import tempfile
import paramiko
import shlex
import glob
import secrets
import hashlib
import stat

_log = getLogger(__name__)


class FS:
    HASH_ALGO = "sha1"  # normally "sha1" or "md5"

    def __init__(self, host: str | None = None, *, client: paramiko.SSHClient | None = None, **kwargs):
        getLogger("paramiko").setLevel("INFO")
        if host:
            conf = paramiko.SSHConfig().from_path(os.path.expanduser("~/.ssh/config"))
            cfg = conf.lookup(host)
            self.client = paramiko.SSHClient()
            self.client.load_system_host_keys()
            self.client.set_missing_host_key_policy(paramiko.WarningPolicy())
            self.client.connect(
                hostname=cfg["hostname"],
                username=cfg.get("user"),
                key_filename=cfg.get("identityfile"),
                port=int(cfg.get("port", 22)),
            )
        elif client:
            self.client = client
        else:
            self.client = paramiko.SSHClient()
            self.client.load_system_host_keys()
            self.client.set_missing_host_key_policy(paramiko.WarningPolicy())
            self.client.connect(**kwargs)
        self.sftp = self.client.open_sftp()

    def __del__(self):
        if hasattr(self, "sftp"):
            self.sftp.close()
            del self.sftp
        if hasattr(self, "client"):
            self.client.close()
            del self.client

    def list(self, path: Path):
        return self.sftp.listdir(str(path))

    def put_safe0(self, fp: BinaryIO, remotepath: Path, file_size: int = 0):
        # put-overwrite
        _log.info("put %s", remotepath)
        attr = self.sftp.putfo(fl=fp, remotepath=str(remotepath), file_size=file_size)
        return attr.st_size

    def put_safe1(self, fp: BinaryIO, remotepath: Path, file_size: int = 0):
        # remove, put
        if self.exists(remotepath):
            _log.info("remove %s", remotepath)
            self.sftp.unlink(str(remotepath))
        _log.info("put %s", remotepath)
        attr = self.sftp.putfo(fl=fp, remotepath=str(remotepath), file_size=file_size)
        return attr.st_size

    def put_safe2(self, fp: BinaryIO, remotepath: Path, file_size: int = 0):
        # rename-tmp, put, delete-tmp
        tmpfn = self.tmpfile(remotepath)
        exists = self.exists(remotepath)
        if exists:
            _log.info("rename %s -> %s", remotepath, tmpfn)
            self.sftp.posix_rename(str(remotepath), str(tmpfn))
        _log.info("put %s", remotepath)
        attr = self.sftp.putfo(fp, remotepath=str(remotepath), file_size=file_size)
        if exists:
            _log.info("unlink %s", tmpfn)
            self.sftp.unlink(str(tmpfn))
        return attr.st_size

    def put_safe3(self, fp: BinaryIO, remotepath: Path, file_size: int = 0):
        # put-tmp, rename
        tmpfn = self.tmpfile(remotepath)
        _log.info("put %s", tmpfn)
        attr = self.sftp.putfo(fp, remotepath=str(tmpfn), file_size=file_size)
        _log.info("rename %s -> %s", tmpfn, remotepath)
        self.sftp.posix_rename(str(tmpfn), str(remotepath))
        return attr.st_size

    def put_safe4(self, fp: BinaryIO, remotepath: Path, file_size: int = 0):
        # put-tmp1, rename-tmp2, rename, unlink
        exists = self.exists(remotepath)
        tmpfn1 = self.tmpfile(remotepath)
        tmpfn2 = self.tmpfile(remotepath)
        _log.info("put %s", tmpfn1)
        attr = self.sftp.putfo(fp, remotepath=str(tmpfn1), file_size=file_size)
        if exists:
            _log.info("rename %s -> %s", remotepath, tmpfn2)
            self.sftp.posix_rename(str(remotepath), str(tmpfn2))
        _log.info("rename %s -> %s", tmpfn1, remotepath)
        self.sftp.posix_rename(str(tmpfn1), str(remotepath))
        if exists:
            _log.info("unlink %s", tmpfn2)
            self.sftp.unlink(str(tmpfn2))
        return attr.st_size

    def get_safe0(self, remotepath: Path, localpath: Path):
        # get-overwrite
        _log.info("get %s -> %s", remotepath, localpath)
        self.sftp.get(remotepath=str(remotepath), localpath=localpath)
        return localpath.stat().st_size

    def get_safe1(self, remotepath: Path, localpath: Path):
        # remove, get
        if os.path.exists(localpath):
            _log.info("unlink(local) %s", localpath)
            os.unlink(localpath)
        _log.info("get %s -> %s", remotepath, localpath)
        self.sftp.get(remotepath=str(remotepath), localpath=localpath)
        return localpath.stat().st_size

    def get_safe2(self, remotepath: Path, localpath: Path):
        # rename-tmp, get, delete-tmp
        exists = os.path.exists(localpath)
        if exists:
            tmpfn = self.tmpfile_local(localpath)
            _log.info("rename(local) %s -> %s", localpath, tmpfn)
            os.rename(localpath, tmpfn)
        _log.info("get %s -> %s", remotepath, localpath)
        self.sftp.get(remotepath=str(remotepath), localpath=localpath)
        if exists:
            _log.info("unlink(local) %s -> %s", tmpfn)
            os.unlink(tmpfn)
        return localpath.stat().st_size

    def get_safe3(self, remotepath: Path, localpath: Path):
        # get-tmp, rename
        tmpfn = self.tmpfile_local(localpath)
        _log.info("get %s -> %s", remotepath, tmpfn)
        self.sftp.get(remotepath=str(remotepath), localpath=tmpfn)
        _log.info("rename(local) %s -> %s", tmpfn, localpath)
        os.rename(tmpfn, localpath)
        return localpath.stat().st_size

    def get_safe4(self, remotepath: Path, localpath: Path):
        # get-tmp1, rename-tmp2, rename, unlink
        exists = os.path.exists(localpath)
        tmpfn1 = self.tmpfile_local(localpath)
        _log.info("get %s -> %s", remotepath, tmpfn1)
        self.sftp.get(remotepath=str(remotepath), localpath=tmpfn1)
        if exists:
            tmpfn2 = self.tmpfile_local(localpath)
            _log.info("rename(local) %s -> %s", localpath, tmpfn2)
            os.rename(localpath, tmpfn2)
        _log.info("rename(local) %s -> %s", tmpfn1, localpath)
        os.rename(tmpfn1, localpath)
        if exists:
            _log.info("unlink(local) %s -> %s", tmpfn2)
            os.unlink(tmpfn2)
        return localpath.stat().st_size

    def put(self, fp: BinaryIO, remotepath: Path, file_size: int = 0, level: int = 3) -> int:
        st = time.time()
        try:
            res = getattr(self, f"put_safe{level}")(fp, remotepath, file_size)
            elapsed = time.time() - st
            _log.info("put%d %d bytes in %.f second (%.f Bytes/s)", level, res, res / elapsed)
            return res
        except Exception:
            elapsed = time.time() - st
            _log.exception("put%d failed in %.f second", level, elapsed)
            raise

    def get(self, remotepath: Path, localpath: Path, level: int = 3) -> int:
        st = time.time()
        try:
            res = getattr(self, f"get_safe{level}")(remotepath, localpath)
            elapsed = time.time() - st
            _log.info("get%d %d bytes in %.f second (%.f Bytes/s)", level, res, res / elapsed)
            return res
        except Exception:
            elapsed = time.time() - st
            _log.exception("get%d failed in %.f second", level, elapsed)
            raise

    def hash(self, paths: Iterable[Path]) -> dict[str, str]:
        try:
            # Many (most?) servers don’t support this extension yet.
            # normally "sha1" or "md5"
            # https://docs.paramiko.org/en/stable/api/sftp.html#paramiko.sftp_file.SFTPFile.check
            # Currently defined algorithms are "md5", "sha1", "sha224", "sha256", "sha384", "sha512", and "crc32"
            # https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-extensions-00#section-3
            _log.info("hash by sftp protocol: %s", paths)
            res = {}
            for path in paths:
                with self.sftp.file(str(path)) as fp:
                    res[path] = fp.check(self.HASH_ALGO).hex()
            return res
        except IOError:
            return self.hash_bycmd(paths)

    def hash_bycmd(self, paths: Iterable[Path]) -> dict[str, str]:
        _log.info("hash by cmd: %s", paths)
        cmdstr = shlex.join(["sha1sum", *[str(x) for x in paths]])
        stdin, stdout, stderr = self.client.exec_command(cmdstr)
        stdin.close()
        res = {}
        stdout_str = stdout.read()
        exit_code = stdout.channel.recv_exit_status()
        if exit_code == 127:
            raise ValueError(f"command not found: {stdout_str}")
        for line in stdout_str.splitlines():
            val, fn = line.strip().split(maxsplit=1)
            res[fn] = val
        if exit_code == 1:
            _log.error("file not found? stderr=%s", stderr.read())
        elif exit_code != 0:
            _log.warning("unknown error(exit %s): stderr=%s", exit_code, stderr.read())
            if not res:
                raise Exception(f"unknown error({exit_code}): stderr={stderr.read()}")
        return res

    def exists(self, path: Path) -> bool:
        try:
            self.sftp.stat(str(path))
            return True
        except FileNotFoundError:
            return False

    def is_dir(self, path: Path) -> bool:
        try:
            st = self.sftp.stat(str(path))
            if not st or not st.st_mode:
                return False
            return bool(st.st_mode & stat.S_IFDIR)
        except FileNotFoundError:
            return False

    def tmpfile(self, path: Path) -> Path:
        for _ in range(10):
            suffix = secrets.token_hex(10)
            name = path.with_suffix(path.suffix + "." + suffix)
            if not self.exists(name):
                return name
        raise Exception("cannot create tmpfile?")

    def listtmp(self, path: Path) -> list[Path]:
        name = self.tmpfile(path)
        tmplen = len(str(name))
        res = []
        dirname = path.parent
        basename = path.name
        for i in self.sftp.listdir(str(dirname)):
            if len(i) == tmplen and i.startswith(basename + "."):
                res.append(dirname / i)
        return res

    @classmethod
    def tmpfile_local(cls, path: Path) -> Path:
        dirname = path.parent
        basename = path.name
        res = tempfile.mkstemp(prefix=basename + ".", dir=dirname)
        os.close(res[0])
        return Path(res[1])

    @classmethod
    def listtmp_local(cls, path: Path) -> list[Path]:
        name = cls.tmpfile_local(path)
        tmplen = len(str(name))
        os.unlink(name)
        res = []
        for i in glob.glob(str(path) + ".*"):
            if len(i) == tmplen:
                res.append(path.parent / i)
        return res

    @classmethod
    def hash_local(cls, paths: Iterable[Path]) -> dict[str, str]:
        res = {}
        for path in paths:
            with open(path, "rb") as ifp:
                res[str(path)] = hashlib.file_digest(ifp, cls.HASH_ALGO).hexdigest()
        return res
