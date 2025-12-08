from logging import getLogger
from typing import Iterable
import os
import tempfile
import paramiko
import shlex
import glob
import secrets

_log = getLogger(__name__)


class FS:
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

    def list(self, path):
        return self.sftp.listdir(path)

    def put_unsafe(self, fp, remotepath: str, file_size: int = 0):
        # put-overwrite
        _log.info("put %s", remotepath)
        self.sftp.putfo(fl=fp, remotepath=remotepath, file_size=file_size)

    def put_safe1(self, fp, remotepath: str, file_size: int = 0):
        # remove, put
        if self.exists(remotepath):
            _log.info("remove %s", remotepath)
            self.sftp.unlink(remotepath)
        _log.info("put %s", remotepath)
        self.sftp.putfo(fl=fp, remotepath=remotepath, file_size=file_size)

    def put_safe2(self, fp, remotepath: str, file_size: int = 0) -> None:
        # rename-tmp, put, delete-tmp
        tmpfn = self.tmpfile(remotepath)
        exists = self.exists(remotepath)
        if exists:
            _log.info("rename %s -> %s", remotepath, tmpfn)
            self.sftp.rename(remotepath, tmpfn)
        _log.info("put %s", remotepath)
        self.sftp.putfo(fp, remotepath=remotepath, file_size=file_size)
        if exists:
            _log.info("unlink %s", tmpfn)
            self.sftp.unlink(tmpfn)

    def put_safe3(self, fp, remotepath: str, file_size: int = 0):
        # put-tmp, rename
        tmpfn = self.tmpfile(remotepath)
        _log.info("put %s", tmpfn)
        self.sftp.putfo(fp, remotepath=tmpfn, file_size=file_size)
        _log.info("rename %s -> %s", tmpfn, remotepath)
        self.sftp.rename(tmpfn, remotepath)

    def put_safe4(self, fp, remotepath: str, file_size: int = 0):
        # put-tmp1, rename-tmp2, rename, unlink
        exists = self.exists(remotepath)
        tmpfn1 = self.tmpfile(remotepath)
        tmpfn2 = self.tmpfile(remotepath)
        _log.info("put %s", tmpfn1)
        self.sftp.putfo(fp, remotepath=tmpfn1, file_size=file_size)
        if exists:
            _log.info("rename %s -> %s", remotepath, tmpfn2)
            self.sftp.rename(remotepath, tmpfn2)
        _log.info("rename %s -> %s", tmpfn1, remotepath)
        self.sftp.rename(tmpfn1, remotepath)
        if exists:
            _log.info("unlink %s", tmpfn2)
            self.sftp.unlink(tmpfn2)

    def get_unsafe(self, remotepath: str, localpath: str):
        # get-overwrite
        _log.info("get %s -> %s", remotepath, localpath)
        self.sftp.get(remotepath=remotepath, localpath=localpath)

    def get_safe1(self, remotepath: str, localpath: str):
        # remove, get
        if os.path.exists(localpath):
            _log.info("unlink(local) %s", localpath)
            os.unlink(localpath)
        _log.info("get %s -> %s", remotepath, localpath)
        self.sftp.get(remotepath=remotepath, localpath=localpath)

    def get_safe2(self, fp, remotepath: str, localpath: str):
        # rename-tmp, get, delete-tmp
        exists = os.path.exists(localpath)
        if exists:
            tmpfn = self.tmpfile_local(localpath)
            _log.info("rename(local) %s -> %s", localpath, tmpfn)
            os.rename(localpath, tmpfn)
        _log.info("get %s -> %s", remotepath, localpath)
        self.sftp.get(remotepath=remotepath, localpath=localpath)
        if exists:
            _log.info("unlink(local) %s -> %s", tmpfn)
            os.unlink(tmpfn)

    def get_safe3(self, remotepath: str, localpath: str):
        # get-tmp, rename
        tmpfn = self.tmpfile_local(localpath)
        _log.info("get %s -> %s", remotepath, tmpfn)
        self.sftp.get(remotepath=remotepath, localpath=tmpfn)
        _log.info("rename(local) %s -> %s", tmpfn, localpath)
        os.rename(tmpfn, localpath)

    def get_safe4(self, remotepath: str, localpath: str):
        # get-tmp1, rename-tmp2, rename, unlink
        exists = os.path.exists(localpath)
        tmpfn1 = self.tmpfile_local(localpath)
        _log.info("get %s -> %s", remotepath, tmpfn1)
        self.sftp.get(remotepath=remotepath, localpath=tmpfn1)
        if exists:
            tmpfn2 = self.tmpfile_local(localpath)
            _log.info("rename(local) %s -> %s", localpath, tmpfn2)
            os.rename(localpath, tmpfn2)
        _log.info("rename(local) %s -> %s", tmpfn1, localpath)
        os.rename(tmpfn1, localpath)
        if exists:
            _log.info("unlink(local) %s -> %s", tmpfn2)
            os.unlink(tmpfn2)

    def put(self, fp, remotepath: str, file_size: int = 0, level=3):
        if level == 0:
            return self.put_unsafe(fp, remotepath, file_size)
        return getattr(self, f"put_safe{level}")(fp, remotepath, file_size)

    def get(self, remotepath: str, localpath: str, level=3):
        if level == 0:
            return self.get_unsafe(remotepath, localpath)
        return getattr(self, f"get_safe{level}")(remotepath, localpath)

    def hash(self, paths: Iterable[str]) -> dict[str, str]:
        try:
            # Many (most?) servers don’t support this extension yet.
            # https://docs.paramiko.org/en/stable/api/sftp.html#paramiko.sftp_file.SFTPFile.check
            _log.info("hash by sftp protocol: %s", paths)
            res = {}
            for path in paths:
                with self.sftp.file(path) as fp:
                    res[path] = fp.check("sha1").hex()
            return res
        except IOError:
            return self.hash_bycmd(paths)

    def hash_bycmd(self, paths: Iterable[str]) -> dict[str, str]:
        _log.info("hash by cmd: %s", paths)
        cmdstr = shlex.join(["sha1sum", *paths])
        stdin, stdout, stderr = self.client.exec_command(cmdstr)
        stdin.close()
        res = {}
        for line in stdout.readlines():
            val, fn = line.strip().split(maxsplit=1)
            res[fn] = val
        return res

    def exists(self, path) -> bool:
        try:
            self.sftp.stat(path)
            return True
        except FileNotFoundError:
            return False

    def tmpfile(self, path) -> str:
        for _ in range(10):
            suffix = secrets.token_hex(10)
            name = path + "." + suffix
            if not self.exists(name):
                return name
        raise Exception("cannot create tmpfile?")

    def listtmp(self, path) -> list[str]:
        name = self.tmpfile(path)
        tmplen = len(name)
        res = []
        dirname = os.path.dirname(path) or "."
        basename = os.path.basename(path)
        for i in self.sftp.listdir(dirname):
            if len(i) == tmplen and i.startswith(basename + "."):
                res.append(os.path.join(dirname, i))
        return res

    def tmpfile_local(self, path) -> str:
        dir = os.path.dirname(path) or "."
        base = os.path.basename(path)
        res = tempfile.mkstemp(prefix=base + ".", dir=dir)
        os.close(res[0])
        return res[1]

    def listtmp_local(self, path) -> list[str]:
        name = self.tmpfile_local(path)
        tmplen = len(name)
        os.unlink(name)
        res = []
        for i in glob.glob(path + ".*"):
            if len(i) == tmplen:
                res.append(i)
        return res
