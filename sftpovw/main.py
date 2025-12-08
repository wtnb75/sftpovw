import click
import functools
import paramiko
import os
import json
from logging import getLogger
from pathlib import Path
from .fs import FS
from .version import VERSION

_log = getLogger(__name__)


@click.group(invoke_without_command=True)
@click.pass_context
@click.version_option(version=VERSION, prog_name="requestsml")
def cli(ctx):
    if ctx.invoked_subcommand is None:
        print(ctx.get_help())


class PathParamType(click.ParamType):
    name = "path"

    def convert(self, value, param, ctx):
        return Path(value)


def verbose_option(func):
    @click.option("--verbose/--quiet", default=None)
    @functools.wraps(func)
    def _(verbose: bool | None, **kwargs):
        from logging import basicConfig

        fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
        if verbose is None:
            basicConfig(level="INFO", format=fmt)
        elif verbose is False:
            basicConfig(level="WARNING", format=fmt)
        else:
            basicConfig(level="DEBUG", format=fmt)
        return func(**kwargs)

    return _


def ssh_option(func):
    @click.option("--host", required=True)
    @click.option("--identity-file", type=PathParamType())
    @click.option("--ssh-config", type=PathParamType(), default="~/.ssh/config")
    @functools.wraps(func)
    def _(host, identity_file: Path | None, ssh_config: Path, **kwargs):
        conf = paramiko.SSHConfig().from_path(os.path.expanduser(ssh_config))
        cfg = conf.lookup(host)
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
            ssh.load_system_host_keys()
            idf = identity_file and str(identity_file) or cfg.get("identityfile")
            ssh.connect(
                hostname=cfg["hostname"],
                username=cfg.get("user"),
                key_filename=idf,
                port=int(cfg.get("port", 22)),
            )
            return func(client=ssh, **kwargs)

    return _


@cli.command()
@verbose_option
@ssh_option
@click.option("--level", type=int, default=3)
@click.argument("remote-files", type=PathParamType(), nargs=-1)
@click.argument("local", type=PathParamType())
def get(client: paramiko.SSHClient, remote_files: tuple[Path, ...], local: Path, level):
    """get files from remote"""
    fs = FS(client=client)

    for rf in remote_files:
        if local.is_dir():
            ofn = local / rf.name
            fs.get(remotepath=rf, localpath=ofn, level=level)
        else:
            fs.get(remotepath=rf, localpath=local, level=level)


@cli.command()
@verbose_option
@ssh_option
@click.option("--level", type=int, default=3)
@click.argument("local-files", type=PathParamType(), nargs=-1)
@click.argument("remote", type=PathParamType())
def put(client: paramiko.SSHClient, local_files: tuple[Path, ...], remote: Path, level):
    """put files to remote"""
    fs = FS(client=client)
    is_dir = fs.is_dir(remote)
    for lf in local_files:
        with open(lf, "rb") as fp:
            if is_dir:
                remote_path = remote / lf.name
                fs.put(fp=fp, remotepath=remote_path, level=level)
            else:
                fs.put(fp=fp, remotepath=remote, level=level)


@cli.command()
@verbose_option
@ssh_option
@click.argument("filenames", type=PathParamType(), nargs=-1)
def checksum(client: paramiko.SSHClient, filenames: tuple[Path]):
    """checksum remote files"""
    fs = FS(client=client)
    click.echo(json.dumps(fs.hash(filenames)))


@cli.command()
@verbose_option
@click.argument("filenames", type=PathParamType(), nargs=-1)
def checksum_local(filenames: tuple[Path]):
    """checksum local files"""
    click.echo(json.dumps(FS.hash_local(filenames)))


@cli.command()
@verbose_option
@ssh_option
@click.argument("filename", type=PathParamType())
def listtmp(client: paramiko.SSHClient, filename: Path):
    """list remote temporary garbage"""
    fs = FS(client=client)
    click.echo(json.dumps(fs.listtmp(filename)))


@cli.command()
@verbose_option
@click.argument("filename", type=PathParamType())
def listtmp_local(filename: Path):
    """list local temporary garbage"""
    click.echo(json.dumps(FS.listtmp_local(filename)))


if __name__ == "__main__":
    cli()
