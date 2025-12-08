import click
import functools
import paramiko
import os
import json
from logging import getLogger
from .fs import FS
from .version import VERSION

_log = getLogger(__name__)


@click.group(invoke_without_command=True)
@click.pass_context
@click.version_option(version=VERSION, prog_name="requestsml")
def cli(ctx):
    if ctx.invoked_subcommand is None:
        print(ctx.get_help())


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
    @click.option("--identity-file", type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True))
    @click.option("--ssh-config", type=click.Path(), default="~/.ssh/config")
    @functools.wraps(func)
    def _(host, identity_file, ssh_config, **kwargs):
        conf = paramiko.SSHConfig().from_path(os.path.expanduser(ssh_config))
        cfg = conf.lookup(host)
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
            ssh.load_system_host_keys()
            ssh.connect(
                hostname=cfg["hostname"],
                username=cfg.get("user"),
                key_filename=identity_file or cfg.get("identityfile"),
                port=int(cfg.get("port", 22)),
            )
            return func(client=ssh, **kwargs)

    return _


def copy_option(func):
    @click.argument("remote-file")
    @click.argument("local-file")
    @click.option("--level", type=int, default=3)
    @functools.wraps(func)
    def _(**kwargs):
        return func(**kwargs)

    return _


@cli.command()
@verbose_option
@ssh_option
@copy_option
def get(client: paramiko.SSHClient, remote_file: str, local_file: str, level):
    fs = FS(client=client)
    fs.get(remotepath=remote_file, localpath=local_file, level=level)


@cli.command()
@verbose_option
@ssh_option
@copy_option
def put(client: paramiko.SSHClient, remote_file: str, local_file: str, level):
    fs = FS(client=client)
    with open(local_file) as fp:
        fs.put(fp=fp, remotepath=remote_file, level=level)


@cli.command()
@verbose_option
@ssh_option
@click.argument("filenames", nargs=-1)
def checksum(client: paramiko.SSHClient, filenames: tuple[str]):
    fs = FS(client=client)
    click.echo(json.dumps(fs.hash(filenames)))


@cli.command()
@verbose_option
@ssh_option
@click.argument("filename")
def listtmp_remote(client: paramiko.SSHClient, filename: str):
    fs = FS(client=client)
    click.echo(json.dumps(fs.listtmp(filename)))


if __name__ == "__main__":
    cli()
