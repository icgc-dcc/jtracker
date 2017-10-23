import json
import click
from jtracker import __version__ as ver
from .user import commands as user_commands
from .org import commands as org_commands
from .wf import commands as wf_commands
from .queue import commands as queue_commands
from .job import commands as job_commands
from .task import commands as task_commands
from .exec import commands as exec_commands

from .config import Config


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo('JTracker cli %s' % ver)
    ctx.exit()


@click.group()
@click.option('--config-file', '-c', envvar='JT_CONFIG_FILE', type=click.Path(exists=True),
              default='.jt/config', help='JTracker configuration file', required=False)
@click.option('--version', '-v', is_flag=True, callback=print_version, expose_value=False,
              help='Show JTracker version', is_eager=True)
@click.pass_context
def main(ctx, config_file):
    # initialize configuration from config_file
    try:
        jt_config = Config(config_file).dict
    except Exception as err:
        click.echo(str(err))
        ctx.abort()

    # initializing ctx.obj
    ctx.obj = {
        'JT_CONFIG_FILE': config_file,
        'JT_CONFIG': jt_config
    }


@main.command()
@click.pass_context
def config(ctx):
    """
    View or update JT cli configuration
    """
    click.echo(json.dumps(ctx.obj.get('JT_CONFIG')))
    click.echo('*** other features to be implemented ***')


@main.group()
@click.pass_context
def user(ctx):
    """
    Commands related to user
    """
    pass


# user subcommands
user.add_command(user_commands.list)
user.add_command(user_commands.login)
user.add_command(user_commands.whoami)
user.add_command(user_commands.signup)
user.add_command(user_commands.delete)
user.add_command(user_commands.update)


@main.group()
@click.pass_context
def org(ctx):
    """
    Commands related to organization
    """
    pass


# org subcommands
org.add_command(org_commands.list)


@main.group()
@click.pass_context
def wf(ctx):
    """
    Commands related to workflow
    """
    pass


wf.add_command(wf_commands.list)


@main.group()
@click.pass_context
def queue(ctx):
    """
    Commands related to queue
    """
    pass


queue.add_command(queue_commands.list)


@main.group()
@click.pass_context
def job(ctx):
    """
    Commands related to job
    """
    pass


job.add_command(job_commands.list)


@main.group()
@click.pass_context
def task(ctx):
    """
    Commands related to executor
    """
    pass

# exec subcommands
task.add_command(task_commands.list)


@main.group()
@click.pass_context
def exec(ctx):
    """
    Commands related to executor
    """
    pass

# exec subcommands
exec.add_command(exec_commands.start)


if __name__ == '__main__':
    main()
