import click
import requests
import json
from jtracker.exceptions import AMSNotAvailable, AccountNameNotFound


@click.command()
@click.pass_context
def list(ctx):
    """
    Listing users
    """
    click.echo('user list subcommand')
    click.echo(ctx.obj)


@click.command()
@click.option('-u', '--user', required=True, help='User name')
@click.option('-e', '--email', required=True, help='User email')
@click.pass_context
def signup(ctx):
    """
    Sign up as a new user
    """
    click.echo('user signup subcommand')
    click.echo(ctx.obj)


@click.command()
@click.pass_context
def login(ctx):
    """
    User log in
    """
    click.echo('user login subcommand')
    click.echo(ctx.obj)


@click.command()
@click.pass_context
def whoami(ctx):
    """
    Get the current user
    """
    click.echo('user whoami subcommand')
    click.echo(ctx.obj)


@click.command()
@click.pass_context
def delete(ctx):
    """
    Delete a user
    """
    click.echo('user delete subcommand')
    click.echo(ctx.obj)


@click.command()
@click.pass_context
def update(ctx):
    """
    Update user info
    """
    click.echo('user update subcommand')
    click.echo(ctx.obj)


class UserClient(object):
    def __init__(self, jt_home=None, jt_account=None, ams_server=None):
        self._jt_home = jt_home
        self._jt_account = jt_account
        self._ams_server = ams_server

    @property
    def jt_home(self):
        return self._jt_home

    @property
    def jt_account(self):
        return self._jt_account

    @property
    def ams_server(self):
        return self._ams_server

    def create(self, user_name=None):
        print('create user: %s' % user_name)
        print('not implemented yet')
