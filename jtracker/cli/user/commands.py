import click
import requests
import json
from jtracker.exceptions import AMSNotAvailable, AccountNameNotFound


@click.command()
@click.pass_context
def ls(ctx):
    """
    Listing users
    """
    click.echo('Not implemented yet: user list subcommand')


@click.command()
@click.option('-u', '--user', required=True, help='User name')
# @click.option('-e', '--email', required=True, help='User email')
@click.pass_context
def signup(ctx, user):
    """
    Sign up as a new user
    """
    ams_url = ctx.obj.get('JT_CONFIG').get('ams_server')

    url = "%s/accounts" % ams_url

    r = requests.post(url=url, json={
        "account_type": "user",
        "name": user
    })

    if r.status_code != 200:
        click.echo('Account sign up failed: %s' % r.text)
        ctx.abort()
    else:
        rv = json.loads(r.text)
        click.echo("Account sign up succeeded")
        click.echo("user_name: %s\nuser_id: %s" %(rv.get('name'), rv.get('id')))


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
