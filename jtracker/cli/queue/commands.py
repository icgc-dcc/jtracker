import click
import requests


@click.command()
@click.pass_context
def ls(ctx):
    """
    Listing workflow queues
    """
    click.echo('queue list subcommand')


@click.command()
@click.option('-n', '--wf-name', required=True, help='Workflow name')
@click.option('-v', '--wf-version', required=True, help='Workflow version')
@click.option('-o', '--wf-owner', help='Workflow owner account name')
@click.pass_context
def add(ctx, wf_name, wf_version, wf_owner):
    """
    Listing workflow queues
    """
    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    if wf_owner is None:
        wf_owner = ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/queues/owner/%s/workflow/%s/ver/%s" % (jess_url, wf_owner, wf_name, wf_version)

    r = requests.post(url)
    if r.status_code != 200:
        click.echo('Queue creation for: %s failed: %s' % (wf_owner, r.text))
        ctx.abort()
    else:
        click.echo("Queue registration succeeded, details as below")
        click.echo(r.text)


class QueueClient(object):
    pass
