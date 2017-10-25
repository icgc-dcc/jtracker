import click
import json
import requests


@click.command()
@click.option('-q', '--queue-id', required=True, help='Job queue ID')
@click.option('-o', '--owner', help='Queue owner account name')
@click.option('-s', '--status', help='Job status',type=click.Choice(
    ['running', 'queued', 'completed', 'failed', 'suspended', 'cancelled']))
@click.pass_context
def ls(ctx, queue_id, status, owner):
    """
    Listing workflow jobs in specified queue
    """
    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    owner = owner if owner else ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/jobs/owner/%s/queue/%s" % (jess_url, owner, queue_id)

    if status:
        url = url + '?state=%s' % status

    r = requests.get(url)

    if r.status_code != 200:
        click.echo('List job for: %s failed: %s' % (owner, r.text))
        ctx.abort()
    else:
        try:
            rv = json.loads(r.text)
            if isinstance(rv, (list, tuple)):
                for j in rv:
                    if ctx.obj.get('JT_WRITE_OUT') == 'simple':
                        click.echo("job_id: %s, status: %s" % (j.get('id'), j.get('state')))
                    elif ctx.obj.get('JT_WRITE_OUT') == 'json':
                        click.echo(json.dumps(j))
            else:
                click.echo(rv)
        except Exception as err:
            click.echo(r.text)


@click.command()
@click.option('-q', '--queue-id', required=True, help='Job queue ID')
@click.option('-j', '--job-id', required=True, help='Job ID')
@click.option('-o', '--queue-owner', help='Queue owner account name')
@click.option('-s', '--status', help='Job status',type=click.Choice(
    ['running', 'queued', 'completed', 'failed', 'suspended', 'cancelled']))
@click.pass_context
def get(ctx, queue_id, status, job_id, queue_owner):
    """
    Get workflow job in specified queue with specified job_id
    """

    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    queue_owner = queue_owner if queue_owner else ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/jobs/owner/%s/queue/%s/job/%s" % (jess_url, queue_owner, queue_id, job_id)

    if status:
        url = url + '?state=%s' % status

    r = requests.get(url)
    if r.status_code != 200:
        click.echo('Get job for: %s failed: %s' % (queue_owner, r.text))
        ctx.abort()
    else:
        click.echo(r.text)


@click.command()
@click.option('-q', '--queue-id', required=True, help='Job queue ID')
@click.option('-j', '--job-json', required=True, help='Job JSON')
@click.option('-o', '--queue-owner', help='Queue owner account name')
@click.pass_context
def add(ctx, queue_id, job_json, queue_owner):
    """
    Enqueue new job to specified queue
    """

    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    queue_owner = queue_owner if queue_owner else ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/jobs/owner/%s/queue/%s" % (jess_url, queue_owner, queue_id)

    job = json.loads(job_json)

    r = requests.post(url=url, json=job)
    if r.status_code != 200:
        click.echo('Enqueue job for: %s failed: %s' % (queue_owner, r.text))
        ctx.abort()
    else:
        click.echo('Enqueue job for: %s into queue: %s succeeded' % (queue_owner, queue_id))
        click.echo(r.text)
