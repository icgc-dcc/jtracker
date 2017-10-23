import click
import requests


@click.command()
@click.option('-o', '--owner', help='Owner account name')
@click.pass_context
def ls(ctx, owner):
    """
    Listing registered workflows
    """
    wrs_url = ctx.obj.get('JT_CONFIG').get('wrs_server')
    if owner is None:
        owner = ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/workflows/owner/%s" % (wrs_url, owner)

    r = requests.get(url)
    if r.status_code != 200:
        click.echo('Workflow for: %s not found: %s' % (owner, r.text))
        ctx.abort()
    else:
        click.echo(r.text)


@click.command()
@click.option('--git-server', required=True, help='Git server url')
@click.option('--git-account', required=True, help='Git account name')
@click.option('--git-repo', required=True, help='Git repository name')
@click.option('--git-path', required=True, help='Git repository path to the workflow code')
@click.option('--git-tag', required=True, help='Git repository release tag')
@click.option('--wf-name', required=True, help='Workflow name')
@click.option('--wf-type', required=True, default='JTracker', help='Workflow type, only support JTracker for now',
              type=click.Choice(['JTracker']))
@click.option('--wf-version', required=True, help='Workflow version')
@click.pass_context
def register(ctx, git_server, git_account, git_repo, git_path, git_tag, wf_name, wf_type, wf_version):
    """
    Register a workflow
    """
    data = {
        "git_server": git_server,
        "git_account": git_account,
        "git_repo": git_repo,
        "git_path": git_path,
        "git_tag": git_tag,
        "name": wf_name,
        "version": wf_version,
        "workflow_type": wf_type
    }

    wrs_url = ctx.obj.get('JT_CONFIG').get('wrs_server')
    user = ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/workflows/owner/%s" % (wrs_url, user)

    r = requests.post(url, json=data)
    if r.status_code != 200:
        click.echo('Workflow registration failed: %s' % r.text)
        ctx.abort()
    else:
        click.echo("Workflow registration succeeded, details as below")
        click.echo(r.text)
