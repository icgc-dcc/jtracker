import click


@click.command()
@click.pass_context
def list(ctx):
    """
    Listing registered workflows
    """
    click.echo('workflow list subcommand')
    click.echo(ctx.obj)


class WorkflowClient(object):
    pass
