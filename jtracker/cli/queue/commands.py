import click


@click.command()
@click.pass_context
def list(ctx):
    """
    Listing workflow queues
    """
    click.echo('queue list subcommand')
    click.echo(ctx.obj)


class QueueClient(object):
    pass
