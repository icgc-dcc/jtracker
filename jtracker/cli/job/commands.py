import click


@click.command()
@click.pass_context
def list(ctx):
    """
    Listing workflow jobs
    """
    click.echo('job list subcommand')
    click.echo(ctx.obj)


class JobClient(object):
    pass
