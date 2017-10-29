import click
from jtracker.execution import Executor


@click.command()
@click.option('-q', '--queue-id', help='Job queue ID')
@click.option('-e', '--executor-id', help='Specify executor ID to resume interrupted execution')
@click.option('-k', '--parallel-workers', type=int, default=2, help='Max number of parallel workers')
@click.option('-p', '--parallel-jobs', type=int, default=1, help='Max number of parallel running jobs')
@click.option('-m', '--max-jobs', type=int, default=0, help='Max number of jobs to be run by the executor')
@click.option('-i', '--job-id', help='Execute specified job')
@click.option('-j', '--job-file', type=click.Path(exists=True), help='Execute local job file')
@click.option('-w', '--workflow-name', help='Specify registered workflow name in format: [{owner}/]{workflow}:{ver}')
@click.option('-c', '--continuous-run', is_flag=True, help='Keep executor running even job queue is empty')
@click.pass_context
def run(ctx, job_file, job_id, queue_id, executor_id,
             workflow_name, parallel_jobs, max_jobs, parallel_workers, continuous_run):
    """
    Launch JTracker executor
    """
    jt_executor = None
    try:
        jt_executor = Executor(jt_home=ctx.obj['JT_CONFIG'].get('jt_home'),
                               jt_account=ctx.obj['JT_CONFIG'].get('jt_account'),
                               ams_server=ctx.obj['JT_CONFIG'].get('ams_server'),
                               wrs_server=ctx.obj['JT_CONFIG'].get('wrs_server'),
                               jess_server=ctx.obj['JT_CONFIG'].get('jess_server'),
                               job_file=job_file,
                               job_id=job_id,
                               executor_id=executor_id,
                               queue_id=queue_id,
                               workflow_name=workflow_name,
                               parallel_jobs=parallel_jobs,
                               max_jobs=max_jobs,
                               parallel_workers=parallel_workers,
                               continuous_run=continuous_run
                               )
    except Exception as e:
        click.echo(str(e))
        click.echo('For usage: jt exec run --help')
        ctx.abort()

    if jt_executor:
        jt_executor.run()


@click.command()
@click.pass_context
def list(ctx, job_file, job_id, queue_id, executor_id):
    pass
