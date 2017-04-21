import os
import click
from click import echo
from .worker import Worker
from .jtracker import JTracker
from daemonocle.cli import DaemonCLI


@click.group()
@click.pass_context
def main(ctx):
    # initializing ctx.obj
    pass


@main.command()
@click.pass_context
def status(ctx):
    """
    Report status.
    """
    echo('To be implimented')
    pass


#@main.command(cls=DaemonCLI, daemon_params={'pidfile': 'jtracker.pid'})  # we could daemonize the worker
@main.command()
@click.pass_context
def worker(ctx):
    """
    Start worker.
    """
    jt = JTracker(
                    git_repo_url = 'git@github.com:icgc-dcc/jtracker-example-workflows.git',
                    workflow_name = 'ega-file-transfer-to-collab',
                    workflow_version = '0.3.0'
                )

    worker = Worker(jtracker=jt)
    echo('Worker ID: %s' % worker.worker_id)

    while worker.next_task():
        echo('Start run task: %s in job: %s' % (worker.task.name, worker.task.job.job_id))
        if worker.run():
            echo('Completed task: %s in job: %s' % (worker.task.name, worker.task.job.job_id))
            worker.task_completed()
        else:
            echo('Task failed: %s in job: %s' % (worker.task.name, worker.task.job.job_id))
            worker.task_failed()


if __name__ == '__main__':
  main()

