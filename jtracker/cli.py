import os
import click
from click import echo
from .worker import Worker
from .jtracker import JTracker


@click.group()
@click.option('--git-repo-url', '-g', envvar='JT_GIT_URL', required=True)
@click.option('--workflow-name', '-n', envvar='JT_WORKFLOW', required=True)
@click.option('--workflow-version', '-w', envvar='JT_WKFL_VER', required=True)
@click.pass_context
def main(ctx, git_repo_url, workflow_name, workflow_version):
    # initializing ctx.obj
    ctx.obj = {}
    ctx.obj['JT_GIT_URL'] = git_repo_url
    ctx.obj['JT_WORKFLOW'] = workflow_name
    ctx.obj['JT_WKFL_VER'] = workflow_version


@main.command()
@click.pass_context
def status(ctx):
    """
    Report status.
    """
    echo('To be implimented')
    pass


@main.command()
@click.pass_context
def worker(ctx):
    """
    Start worker.
    """
    jt = JTracker(
                    git_repo_url = ctx.obj['JT_GIT_URL'],
                    workflow_name = ctx.obj['JT_WORKFLOW'],
                    workflow_version = ctx.obj['JT_WKFL_VER']
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

