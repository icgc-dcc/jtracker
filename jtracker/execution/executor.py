import os
import zipfile
import tempfile
from io import BytesIO
import requests
import shutil
import subprocess
import errno
import yaml
import click
import signal
import multiprocessing
from time import sleep
from uuid import uuid4
from .scheduler import JessScheduler
from .scheduler import LocalScheduler
from .worker import Worker


class GracefulKiller:
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        click.echo('Interruption signal received, will exit when the current running job finishes.')
        self.kill_now = True


def work(worker):
    proc_name = multiprocessing.current_process().name
    try:
        rv = worker.run()
        click.echo('Finish task: {} by worker: {}'.format(proc_name, worker.id))
    except:
        # for server mode:
        # if exception happened, worker might not have reported failure to the server
        # need to check the server and report as needed
        pass


class Executor(object):
    def __init__(self, jt_home=None, jt_account=None,
                 ams_server=None, wrs_server=None, jess_server=None,
                 executor_id=None,
                 queue_id=None,
                 workflow_name=None,
                 job_file=None,  # when job_file is provided, it's local mode, no tracking from the server side
                 job_id=None,  # can optionally specify which job to run, not applicable when job_file specified
                 parallel_jobs=1, parallel_workers=1, sleep_interval=5, max_jobs=0, continuous_run=False):

        self._killer = GracefulKiller()

        # TODO: will need to verify jt_account

        # allow user specify executor_id for resuming,
        # TODO: has to check to make sure there is no such executor currently running
        #       need some server side work to support this
        self._id = executor_id if executor_id else str(uuid4())

        self._jt_home = jt_home

        self._parallel_jobs = parallel_jobs
        self._max_jobs = max_jobs
        self._parallel_workers = parallel_workers
        self._sleep_interval = sleep_interval
        self._ran_jobs = 0
        self._continuous_run = continuous_run

        # params for server mode
        if queue_id and job_file is None:
            self._scheduler = JessScheduler(jess_server=jess_server,
                                            wrs_server=wrs_server,
                                            ams_server=ams_server,
                                            jt_account=jt_account,
                                            queue_id=queue_id,
                                            job_id=job_id,  # optionally specify which job to run
                                            executor_id=self.id)

        # local mode if supplied
        elif job_file and queue_id is None:
            self._scheduler = LocalScheduler(job_file=job_file,
                                             workflow_name=workflow_name,
                                             executor_id=self.id)

        else:
            raise Exception('Please specify either queue_id for executing jobs on remote job queue or '
                            'job_file to run local job.')

        self._running_jobs = []
        self._worker_processes = {}

        # init jt_home dir
        self._init_jt_home()

        # init workflow dir
        self._init_workflow_dir()

        # init queue dir
        self._init_queue_dir()

        # init executor dir
        self._init_executor_dir()

        # TODO: check whether previous executor session exists, restore it unless user chose not to (via options)

        click.echo("Executor: %s started." % self.id)

    @property
    def killer(self):
        return self._killer

    @property
    def id(self):
        return self._id

    @property
    def scheduler(self):
        return self._scheduler

    @property
    def jt_home(self):
        return self._jt_home

    @property
    def jt_account(self):
        return self._jt_account

    @property
    def account_id(self):
        return self.scheduler.account_id

    @property
    def node_id(self):
        return self._node_id

    @property
    def node_dir(self):
        return os.path.join(self.jt_home, 'account.%s' % self.account_id, 'node')

    @property
    def workflow_dir(self):
        return os.path.join(self.node_dir,
                            'workflow.%s' % self.scheduler.workflow_id,
                            self.scheduler.workflow_version)

    @property
    def queue_dir(self):
        return os.path.join(self.workflow_dir, 'queue.%s' % self.scheduler.queue_id)

    @property
    def executor_dir(self):
        return os.path.join(self.queue_dir, 'executor.%s' % self.scheduler.executor_id)

    @property
    def sleep_interval(self):
        return self._sleep_interval

    @property
    def parallel_jobs(self):
        return self._parallel_jobs

    @property
    def max_jobs(self):
        return self._max_jobs

    @property
    def parallel_workers(self):
        return self._parallel_workers

    @property
    def running_jobs(self):
        return self._running_jobs

    @property
    def ran_jobs(self):
        return self._ran_jobs

    @property
    def continuous_run(self):
        return self._continuous_run

    @property
    def worker_processes(self):
        return self._worker_processes

    def run(self):
        if self.scheduler.mode == 'local':
            self._run_local()
        else:
            self._run_remote()

    def _run_local(self):
        # TODO: local run is more complicated, let's worry about it later
        click.echo('Run local job not implemented yet.')

    def _run_remote(self):
        # let's register the executor to the queue first
        # TODO: if we are going to support 'resume' an executor, we will need to check JESS for existing executor
        #       and test whether it's resumeable
        while True:
            if self.killer.kill_now:
                click.echo('Received interruption signal, will not pick up new job. Exit after finishing current '
                           'running job (if any) ...')
                break

            if self.max_jobs and self.ran_jobs >= self.max_jobs:
                job_count_text = 'job has' if self.max_jobs == 1 else 'jobs have'
                click.echo('Total number of executed %s reached preset limit of %s, relevant tasks are either '
                           'finished or scheduled, executor will exit after running tasks finish ...' %
                           (job_count_text, self.max_jobs)
                           )

                click.echo("Current running jobs: %s, running tasks: %s" % self._get_run_status())
                break

            if self.scheduler.running_jobs() and len(self.scheduler.running_jobs()) >= self.parallel_jobs:
                # basically we check again to see if running jobs drop to under the parallel limit
                # detail: we need to worry about possible run-away job, job appears to be running but worker died
                #         already, is that possible if executor is still alive? Can executor report the state of a
                #         task ran by a worker whose process exited with error?
                click.echo('Reached limit for parallel running jobs, will start new job after completing a current job.')
                click.echo("Current running jobs: %s, running tasks: %s" % self._get_run_status())

                sleep(self.sleep_interval)
                continue

            worker = Worker(jt_home=self.jt_home, account_id=self.account_id,
                            scheduler=self.scheduler, node_id=self.node_id)

            # get a task from a new job, break if no task returned, which suggests there is no more job
            if not worker.next_task(job_state='queued'):
                if self.continuous_run:
                    click.echo('No job in the queue, will start new job as it arrives.')
                    click.echo("Current running jobs: %s, running tasks: %s" % self._get_run_status())
                    sleep(self.sleep_interval)  # TODO: may want to have a smarter wait intervals
                    continue
                else:
                    click.echo('No job in the queue. Exit after finishing current running job (if any) ...')
                    click.echo("Current running jobs: %s, running tasks: %s" % self._get_run_status())
                    break

            # start the task
            p = multiprocessing.Process(target=work,
                                        name='task:%s job:%s' % (worker.task.get('name'), worker.task.get('job.id')),
                                        args=(worker,)
                                        )

            self._worker_processes[worker.task.get('job.id')] = [p]
            p.start()

            # this is the first task of a new job
            self._ran_jobs += 1
            click.echo('Executor: %s starts no. %s job' % (self.id, self.ran_jobs))

            shutdown = False
            # stay in this loop when there are tasks to be run related to current running jobs
            while self.scheduler.has_next_task():
                if self.killer.kill_now:
                    click.echo(
                        'Received interruption signal, will not pick up new task. Exit when current running task(s) '
                        'finishes...')
                    shutdown = True
                    break

                running_jobs, running_workers = self._get_run_status()

                click.echo('Current running jobs: %s, running tasks: %s' % (running_jobs, running_workers))

                if running_workers < self.parallel_workers:
                    worker = Worker(jt_home=self.jt_home, account_id=self.account_id,
                                    scheduler=self.scheduler, node_id=self.node_id)
                    try:
                        task = worker.next_task(job_state='running')  # get next task in the current running jobs
                    except:
                        pass  # for whatever reason next_task call failed, TODO: add count for failed attempts, fail the job when next_task failed too many times
                    if not task:  # else try to start task for next job if it's appropriate to do so
                        if not (self.max_jobs and self.ran_jobs >= self.max_jobs) and \
                                        not len(self.scheduler.running_jobs()) >= self.parallel_jobs:
                            try:
                                task = worker.next_task(job_state='queued')
                                if task:
                                    self._ran_jobs += 1
                                    click.echo('Executor: %s starts no. %s job' % (self.id, self.ran_jobs))
                            except:
                                pass  # no need to do anything, except maybe counting of failures
                    if task:
                        p = multiprocessing.Process(target=work,
                                                name='task:%s job:%s' % (worker.task.get('name'), worker.task.get('job.id')),
                                                args=(worker,)
                                                )
                        if not self.worker_processes.get(worker.task.get('job.id')):
                            self._worker_processes[worker.task.get('job.id')] = [p]
                        else:
                            self._worker_processes[worker.task.get('job.id')].append(p)
                        p.start()

                sleep(self.sleep_interval)

            if shutdown:
                break

        for j in self.running_jobs:
            if not self.worker_processes: break
            for p in self.worker_processes.get(j):
                if p.is_alive():
                    p.join()

        # call server to mark this executor terminated
        if self.killer.kill_now:
            for j in self.scheduler.running_jobs():
                print('Cancelling job: %s' % j.get('id'))
                self.scheduler.cancel_job(job_id=j.get('id'))

        # report summary about completed jobs and running jobs if any
        click.echo('Executed %s %s.' % (self.ran_jobs, 'job' if self.ran_jobs <= 1 else 'jobs'))

    def _get_run_status(self):
        running_workers = 0
        running_jobs = 0
        for j in self.scheduler.running_jobs():
            click.echo('Running job: %s' % j.get('id'))
            running_jobs += 1
            for p in self.worker_processes.get(j.get('id')):
                if p.is_alive():
                    click.echo('Running task: %s' % p.name)
                    running_workers += 1
                    p.join(timeout=0.1)
        return running_jobs, running_workers

    def _init_jt_home(self):
        # initial it if needed
        node_info_file = os.path.join(self.node_dir,'info.yaml')

        if os.path.isfile(node_info_file):
            with open(node_info_file, 'r') as f:
                node_info = yaml.load(f)
        else:  # not exist
            node_info = {'id': str(uuid4())}  # may need to add other information
            try:
                os.makedirs(self.node_dir)
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
            with open(node_info_file, "w") as f:
                f.write(yaml.dump(node_info, default_flow_style=False))

        self._node_id = node_info.get('id')

    def _init_workflow_dir(self):
        try:
            os.makedirs(self.workflow_dir)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

        # detect whether workflow has already been installed
        workflow_installation_flag_file = os.path.join(self.workflow_dir, 'workflow.installed')
        if os.path.isfile(workflow_installation_flag_file):
            return

        click.echo('Installing workflow package ...')
        workflow = self.scheduler.get_workflow()

        git_account = workflow.get('git_account')
        git_repo = workflow.get('git_repo')
        git_tag = workflow.get('ver:%s' % self.scheduler.workflow_version).get('git_tag')
        git_path = workflow.get('ver:%s' % self.scheduler.workflow_version).get('git_path')

        # https://github.com/jthub/jtracker-example-workflows/archive/0.2.0.tar.gz
        git_download_url = "https://github.com/%s/%s/archive/%s.zip" % (git_account, git_repo, git_tag)

        tmp_dir = tempfile.mkdtemp()
        request = requests.get(git_download_url)
        zfile = zipfile.ZipFile(BytesIO(request.content))
        zfile.extractall(tmp_dir)

        source_workflow_path = os.path.join(tmp_dir, '%s-%s' % (git_repo, git_tag), git_path, 'workflow')
        source_tool_path = os.path.join(source_workflow_path, 'tools')
        if os.path.isdir(source_tool_path):
            subprocess.check_output(["chmod", "-R", "755", source_tool_path])

        # rm first in case exist
        shutil.rmtree(os.path.join(self.workflow_dir, 'workflow'), ignore_errors=True)

        shutil.move(source_workflow_path, self.workflow_dir)

        # now create the installation flag file
        open(workflow_installation_flag_file, 'a').close()
        click.echo('Workflow package installed')

    def _init_queue_dir(self):
        try:
            os.makedirs(self.queue_dir)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    def _init_executor_dir(self):
        try:
            os.makedirs(self.executor_dir)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
