import click
import signal
import multiprocessing
from time import sleep
from timeit import default_timer as timer
from uuid import uuid4
from ..core import Workflow  # needed for running local job
from ..core import Job  # needed for running local job
from .worker import Worker


class GracefulKiller:
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        click.echo('Interruption signal received, will exit when the current running job finishes.')
        self.kill_now = True


class Executor(object):
    def __init__(self, jt_home=None, ams_server=None, wrs_server=None, jess_server=None,
                 queue=None,
                 workflow_name=None,
                 # queue and workflow_name must be mutually exclusive, workflow_name must be 'dev' version
                 workflow_file=None,
                 job_file=None,  # when job_file is provided, it's local mode, no tracking from the server side
                 job_id=None,  # can optionally specify which job to run, not applicable when job_file specified
                 parallel_jobs=1, parallel_workers=1, sleep_interval=10):

        self._killer = GracefulKiller()
        self._id = str(uuid4())
        self._jt_home = jt_home
        self._ams_server = ams_server
        self._wrs_server = wrs_server
        self._jess_server = jess_server
        self._queue = queue
        self._job_id = job_id
        self._job_file = job_file
        self._workflow_file = workflow_file
        self._workflow_name = workflow_name
        self._parallel_jobs = parallel_jobs
        self._parallel_workers = parallel_workers
        self._sleep_interval = sleep_interval

        self._running_jobs = []
        self._processes = {}

        # if job_file specified, ignore job queue and job id
        if self.job_file:  # local mode
            if self.queue:
                click.echo('Local job file supplied, specified job queue ignored.')
                self._queue = None
            if self.job_id:
                click.echo('Local job file supplied, specified job id ignored.')
                self._job_id = None

            if self.workflow_file:
                if workflow_name:
                    click.echo("Local workflow file supplied, specified workflow name ignored '%s'." % workflow_name)
                    self._workflow_name = None
            else:  # if no workflow_file, then must provide workflow_name
                if not self.workflow_name:
                    raise Exception("Must specify full workflow name.")

                workflow_file = self.get_workflow_file_by_name(self.workflow_name)
                if workflow_file:
                    self._workflow_file = workflow_file
                else:
                    raise Exception("Specified workflow not registered.")

                    # now we have local job_file and workflow_file, ready to perform local execution

        else:  # server mode
            if not (self.ams_server and self.wrs_server and self.jess_server):
                raise Exception("Must specify all three servers: AMS, WRS and JESS.")

            if self.workflow_file:
                click.echo("Execute jobs registered on the server, ignore supplied local workflow file.")
                self._workflow_file = None

            if not (self.queue or self.workflow_name):
                raise Exception("Must specify queue ID or workflow name.")
            elif self.queue and self.workflow_name:
                workflow_name = self.get_workflow_name_by_queue(queue_id=self.queue)
                if not workflow_name:
                    raise Exception("Specified queue does not exist.")
                if workflow_name != self.workflow_name:
                    raise Exception(
                        "Specified workflow name was not registered for the specified job queue, no job to run.")
            elif self.workflow_name:
                queues = self.get_queue_by_workflow_name(workflow_name=self.workflow_name)
                if not queues:
                    raise Exception("Specified workflow has not been assigned to any job queue.")
                if len(queues) > 1:
                    raise Exception(
                        "Specified workflow has been assigned to multiple job queues, please specify a job queue.")
            else:  # now must have queue specified
                workflow_name = self.get_workflow_name_by_queue(queue_id=self.queue)
                if not workflow_name:
                    raise Exception("Specified queue does not exist.")

                    # now we have a specific job queue to execute jobs

    @property
    def killer(self):
        return self._killer

    @property
    def id(self):
        return self._id

    @property
    def jt_home(self):
        return self._jt_home

    @property
    def workflow_name(self):
        return self._workflow_name

    @property
    def workflow_file(self):
        return self._workflow_file

    @property
    def job_file(self):
        return self._job_file

    @property
    def job_id(self):
        return self._job_id

    @property
    def sleep_interval(self):
        return self._sleep_interval

    @property
    def ams_server(self):
        return self._ams_server

    @property
    def wrs_server(self):
        return self._wrs_server

    @property
    def jess_server(self):
        return self._jess_server

    @property
    def queue(self):
        return self._queue

    @property
    def parallel_jobs(self):
        return self._parallel_jobs

    @property
    def parallel_workers(self):
        return self._parallel_workers

    @property
    def running_jobs(self):
        return self._running_jobs

    @property
    def processes(self):
        return self._processes

    def get_workflow_name_by_queue(self, queue_id=None):
        jess_server = self.jess_server
        # to be implemented
        return jess_server

    def get_workflow_file_by_name(self, workflow_name=None):
        wrs_server = self.wrs_server
        # to be implemented
        return wrs_server

    def get_queue_by_workflow_name(self, workflow_name=None):
        return

    def work(self, worker):
        proc_name = multiprocessing.current_process().name
        worker.run()
        click.echo('Finish task: {} by worker: {}'.format(proc_name, worker.id))

    def next_job(self):
        # get next job with optional 'job_id' parameter
        return {
            "job.id": "afdfa"
        }

    def has_next_task(self):
        return True

    def next_task_ready(self):
        return True

    def next_task(self):
        return {
            "name": "download",
            "job_id": "afdfa"
        }

    def run(self):
        if self.job_file:
            self.run_local()
        else:
            self.run_remote()

    def run_local(self):
        # TODO: local run is more complicated, let's worry about it later
        click.echo('Run local job not implemented yet.')

    def run_remote(self):
        # let's register the executor to the queue first
        # TODO

        while True:
            if self.killer.kill_now:
                click.echo('Received interruption signal, will not pick up new job. Exit when current running job (if '
                           'any) finishes ...')
                break

            if len(self.running_jobs) >= self.parallel_jobs:
                sleep(self.sleep_interval)
                continue

            job = self.next_job()
            if not job:
                click.echo('No job to run, will exit when current running job (if any) finishes.')
                break

            click.echo('Executor: %s starts working on job: %s' % (self.id, job))
            self._running_jobs.append(job.get('job.id'))

            shutdown = False
            while self.has_next_task():
                running_workers = 0
                for j in self.running_jobs:
                    if not self.processes: break
                    for p in self.processes.get(j):
                        if p.is_alive():
                            click.echo('Running process: ', p.name)
                            running_workers += 1
                            p.join(timeout=0.1)

                click.echo('Number of tasks running: {}'.format(running_workers))

                if running_workers < self.parallel_workers and self.next_task_ready():
                    task = self.next_task()
                    worker = Worker(jt_home=self.jt_home, task=task, executor_id=self.id, queue_id=self.queue,
                                    jess_server=self.jess_server)
                    click.echo('Worker: %s start task: %s in job: %s' %
                               (worker.id, task.get('name'), task.get('job_id'))
                               )
                    p = multiprocessing.Process(target=self.work,
                                                name='task:%s job:%s' % (task.get('name'), task.get('job_id')),
                                                args=(worker,)
                                                )

                    if task.get('job_id') not in self.processes:
                        self._processes[task.get('job_id')] = []
                    self._processes[task.get('job_id')].append(p)

                    p.start()

                if self.killer.kill_now:
                    click.echo(
                        'Received interruption signal, will not pick up new task. Exit when current running task(s) '
                        'finishes ...')
                    shutdown = True
                    break

                sleep(self.sleep_interval)

            if shutdown:
                break

        for j in self.running_jobs:
            if not self.processes: break
            for p in self.processes.get(j):
                if p.is_alive():
                    p.join()

                    # TODO: call server to mark executor terminated
                    # report summary about completed jobs and running jobs if any
