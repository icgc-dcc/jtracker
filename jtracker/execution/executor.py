import click
import signal
import multiprocessing
from time import sleep
import random
import requests
from uuid import uuid4
from ..core import Workflow  # needed for running local job
from ..core import Job  # needed for running local job
from .scheduler import JessScheduler
from .scheduler import LocalScheduler
from .worker import Worker
from jtracker.exceptions import JessNotAvailable


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
                 workflow_file=None,
                 job_file=None,  # when job_file is provided, it's local mode, no tracking from the server side
                 job_id=None,  # can optionally specify which job to run, not applicable when job_file specified
                 parallel_jobs=1, parallel_workers=1, sleep_interval=5, max_jobs=0, continuous_run=False):

        self._scheduler = None
        self._killer = GracefulKiller()
        # allow user specify executor_id for resuming,
        # TODO: has to check to make sure there is no such executor currently running
        #       need some server side work to support this
        self._id = executor_id if executor_id else str(uuid4())
        self._jt_home = jt_home
        self._jt_account = jt_account
        self._ams_server = ams_server
        self._wrs_server = wrs_server
        self._jess_server = jess_server
        self._queue = queue_id
        self._job_id = job_id
        self._job_file = job_file
        self._workflow_file = workflow_file
        self._workflow_name = workflow_name
        self._parallel_jobs = parallel_jobs
        self._max_jobs = max_jobs
        self._parallel_workers = parallel_workers
        self._sleep_interval = sleep_interval
        self._ran_jobs = 0
        self._continuous_run = continuous_run

        self._running_jobs = []
        self._worker_processes = {}

        self._init_jt_home()
        self._validate_and_setup()

        # TODO: check whether workflow package has already downloaded
        # TODO: check whether previous executor session exists, restore it unless uesr chose not to (via options)

    def _init_jt_home(self):
        # TODO: read from config under jt_home, if config not exist create one

        # get node id
        self._node_id = str(uuid4())

    def _validate_and_setup(self):
        # if job_file specified, ignore job queue and job id
        if self.job_file:  # local mode
            if self.queue:
                click.echo('Local job file supplied, specified job queue ignored.')
                self._queue = None
            if self.job_id:
                click.echo('Local job file supplied, specified job id ignored.')
                self._job_id = None

            if self.workflow_file:
                if self.workflow_name:
                    click.echo(
                        "Local workflow file supplied, specified workflow name ignored '%s'." % self.workflow_name)
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
    def scheduler(self):
        return self._scheduler

    @property
    def node_id(self):
        return self._node_id

    @property
    def jt_home(self):
        return self._jt_home

    @property
    def jt_account(self):
        return self._jt_account

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

    def _register_executor(self):
        # JESS endpoint: /executors/owner/{owner_name}/queue/{queue_id}

        # later we should really get executor dict by self.to_dict, dict version of the executor object
        executor = {
            'id': self.id  # only ID field for now
        }

        request_url = "%s/executors/owner/%s/queue/%s" % (self.jess_server.strip('/'), self.jt_account, self.queue)

        try:
            r = requests.post(url=request_url, json=executor)
        except:
            raise JessNotAvailable('JESS service temporarily unavailable')

        if r.status_code != 200:
            raise Exception('Failed to register the executor, please make sure it has not been registered before')

    def run(self):
        if self.job_file:
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
        self._register_executor()

        # get the scheduler
        self._scheduler = JessScheduler(jess_server=self.jess_server,
                                        jt_account=self.jt_account, queue=self.queue, executor_id=self.id)
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

            worker = Worker(jt_home=self.jt_home, scheduler=self.scheduler, node_id=self.node_id)

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
                    worker = Worker(jt_home=self.jt_home, scheduler=self.scheduler, node_id=self.node_id)
                    task = worker.next_task(job_state='running')  # get next task in the current running jobs
                    if not task:  # else try to start task for next job if it's appropriate to do so
                        if not (self.max_jobs and self.ran_jobs >= self.max_jobs) and \
                                        not len(self.scheduler.running_jobs()) >= self.parallel_jobs:
                            task = worker.next_task(job_state='queued')
                            if task:
                                self._ran_jobs += 1
                                click.echo('Executor: %s starts no. %s job' % (self.id, self.ran_jobs))
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

        # TODO: call server to mark this executor terminated
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
