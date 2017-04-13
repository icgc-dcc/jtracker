import os
import re
import glob
import uuid
import shutil
import tempfile
import subprocess
import json
from .utils import JOB_STATE, TASK_STATE
from .task import Task
from .job import Job
from .workflow import Workflow


class GiTracker(object):
    def __init__(self, git_repo_url=None, workflow_name=None, workflow_version=None):
        self._git_repo_url = git_repo_url
        folder_name = re.sub(r'\.git$', '', os.path.basename(self.git_repo_url))
        self._local_git_path = os.path.join(tempfile.mkdtemp(), folder_name)  # TODO: we can allow user to choose where to clone git

        output = subprocess.check_output(["git", "clone", self.git_repo_url, self.local_git_path])

        self._gitracker_home = os.path.join(self.local_git_path,
                                            '.'.join([workflow_name, workflow_version, 'jtracker']))

        yaml_file_name = '.'.join([workflow_name, workflow_version, 'workflow.yaml'])
        self._workflow = Workflow(os.path.join(self.local_git_path, yaml_file_name))


    @property
    def gitracker_home(self):
        return self._gitracker_home


    @property
    def git_repo_url(self):
        return self._git_repo_url


    @property
    def local_git_path(self):
        return self._local_git_path


    @property
    def workflow(self):
        return self._workflow


    def job_completed(self, job_id=None):
        # will first check whether all tasks for this job are all completed
        # if yes, move the job to completed state
        pass


    def next_job(self, worker=None):
        # always git pull first to synchronize with the remote
        # we may need to do Git hard reset when a job/task update already done by another worker
        self._git_cmd(["checkout", "master"])
        self._git_cmd(["reset", "--hard", "origin/master"])
        self._git_cmd(["pull"])
        # check queued jobs
        queued_job_path = os.path.join(self.gitracker_home, JOB_STATE.QUEUED)
        job_files = glob.glob(os.path.join(queued_job_path, 'job.*.json'))

        if job_files:
            job_file = os.path.basename(job_files[0])
            job_id = re.sub(r'\.json$', '', job_file)
            job_file_path = os.path.join(queued_job_path, job_file)

            with open(job_file_path, 'r') as f:
                job_dict = json.loads(f.read())

            new_job_path = os.path.join(self.gitracker_home, JOB_STATE.RUNNING, job_id)
            if not os.path.isdir(new_job_path): os.makedirs(new_job_path)

            self._git_cmd(['mv', job_file_path, new_job_path])
            for t_state in (TASK_STATE.QUEUED, TASK_STATE.RUNNING, TASK_STATE.COMPLETED, TASK_STATE.FAILED):
                t_path = os.path.join(new_job_path, t_state)
                if not os.path.isdir(t_path): os.makedirs(t_path)
                with open(os.path.join(t_path, '.gitignore'), 'w') as f:
                    f.write('')

                # TODO: create task folders under new_job_path/TASK_STATE.QUEUED
                if t_state == TASK_STATE.QUEUED:
                    for stp in self.workflow.workflow_steps:
                        task_folder = os.path.join(t_path, 'task.%s' % stp)
                        os.makedirs(task_folder)  # create the task folder

                        # new create task JSON file under task_folder
                        task_dict = self._job_dict2task_dict(task_name=stp, job_dict=job_dict)
                        with open(os.path.join(task_folder, 'task.%s.json' % stp), 'w') as f:
                            f.write(json.dumps(task_dict))

            self._git_cmd(['add', self.gitracker_home])  # stage the change
            self._git_cmd(['commit', '-m', 'Started new job %s' % job_id])
            self._git_cmd(['push'])
            return True  # successfully started a new job


    def next_task(self, worker=None, jtracker=None, timeout=None):
        # always git pull first to synchronize with the remote
        self._git_cmd(["checkout", "master"])
        self._git_cmd(["reset", "--hard", "origin/master"])
        self._git_cmd(["pull"])
        # check queued task in running jobs
        running_job_path = os.path.join(self.gitracker_home, JOB_STATE.RUNNING)

        for root, dirs, files in os.walk(running_job_path):
            if root.endswith(TASK_STATE.QUEUED):
                for task_name in dirs:
                    # TODO: check whether this t is ready to run by looking into its depends_on task(s)
                    #       for now, choose this first t

                    job_id = root.split('/')[-2]

                    if not self._check_task_dependency(worker=worker, task_name=task_name, job_id=job_id): continue

                    running_worker_path = os.path.join(root.replace(TASK_STATE.QUEUED, TASK_STATE.RUNNING), worker.worker_id)
                    if not os.path.isdir(running_worker_path): os.makedirs(running_worker_path)

                    if self._move_task(os.path.join(root, task_name), running_worker_path):
                        # succeeded
                        # TODO: consider move object creation to jtracker instead of here, just
                        #       need to return needed information to create Task and Job instances
                        task = Task(name=task_name, job=Job(
                                                                job_id = job_id,
                                                                state = JOB_STATE.RUNNING,
                                                                jtracker = jtracker
                                                            ),
                                        worker=worker, jtracker=jtracker
                                    )
                        return task


    def task_completed(self, task_name, worker_id, job_id, timeout):
        # always git pull first to synchronize with the remote
        self._git_cmd(["checkout", "master"])
        self._git_cmd(["reset", "--hard", "origin/master"])
        self._git_cmd(["pull"])

        # current task and job states must be both RUNNING
        task_state = TASK_STATE.RUNNING
        job_state = JOB_STATE.RUNNING

        source_path = os.path.join(
                            self.gitracker_home,
                            job_state,
                            job_id,
                            task_state,
                            worker_id,
                            task_name
                        )

        # perform a check to ensure task_name matches what's inside the worker folder
        if not os.path.isdir(source_path):
            return False # we will need better error handling later

        target_path = os.path.join(
                            self.gitracker_home,
                            job_state,
                            job_id,
                            TASK_STATE.COMPLETED,
                            worker_id
                        )
        if not os.path.isdir(target_path): os.makedirs(target_path)

        return self._move_task(source_path=source_path, target_path=target_path, timeout=timeout)


    def task_failed(self, task_name, worker_id, job_id, timeout):
        pass


    def _move_task(self, source_path=None, target_path=None, timeout=None):
        self._git_cmd(["mv", source_path, target_path])
        self._git_cmd(["commit", "-m", "JTracker moved %s to %s" % (
                                source_path.replace('%s/' % self.gitracker_home, ''),
                                target_path.replace('%s/' % self.gitracker_home, ''))]
                    )
        self._git_cmd(["push"])
        return True


    def _git_cmd(self, cmds=[]):
        origWD = os.getcwd()
        os.chdir(self.gitracker_home)

        subprocess.check_output(["git"] + cmds)

        os.chdir(origWD)


    def _job_dict2task_dict(self, task_name=None, job_dict={}):
        # TODO: to be implemented
        return {}


    def _check_task_dependency(self, worker=None, task_name=None, job_id=None):
        # constraint can be 'same_host', 'same_worker' or None
        worker_id = worker.worker_id
        host_id = worker.host_id
        task_name = re.sub(r'^task\.', '', task_name)

        depends_on = self.workflow.workflow_steps.get(task_name).get('depends_on')
        if not depends_on: return True

        constraint = self.workflow.workflow_steps.get(task_name).get('constraint')

        worker_id_pattern = 'worker.*'
        if constraint == 'same_worker':
            worker_id_pattern = worker_id
        elif constraint == 'same_host':
            worker_id_pattern = 'worker.*.host.%s.*' % host_id

        for d in depends_on:
            parent_task, parent_state = d.split('.')
            path_to_parent_task_file = os.path.join(
                                                    self.gitracker_home,
                                                    JOB_STATE.RUNNING,
                                                    job_id,
                                                    'task_state.%s' % parent_state,
                                                    worker_id_pattern,
                                                    'task.%s' % parent_task,
                                                    'task.%s.json' % parent_task
                                                )

            if not glob.glob(path_to_parent_task_file):
                # TODO: to be safe, it's neccessary to tag the parent task file, maybe by registering
                #       the worker and next step in the parent task file, this will make this file update
                #       part of the transaction, if the commit and push step (final step) succeed, it's safe
                #       to proceed because the parent task file is still in previous checked state. Without
                #       this tagging, a locally checked parent task file might get updated / moved by other
                #       worker, so it's not safe to ensure the dependent task stayed the same state as when
                #       it's checked
                return False

        return True
