import os
import re
import uuid
import shutil
import tempfile
import subprocess
from .utils import JOB_STATE, TASK_STATE
from .task import Task
from .job import Job


class GiTracker(object):
    def __init__(self, git_repo_url=None, workflow_name=None, workflow_version=None):
        self._git_repo_url = git_repo_url
        folder_name = re.sub(r'\.git$', '', os.path.basename(self.git_repo_url))
        self._local_git_path = os.path.join(tempfile.mkdtemp(), folder_name)  # TODO: we can allow user to choose where to clone git

        output = subprocess.check_output(["git", "clone", self.git_repo_url, self.local_git_path])

        self._gitracker_home = os.path.join(self.local_git_path,
                                            '.'.join([workflow_name, workflow_version, 'jtracker']))


    @property
    def gitracker_home(self):
        return self._gitracker_home


    @property
    def git_repo_url(self):
        return self._git_repo_url


    @property
    def local_git_path(self):
        return self._local_git_path


    def job_completed(self, job_id=None):
        # will first check whether all tasks for this job are all completed
        # if yes, move the job to completed state
        pass


    def next_job(self, worker=None):
        # always git pull first to synchronize with the remote
        # we may need to do Git hard reset when a job/task update already done by another worker
        self._git_cmd(["pull"])
        # check queued jobs
        queued_job_path = os.path.join(self.gitracker_home, JOB_STATE.QUEUED)
        job_files = os.listdir(queued_job_path)
        if job_files:
            job_file = job_files[0]
            job_id = re.sub(r'\.json$', '', job_file)
            job_file_path = os.path.join(queued_job_path, job_file)
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
                    pass

            self._git_cmd(['add', new_job_path])
            self._git_cmd(['commit', '-m', 'Started new job %s' % job_id])
            self._git_cmd(['push'])
            return True  # successfully started a new job


    def next_task(self, worker=None, jtracker=None, timeout=None):
        # always git pull first to synchronize with the remote
        self._git_cmd(["pull"])
        # check queued task in running jobs
        running_job_path = os.path.join(self.gitracker_home, JOB_STATE.RUNNING)

        for root, dirs, files in os.walk(running_job_path):
            if root.endswith(TASK_STATE.QUEUED):
                for task_name in dirs:
                    # TODO: check whether this t is ready to run by looking into its depends_on task(s)
                    #       for now, choose this first t
                    job_id = root.split('/')[-2]
                    running_worker_path = os.path.join(root, '..', TASK_STATE.RUNNING, worker.worker_id)
                    if not os.path.isdir(running_worker_path): os.makedirs(running_worker_path)

                    if self._move_task(os.path.join(root, task_name), running_worker_path):
                        # succeeded
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

