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

        output = subprocess.check_output(["git", "clone", "--recursive", self.git_repo_url, self.local_git_path])

        self._gitracker_home = os.path.join(self.local_git_path,
                                            '.'.join([workflow_name, workflow_version, 'jtracker']))

        self._workflow_home = os.path.join(self.gitracker_home, 'workflow')

        yaml_file_name = '.'.join([workflow_name, 'jt', 'yaml'])
        self._workflow = Workflow(os.path.join(self.workflow_home, yaml_file_name))


    @property
    def gitracker_home(self):
        return self._gitracker_home


    @property
    def workflow_home(self):
        return self._workflow_home


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


    def job_failed(self, job_id=None):
        # will first check whether all tasks for this job are stopped, at least one of them is in failed state
        # if yes, move the job to failed state
        pass


    def next_job(self, worker=None):
        # always git pull first to synchronize with the remote
        # we may need to do Git hard reset when a job/task update already done by another worker
        self._git_cmd(["checkout", "-q", "master"])
        self._git_cmd(["reset", "--hard", "origin/master"])
        self._git_cmd(["clean", "-qfdx"])
        self._git_cmd(["pull", "-q"])
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
                    for stp in self.workflow.workflow_calls:  # workflow calls defined to call tasks
                        task_folder = os.path.join(t_path, 'task.%s' % stp)
                        os.makedirs(task_folder)  # create the task folder

                        # new create task JSON file under task_folder
                        task_dict = self._job_dict2task_dict(task_name=stp, job_dict=job_dict)
                        with open(os.path.join(task_folder, 'task.%s.json' % stp), 'w') as f:
                            f.write(json.dumps(task_dict))

            self._git_cmd(['add', self.gitracker_home])  # stage the change
            self._git_cmd(['commit', '-m', 'Started new job %s' % job_id])
            self._git_cmd(['push', '-q'])
            return True  # successfully started a new job


    def next_task(self, worker=None, jtracker=None, timeout=None):
        # always git pull first to synchronize with the remote
        self._git_cmd(["checkout", "-q", "master"])
        self._git_cmd(["reset", "--hard", "origin/master"])
        self._git_cmd(["clean", "-qfdx"])
        self._git_cmd(["pull", "-q"])
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


    def task_completed(self, worker=None):
        return self.task_ended(worker=worker, move_to=TASK_STATE.COMPLETED)


    def task_failed(self, worker=None):
        return self.task_ended(worker=worker, move_to=TASK_STATE.FAILED)


    def task_ended(self, worker=None, move_to=None):
        task_name = worker.task.name
        worker_id = worker.worker_id
        job_id = worker.task.job.job_id

        # always git pull first to synchronize with the remote
        self._git_cmd(["checkout", "-q", "master"])
        self._git_cmd(["reset", "--hard", "origin/master"])
        self._git_cmd(["clean", "-qfdx"])
        self._git_cmd(["pull", "-q"])

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

        # now here we may collect outputs from worker to be included in task json file
        output_json_file = os.path.join(worker.cwd, 'output.json')

        if os.path.isfile(output_json_file):
            with open(output_json_file) as output_json:
                output_dict = json.load(output_json)
        else:
            output_dict = {}

        output_dict['worker_id'] = worker.worker_id  # record worker_id
        output_dict['task_state'] = move_to.split('.')[-1]

        task_dict = worker.task.task_dict
        if not task_dict.get('output'):
            task_dict['output'] = []

        task_dict['output'].append(output_dict)

        # write back to task_json file
        with open(os.path.join(source_path, '%s.json' % task_name), 'w') as f:
            f.write(json.dumps(task_dict))

        target_path = os.path.join(
                            self.gitracker_home,
                            job_state,
                            job_id,
                            move_to,
                            worker_id
                        )
        if not os.path.isdir(target_path): os.makedirs(target_path)

        return self._move_task(source_path=source_path, target_path=target_path)


    def _move_task(self, source_path=None, target_path=None):
        self._git_cmd(["mv", source_path, target_path])
        self._git_cmd(["add", target_path])
        self._git_cmd(["commit", "-m", "JTracker moved %s to %s" % (
                                source_path.replace('%s/' % self.gitracker_home, ''),
                                target_path.replace('%s/' % self.gitracker_home, ''))]
                    )
        self._git_cmd(["push", "-q"])
        return True


    def _git_cmd(self, cmds=[]):
        origWD = os.getcwd()
        os.chdir(self.gitracker_home)

        subprocess.check_output(["git"] + cmds)

        os.chdir(origWD)


    def _job_dict2task_dict(self, task_name=None, job_dict={}):
        called_task = self.workflow.workflow_calls[task_name].get('task')
        task_dict = {
            'input': {},
            'command': self.workflow.workflow_dict.get('tasks').get(called_task).get('command'),
            'runtime': self.workflow.workflow_dict.get('tasks').get(called_task).get('runtime')
        }

        call_input = self.workflow.workflow_calls[task_name].get('input')

        for i in call_input:
            if '.' in call_input[i]:
                value = '{%s}' % call_input[i]
            else:
                value = job_dict.get(call_input[i])

            task_dict['input'][i] = value

        return task_dict


    def _check_task_dependency(self, worker=None, task_name=None, job_id=None):
        worker_id = worker.worker_id
        host_id = worker.host_id
        task_name = re.sub(r'^task\.', '', task_name)

        depends_on = self.workflow.workflow_calls.get(task_name).get('depends_on')
        if not depends_on: return True

        # constraint can be 'same_host', 'same_worker', 'shared_fs' or None
        constraint = self.workflow.workflow_dict.get('workflow',{}).get('execution',{}).get('constraint')

        worker_id_pattern = 'worker.*'
        if constraint and constraint == 'same_worker':
            worker_id_pattern = worker_id
        elif constraint and constraint == 'same_host':
            worker_id_pattern = 'worker.*.host.%s.*' % host_id

        for d in depends_on:
            parent_task, parent_state = d.split('@')
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
