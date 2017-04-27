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

from retrying import retry
from .utils import retry_if_result_none


# we will allow this to be set by the client in a config file of some sort
wait_random_min = 1000    #   1 sec
wait_random_max = 10000   #  10 sec
stop_max_delay = None  #60000   #  60 sec, None will never stop


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


    @retry(retry_on_result=retry_if_result_none, \
                wait_random_min=wait_random_min, \
                wait_random_max=wait_random_max, \
                stop_max_delay=None)  # make sure the call success
    def update_job_state(self, job_id=None):
        try:
            self._sync_local_git_with_server()
            job_path = os.path.join(self.gitracker_home, JOB_STATE.RUNNING, job_id)  # the job must be in RUNNING state

            queued_task_files = glob.glob(os.path.join(job_path, TASK_STATE.QUEUED, 'task.*', 'task.*.json'))

            task_states = set()
            task_files = glob.glob(os.path.join(job_path, '*', 'worker.*', 'task.*', 'task.*.json'))  # this includes all non-queued tasks
            for t in task_files:
                task_states.add(t.split(os.path.sep)[-4])   # this is task_state

            # check whether all tasks for this job are all completed
            # if yes, move the job to completed state
            # we can add more check here, for example, making sure no task json is missing
            if not queued_task_files and task_states == set([TASK_STATE.COMPLETED]):
                self._git_cmd(['mv', job_path, os.path.join(self.gitracker_home, JOB_STATE.COMPLETED)])
                # TODO: we can collect some runtime statistics etc here to add to job JSON file

                self._git_cmd(['add', self.gitracker_home])  # stage the change
                self._git_cmd(['commit', '-m', 'Job completed: %s' % job_id])
                self._git_cmd(['push', '-q'])

            # check whether no task is running, at least one of them is in failed state
            # if yes, move the job to failed state
            # it is VERY IMPORTANT to not move the job if there is still running task, otherwise those task will fail miserably or even run into infinite loop
            elif not TASK_STATE.RUNNING in task_states and TASK_STATE.FAILED in task_states:
                self._git_cmd(['mv', job_path, os.path.join(self.gitracker_home, JOB_STATE.FAILED)])
                # TODO: we can collect some runtime statistics etc here to add to job JSON file

                self._git_cmd(['add', self.gitracker_home])  # stage the change
                self._git_cmd(['commit', '-m', 'Job failed: %s' % job_id])
                self._git_cmd(['push', '-q'])

            return True
        except Exception, e:  # except raised likely due to other worker picked made task state change, in such case, we will retry but not try forever
            # print e  # debug
            return  # when that happen this function will be re-tried


    def next_job(self, worker=None):
        # always git pull first to synchronize with the remote
        # we may need to do Git hard reset when a job/task update already done by another worker
        self._sync_local_git_with_server()

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
                    for call_name in self.workflow.workflow_calls:  # workflow calls defined to call tasks

                        call_input = self.workflow.workflow_calls[call_name].get('input')

                        called_task = self.workflow.workflow_calls[call_name].get('task')
                        depends_on = self.workflow.workflow_calls[call_name].get('depends_on')
                        task_dict = {
                            'call': call_name,
                            'input': {},
                            'task': called_task,
                            'depends_on': depends_on,
                            'command': self.workflow.workflow_dict.get('tasks').get(called_task).get('command'),
                            'runtime': self.workflow.workflow_dict.get('tasks').get(called_task).get('runtime')
                        }

                        # need to find out whether this is a task with scatter call
                        scatter_setting = self.workflow.workflow_calls[call_name].get('scatter')
                        if scatter_setting:
                            # TODO: if bad things happen here due to error in workflow definition or input Job JSON
                            #       we will need to be able to move the relevant Job JSON to failed folder
                            scatter_call_name = scatter_setting.get('name')
                            input_variable = scatter_setting.get('input').keys()[0]  # must always have one key (at least for now), will do error checking later
                            task_suffix_field = scatter_setting.get('input').get(input_variable).get('task_suffix')
                            if not task_suffix_field: task_suffix_field = input_variable

                            with_items_field = scatter_setting.get('input').get(input_variable).get('with_items')

                            # the with_items_field must be in the original job JSON, at least for now
                            if not job_dict.get(with_items_field):
                                # TODO: move the job to failed folder with clear error message
                                pass
                            else:
                                task_suffix_set = set([])
                                for item in job_dict.get(with_items_field):
                                    task_suffix = None
                                    if isinstance(item, dict):
                                        suffix_fields = task_suffix_field.split('.')
                                        if len(suffix_fields) != 2 or suffix_fields[0] != input_variable:
                                            # TODO: error in scatter call definition, move the job to failed
                                            # folder with clear error message
                                            pass
                                        else:
                                            task_suffix = item.get(suffix_fields[1])

                                    elif isinstance(item, str) or isinstance(item, int):
                                        task_suffix = str(item)

                                    else:
                                        # TODO: move the job to failed folder with clear error message
                                        pass

                                    task_suffix = re.sub('[^0-9a-zA-Z]+', '_', task_suffix)  # need to avoid special character
                                    task_suffix_set.add(task_suffix)

                                    task_folder = os.path.join(t_path, 'task.%s.%s' % (call_name, task_suffix))  # one of the scattered tasks
                                    os.makedirs(task_folder)  # create the task folder

                                    original_depends_on = task_dict['depends_on']
                                    if original_depends_on:
                                        updated_depends_on = []
                                        for parent_call in original_depends_on:
                                            parts = parent_call.split('@')
                                            parts[1] = '%s.%s' % (parts[1], task_suffix)
                                            updated_depends_on.append('@'.join(parts))

                                        task_dict['depends_on'] = updated_depends_on

                                    for i in call_input:
                                        if '@' in call_input[i]:
                                            value = '{{%s.%s}}' % (call_input[i], task_suffix)
                                        elif len(call_input[i].split('.')) == 2:
                                            if call_input[i].split('.')[0] == input_variable:
                                                value = item.get(call_input[i].split('.')[1])
                                            else:
                                                value = job_dict.get(call_input[i].split('.')[0]).get(call_input[i].split('.')[1])
                                        else:
                                            value = job_dict.get(call_input[i])

                                        task_dict['input'][i] = value

                                    with open(os.path.join(task_folder, 'task.%s.%s.json' % (call_name, task_suffix)), 'w') as f:
                                        f.write(json.dumps(task_dict, indent=2))

                                    # reset for the next iteration
                                    task_dict['input'] = {}
                                    task_dict['depends_on'] = depends_on

                                if len(task_suffix_set) < len(job_dict.get(with_items_field)):
                                    # duplicated task suffix detected
                                    # TODO: move the job to failed folder with clear error message
                                    pass

                        else:
                            task_folder = os.path.join(t_path, 'task.%s' % call_name)
                            os.makedirs(task_folder)  # create the task folder

                            for i in call_input:
                                if '@' in call_input[i]:
                                    value = '{{%s}}' % call_input[i]
                                else:
                                    value = job_dict.get(call_input[i])

                                task_dict['input'][i] = value

                            with open(os.path.join(task_folder, 'task.%s.json' % call_name), 'w') as f:
                                f.write(json.dumps(task_dict, indent=2))

            self._git_cmd(['add', self.gitracker_home])  # stage the change
            self._git_cmd(['commit', '-m', 'Started new job %s' % job_id])
            self._git_cmd(['push', '-q'])
            return True  # successfully started a new job


    def next_task(self, worker=None, jtracker=None, timeout=None):
        # always git pull first to synchronize with the remote
        self._sync_local_git_with_server()

        # check queued task in running jobs
        running_job_path = os.path.join(self.gitracker_home, JOB_STATE.RUNNING)

        for root, dirs, files in os.walk(running_job_path):
            if root.endswith(TASK_STATE.QUEUED):
                for task_name in dirs:
                    job_id = root.split(os.path.sep)[-2]

                    task_json = os.path.join(root, task_name, '%s.json' % task_name)
                    with open(task_json, 'r') as f:
                        task_dict = json.loads(f.read())

                    if not self._check_task_dependency(worker=worker, task_dict=task_dict, job_id=job_id): continue

                    running_worker_path = os.path.join(root.replace(TASK_STATE.QUEUED, TASK_STATE.RUNNING), worker.worker_id)
                    if not os.path.isdir(running_worker_path): os.makedirs(running_worker_path)

                    if self._move_task(os.path.join(root, task_name), running_worker_path):
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
        self._sync_local_git_with_server()

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
            f.write(json.dumps(task_dict, indent=2))

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


    def _check_task_dependency(self, worker=None, task_dict=None, job_id=None):
        worker_id = worker.worker_id
        host_id = worker.host_id

        depends_on = task_dict.get('depends_on')
        if not depends_on: return True

        # constraint can be 'same_host', 'same_worker', 'shared_fs' or None
        constraint = self.workflow.workflow_dict.get('workflow',{}).get('execution',{}).get('constraint')

        worker_id_pattern = 'worker.*'
        """
        ## to be implemented properly, for now every worker can run a task
        if constraint and constraint == 'same_worker':
            worker_id_pattern = worker_id
        elif constraint and constraint == 'same_host':  # more work is needed to handle tasks do not have dependencies, they will run without checking host
            worker_id_pattern = 'worker.*.host.%s.*' % host_id
        """

        # we will need better implementation, consider covering 'failed' parent task as well
        # right now it only check whether parent task state is COMPLETED
        for d in depends_on:
            parent_state, parent_task, execution_constraint = (d.split('@') + [None] * 3)[:3]
            path_to_parent_task_file1 = os.path.join(
                                                    self.gitracker_home,
                                                    JOB_STATE.RUNNING,
                                                    job_id,
                                                    'task_state.*',
                                                    worker_id_pattern,
                                                    'task.%s' % parent_task,
                                                    'task.%s.json' % parent_task
                                                )

            path_to_parent_task_file2 = os.path.join(
                                                    self.gitracker_home,
                                                    JOB_STATE.RUNNING,
                                                    job_id,
                                                    'task_state.*',
                                                    worker_id_pattern,
                                                    'task.%s.*' % parent_task,
                                                    'task.%s.*.json' % parent_task
                                                )

            parent_task_files = glob.glob(path_to_parent_task_file1) + glob.glob(path_to_parent_task_file2)
            parent_task_states = set()
            for pt in parent_task_files:
                parent_task_states.add(str(pt).split(os.path.sep)[-4])

            if not parent_task_states or parent_task_states - set([TASK_STATE.COMPLETED]):  # there are other state than 'completed'
                return False

        return True


    def _sync_local_git_with_server(self):
        self._git_cmd(["checkout", "-q", "master"])
        self._git_cmd(["reset", "--hard", "origin/master"])
        self._git_cmd(["clean", "-qfdx"])
        self._git_cmd(["pull", "-q"])

