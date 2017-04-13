import os
import time
import uuid
from retrying import retry
from .utils import retry_if_result_none

wait_random_min=2000   #  2 sec
wait_random_max=10000  # 10 sec
stop_max_delay=60000   # 60 sec


class Worker(object):
    def __init__(self, jtracker=None, host_ip=None, host_name=None, cpu_cores=None, memory=None):
        self._jtracker = jtracker
        self._host_ip = jtracker.host_ip
        self._host_name = host_name
        self._cpu_cores = cpu_cores
        self._memory = memory
        self._host_id = self.jtracker.host_id
        self._worker_id = 'worker.%s.%s.host.%s.%s' % (
                                                str(int(time.time())),
                                                str(uuid.uuid4())[:8],
                                                self.host_id,
                                                self.host_ip
                                            )

        self._init_workdir()

        self._task = None


    @property
    def jtracker(self):
        return self._jtracker


    @property
    def host_ip(self):
        return self._host_ip


    @property
    def host_id(self):
        return self._host_id


    @property
    def worker_id(self):
        return self._worker_id


    @property
    def workdir(self):
        return self._workdir


    @property
    def task(self):
        return self._task


    @retry(retry_on_result=retry_if_result_none, \
                wait_random_min=wait_random_min, \
                wait_random_max=wait_random_max, \
                stop_max_delay=stop_max_delay)
    def next_task(self, timeout=None, retry=None):
        # if current task exists, return None. Current task must be finished
        # either completed or failed (or maybe suspended if we add support later)
        if self.task: return False

        # it maybe necessary to automatically retry this call on behave of
        # the client according to timeout and retry settings
        next_task = self.jtracker.next_task(self, timeout=None)

        if next_task:
            self._task = next_task
            return self.task
        else:
            return None


    @retry(retry_on_result=retry_if_result_none, \
                wait_random_min=wait_random_min, \
                wait_random_max=wait_random_max, \
                stop_max_delay=stop_max_delay)
    def log_task_info(self, status):
        return self.task.log_task_info(self.task, info={})  # info must be dict


    def task_dict(self):
        return self.task.task_dict(self.task.task_file)


    @retry(retry_on_result=retry_if_result_none, \
                wait_random_min=wait_random_min, \
                wait_random_max=wait_random_max, \
                stop_max_delay=stop_max_delay)
    def task_completed(self, timeout=None):
        if not self.task: return False

        if self.task.task_completed(timeout=timeout):
            self._task = None
            return True
        else:
            return


    @retry(retry_on_result=retry_if_result_none, \
                wait_random_min=wait_random_min, \
                wait_random_max=wait_random_max, \
                stop_max_delay=stop_max_delay)
    def task_failed(self):
        if not self.task: return False

        if self.task.task_failed(timeout=timeout):
            self._task = None
            return True
        else:
            return


    def _init_workdir(self):
        self._workdir = os.path.join(self.jtracker.jt_home, self.worker_id)

        os.makedirs(self.workdir)

        with open(os.path.join(self.workdir, 'local_git_path.txt'), 'w') as f:
            f.write('%s\n' % self.jtracker.gitracker.local_git_path)

