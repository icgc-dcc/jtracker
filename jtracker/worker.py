import os
import time
import uuid


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

        self._current_task = None


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
    def current_task(self):
        return self._current_task


    def next_task(self, timeout=None, retry=None):
        # if current task exists, return None. Current task must be finished
        # either completed or failed (or maybe suspended if we add support later)
        if self.current_task: return

        # it maybe necessary to automatically retry this call on behave of
        # the client according to timeout and retry settings
        next_task = self.jtracker.next_task(self, timeout=None)

        if next_task:
            self._current_task = next_task
            return self.current_task
        else:
            return False


    def log_task_info(self, status):
        self.current_task.log_task_info(self.current_task, info={})  # info must be dict


    def task_dict(self):
        return self.current_task.task_dict(self.current_task.task_file)


    def task_failed(self):
        self.current_task.task_failed(self.current_task.task_file)
        self._current_task = None


    def task_completed(self, timeout=None):
        if not self.current_task: return

        self.current_task.task_completed(timeout=timeout)
        self._current_task = None
        return True


    def _init_workdir(self):
        self._workdir = os.path.join(self.jtracker.jt_home, self.worker_id)

        os.makedirs(self.workdir)

        with open(os.path.join(self.workdir, 'local_git_path.txt'), 'w') as f:
            f.write('%s\n' % self.jtracker.gitracker.local_git_path)

