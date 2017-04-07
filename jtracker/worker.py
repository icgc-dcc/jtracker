import uuid

class Worker(object):
    def __init__(self, jtracker=None, host_ip=None, host_name=None, cpu_cores=None, memory=None):
        self._jtracker = jtracker
        self._host_ip = host_ip
        self._host_name = host_name
        self._cpu_cores = cpu_cores
        self._memory = memory
        self._worker_id = uuid.uuid4()
        self._current_task = None


    @property
    def jtracker(self):
        return self._jtracker

    def current_task(self):
        return self._current_task

    def next_task(self):
        if self._current_task: return  # if current task exists, return None

        next_task = self.jtracker.next_task(self, timeout=None)
        if next_task:
            self._current_task = next_task
            return self._current_task
        else:
            return

    def log_task_info(self, status):
        self.current_task.log_task_info(self.current_task, info={})  # info must be dict

    def task_json(self):
        return self.current_task.task_json(self.current_task.task_file)

    def task_failed(self):
        self.current_task.task_failed(self.current_task.task_file)
        self._current_task = None

    def task_completed(self):
        self.current_task.task_completed(self.current_task.task_file)
        self._current_task = None

