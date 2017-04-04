import uuid

class Worker(object):
    def __init__(self, scheduler=None, host_ip=None, host_name=None, cpu_cores=None, memory=None):
        self._scheduler = scheduler
        self._host_ip = host_ip
        self._host_name = host_name
        self._cpu_cores = cpu_cores
        self._memory = memory
        self._worker_id = uuid.uuid4()
        self._current_job = None


    @property
    def scheduler(self):
        return self._scheduler

    def current_job(self):
        return self._current_job

    def next_job(self):
        if self._current_job: return  # if current job exists, return None

        next_job = self.scheduler.next_job(timeout=None)
        if next_job:
            self._current_job = next_job
            return self._current_job
        else:
            return

    def log_job_info(self, status):
        self.current_job.log_job_info(self.current_job, info={})  # info must be dict

    def job_json(self):
        return self.current_job.job_json(self.current_job.job_file)

    def job_failed(self):
        self.current_job.job_failed(self.current_job.job_file)
        self._current_job = None

    def job_completed(self):
        self.current_job.job_completed(self.current_job.job_file)
        self._current_job = None

