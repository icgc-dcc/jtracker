from jtracker import Scheduler
from jtracker import Worker


js = Scheduler(
                    git_repo_url = 'git@github.com:junjun-zhang/jtracker_example_workflow.git',
                    workflow_name = 'ega-file-transfer',
                    workflow_version = '0.1'
                )

worker = Worker(scheduler=js, host_ip='192.168.1.1')


job_dict1 = worker.next_job()

job_dict1 = worker.current_job()

#worker.log_job_info({})

#worker.job_failed()

worker.next_job()

#worker.job_completed()
