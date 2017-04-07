from jtracker import JTracker
from jtracker import Worker


jt = JTracker(
                    git_repo_url = 'git@github.com:junjun-zhang/jtracker_example_workflow.git',
                    workflow_name = 'ega-file-transfer',
                    workflow_version = '0.1'
                )

worker = Worker(jtracker=jt, host_ip='192.168.1.1')


job_dict1 = worker.next_task()

job_dict1 = worker.current_task()

#worker.log_task_info({})

#worker.task_failed()

worker.next_task()

#worker.task_completed()
