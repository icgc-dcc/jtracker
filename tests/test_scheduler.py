from jtracker import Scheduler
from jtracker import Worker

worker = Worker(host_ip='192.168.1.1')

js = Scheduler(
                    git_repo_url = 'https://github.com/icgc-dcc/jtracker_example_workflow',
                    workflow_name = 'ega-file-transfer',
                    workflow_version = '0.1'
                )

worker.next_job(js)

worker.log_job_info(re.sub(r'[-:.]', '_', datetime.datetime.utcnow().isoformat()))

worker.job_failed()

worker.next_job(js)

worker.job_completed()
