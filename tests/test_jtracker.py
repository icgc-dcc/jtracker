from jtracker import JTracker
from jtracker import Worker


jt = JTracker(
                    #git_repo_url = 'git@github.com:junjun-zhang/jtracker_example_workflow.git',
                    git_repo_url = 'file://Users/junjun/projects/jtracker_example_workflow',  # for local testing
                    workflow_name = 'ega-file-transfer',
                    workflow_version = '0.1'
                )

worker = Worker(jtracker=jt, host_ip='192.168.1.1')


task1 = worker.next_task()

print task1.task_dict

task_dict1 = worker.current_task

print task1.task_dict

worker.task_completed()

print "task completed"
print task1.task_dict

#worker.log_task_info({})

#worker.task_failed()

worker.next_task()

worker.task_completed()

assert False  # just to see the debugging output