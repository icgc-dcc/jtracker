# test driven development

from jtracker import JTracker
from jtracker import Worker


jt = JTracker(
                    #git_repo_url = 'git@github.com:junjun-zhang/jtracker_example_workflow.git',
                    git_repo_url = 'file://Users/junjun/projects/jtracker_example_workflow',  # for local testing
                    workflow_name = 'ega-file-transfer',
                    workflow_version = '0.1'
                )

worker = Worker(jtracker=jt)

print "worker.host_ip: %s" % worker.host_ip
print "worker.host_id: %s" % worker.host_id
print "worker.worker_id: %s" % worker.worker_id
print "worker.workdir: %s" % worker.workdir

print "jt.jt_home: %s" % jt.jt_home
print "jt.gitracker_home: %s" % jt.gitracker_home

task1 = worker.next_task()

print "task1.name: %s" % task1.name
print "task1.task_dict: %s" % task1.task_dict
print "task1.job.job_id: %s" % task1.job.job_id
print "task1.job.job_dict: %s" % task1.job.job_dict

task_dict1 = worker.current_task

print task1.task_dict

worker.task_completed()

print "task completed"
print "task1.task_dict: %s" % task1.task_dict

#worker.log_task_info({})

#worker.task_failed()

worker.next_task()

worker.task_completed()

assert False  # just to see the debugging output