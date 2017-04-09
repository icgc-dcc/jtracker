# test driven development

import json
from jtracker import JTracker
from jtracker import Worker


jt = JTracker(
                    git_repo_url = 'git@github.com:junjun-zhang/jtracker_example_workflow.git',
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

#print "task1.name: %s" % task1.name
#print "task1.task_dict in JSON:\n%s" % json.dumps(task1.task_dict, sort_keys=True, indent=2)
#print "task1.job.job_id: %s" % task1.job.job_id
#print "task1.job.job_dict in JSON:\n%s" % json.dumps(task1.job.job_dict, sort_keys=True, indent=2)

#print "worker.current_task.name: %s" % worker.current_task.name
#print "worker.current_task.job.job_id: %s" % worker.current_task.job.job_id

worker.task_completed()

#print "task completed"
#print "task1.task_dict: %s" % task1.task_dict

#worker.log_task_info({})

#worker.task_failed()

#worker.next_task()

#worker.task_completed(timeout=None)

assert False  # just to see the debugging output
