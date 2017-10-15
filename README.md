# JTracker

JTracker is a job tracking, scheduling and execution system with client-server architecture for distributed
computational workflows. All jobs are centrally managed by a JTracker server, JTracker executors (the clients)
request jobs/tasks from the server and execute them on compute nodes the executors reside.

## Installation

JTracker client needs to be installed on a workflow task execution host. It may be a VM in a cloud environment, an
HPC node, or may be just your laptop, or all of them at the same time.

```
# clone the source code
git clone https://github.com/icgc-dcc/jtracker.git

cd jtracker

# install packages
pip3 install -r requirements.txt

# install JT client
python3 setup.py install

```

## Config JT client
You need to configure JT client so it connects to an appropriate JT server, this can be done by editing `.jt/config`.

You may use the default setting to test out JT client.

## Run it
```
# get usage information
jt --help

# start an executor pulls jobs from job queue: 455000f1-8706-4a15-8005-355e84af6368
jt executor -q 455000f1-8706-4a15-8005-355e84af6368 -c

# for more options on executor
jt executor --help

```

## Quick test run with demo JTracker workflow

This test run will use a demo JT server running at http://159.203.53.247.

### Register a user account

```
# please change the user name to your own
curl 'http://159.203.53.247/api/jt-ams/v0.1/accounts' -H 'Content-Type: application/json' -d $'
{
  "name": "your_account_name"
}'
```

### Update JT configuration with the new user account

Edit the configuration file at `.jt/config`, it should include a line like below:
```
jt_account: your_account_name
```

### Register a JT workflow under your account

The workflow we use for this demo is available here:
 https://github.com/jthub/demo-workflows/tree/master/webpage-word-count.

```
curl 'http://localhost:12015/api/jt-wrs/v0.1/workflows/owner/user1' -H 'Content-Type: application/json' --data-binary $'{
  "git_account": "jthub",
  "git_path": "webpage-word-count",
  "git_repo": "demo-workflows",
  "git_server": "https://github.com",
  "git_tag": "webpage-word-count.0.0.7",
  "name": "webpage-word-count",
  "version": "0.0.7",
  "workflow_type": "JTracker"
}'
```

### Register a Job Queue for your new workflow

```

```

### Enqueue a job


### Launch a JT executor

