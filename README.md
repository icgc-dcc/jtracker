## *This repository has been moved to https://github.com/jthub/jt-cli*


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

# install pip3 if not installed already, for Debian or Ubuntu platform do this:
sudo apt-get install python3-pip

# install packages, you may need to run it with sudo
pip3 install -r requirements.txt

# install JT client, you may need to run it with sudo
python3 setup.py install

# if you see usage information with the follow command, you are ready to go
jt --help
```

## Quick test run with demo JTracker workflow

JTracker is in early phase of development, features and behaviours may change as we advance forward. However this quick
test run should give you a clear picture how JTracker is designed and how it may fit in your workflow use cases.

This test run uses a demo JT server running at http://159.203.53.247.

Note: please **do not** upload sensitive data when following along the steps.

### Register a user account

Please change `your_account_name` to your own in the following command.

```
jt user signup -u your_account_name
```

### Log in as the new user

```
# logging in has not been fully implemented, no password needed for now
jt user login -u your_account_name
```

### Register a JT workflow under your account

The workflow we use for this demo is available here:
 https://github.com/jthub/demo-workflows/tree/master/webpage-word-count.

The workflow git release tag is 'webpage-word-count.0.0.8':
 https://github.com/jthub/demo-workflows/releases/tag/webpage-word-count.0.0.8

```
jt wf register --git-server https://github.com \
               --git-account jthub \
               --git-repo demo-workflows \
               --git-path webpage-word-count \
               --git-tag webpage-word-count.0.0.8 \
               --wf-name webpage-word-count \
               --wf-version 0.0.8 \
               --wf-type JTracker
```

### Create a Job Queue for the workflow you would like to run from

The following command creates a job queue for
workflow: `webpage-word-count` with version: `0.0.8`.

```
jt queue add --wf-name webpage-word-count \
             --wf-version 0.0.8
```

Upon successful creation, you will get a UUID for the new job queue, record it for the next step. In
my test, I got `00e2b2e4-f2dc-420a-bb2d-3df6a7984cc3`.

It's possible to create a job queue based off a workflow registered under another user
given that the workflow is accessible to you. In this case, you provide workflow fullname,
eg, `user1.webpage-word-count` for `webpage-word-count` workflow owned by `user1`.

### Enqueue some jobs

Now you are ready to add some jobs to the new queue.

```
# remember to replace '00e2b2e4-f2dc-420a-bb2d-3df6a7984cc3' with your own queue ID
jt job add -q 00e2b2e4-f2dc-420a-bb2d-3df6a7984cc3 -j '{
  "webpage_url": "https://dzone.com/cloud-computing-tutorials-tools-news",
  "words": [ "Cloud", "Docker", "Kubernetes", "OpenStack" ]
}'
```

You can enqueue a couple of more jobs, simply replace `webpage_url` and `words` with your favorite values and
repeat the above command. New jobs can be added to the queue at any time.

### Launch JT executor

Finally, let's launch a JT executor to run those jobs.

```
# again, replace '00e2b2e4-f2dc-420a-bb2d-3df6a7984cc3' with your own queue ID
jt exec run -q 00e2b2e4-f2dc-420a-bb2d-3df6a7984cc3
```

This will launch an executor that will pull and run jobs from queue `00e2b2e4-f2dc-420a-bb2d-3df6a7984cc3`. Current
running jobs/tasks will be displayed in stdout (this can be turned off later).

There are some useful options give you control over how jobs/tasks are to be run. For example,
`-k` and `-p` allow you control how many parallel tasks and jobs the executor can run respectively.
Option `-c` tells executor to run continuously even after it finises all the jobs in the queue. This is useful
when you know there will be more jobs to be queued and you don't want to start the executor again.
Try `jt exec run --help` to get more information.

To increase job processing throughput, you can run many JT executors on multiple compute nodes
(in any environment cloud or HPC) at the same time.

It's possible to implement auto-scaling on your own, for example, using Kubernetes to increase or
decrease worker nodes on which JT executor runs.

### Check job status and output

If the executor is still running, you can perform the following commands in a different terminal.

Get job status in queue `09360ea8-748a-4a8d-9b55-16b5b7278069`.
```
jt job ls -q 09360ea8-748a-4a8d-9b55-16b5b7278069
```

Get detail for a particular job `c36f6ed7-7639-4ffc-984e-f83e00936d4d` in queue `09360ea8-748a-4a8d-9b55-16b5b7278069`.
```
jt job get -j c36f6ed7-7639-4ffc-984e-f83e00936d4d -q 09360ea8-748a-4a8d-9b55-16b5b7278069
```

In the response JSON you will be able to find the word count result.
