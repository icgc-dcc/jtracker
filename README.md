# JTracker

JTracker is a workflow management and execution engine Backed on Git Repository.

## Installation

JTracker needs to be installed on a workflow task execution host. It may be a VM in a cloud environment, or may be just your laptop.

```
# install pipsi first
curl https://raw.githubusercontent.com/mitsuhiko/pipsi/master/get-pipsi.py | python

# clone the source code
git clone git@github.com:icgc-dcc/jtracker.git

# install jtracker
cd jtracker
pipsi install .

# run it
jt
```

## Design and create your workflow

Follow this example to develop your workflow: https://github.com/icgc-dcc/ega-file-transfer-to-collab


## Create a Git repository to manage workflow execution

Here is an example: https://github.com/icgc-dcc/jtracker-example-workflows/tree/master/ega-file-transfer-to-collab.0.3.0.jtracker

At this time, you will need to set up this Git repository on your own manually. In the near future, 'jt' cli tool will be able to set it up automatically for you. 


## Start JTracker Worker on task execution hosts

On a task execution host, you can start a worker as follow assuming workflow definition and job json files exist as specified.

```
jt -g 'git@github.com:icgc-dcc/jtracker-example-workflows' -n ega-file-transfer-to-collab -w 0.3.0 worker
```

You can start multiple workers on the same host if there is enough computating resource. You can also start workers in different hosts at the same time. Workflow jobs/tasks will be picked up by individual workers as needed.

