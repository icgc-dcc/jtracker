# JTracker

JTracker is a Git repository based solution for workflow management and execution.

## Installation

JTracker needs to be installed on a workflow task execution host. It may be a VM in a cloud environment, or may be just your laptop.

```
# install pipsi first
curl https://raw.githubusercontent.com/mitsuhiko/pipsi/master/get-pipsi.py | python

# modify PATH to find pipsi, you may want to add this to the '.bashrc' file
export PATH="~/.local/bin:$PATH"

# clone the source code
git clone https://github.com/icgc-dcc/jtracker.git

# install jtracker from source
cd jtracker
pipsi install .

# or install from PyPI for latest release (no need to clone the source)
pipsi install jtracker

# run it
jt
```

## Design and create your workflow

Follow this example to develop your workflow: https://github.com/icgc-dcc/ega-file-transfer-to-collab-jt

Quick note on workflow development and testing:

1. You can have workflow definition and workflow execution in one single Git repo, although separating them in dedicated Git repos is recommended for production use. Here is an example: https://github.com/jt-hub/jtracker-example-workflows/tree/master/test.0.2.0.jtracker, note that job states folders and worflow folder are in one repo.

2. It is recommended to use a local Git server for development and testing. Follow this instruction to set up a local git server on a mac: http://www.tomdalling.com/blog/software-processes/how-to-set-up-a-secure-git-server-at-home-osx/. Once set up, you can access it same way as you access github. In my case, `git clone ssh://junjun@localhost:/Users/junjun/mygit/jtracker-demo-workflows.git`


## Create a Git repository to manage and track workflow task execution

Here is an example: https://github.com/jt-hub/ega-file-transfer-to-collab-jtracker/tree/master/ega-file-transfer-to-collab.0.4.0.jtracker

At this time, you will need to set up this Git repository on your own manually. In the near future, 'jt' cli tool will be able to set it up automatically for you. 


## Start JTracker Worker on task execution hosts

On a task execution host, you can start a worker as follow assuming workflow definition and job json files exist as specified.

```
jt -g 'git@github.com:jt-hub/ega-file-transfer-to-collab-jtracker.git' -w ega-file-transfer-to-collab -r 0.4.0 worker
```

You can start multiple workers on the same host if there is enough computating resource. You can also start workers in different hosts at the same time. Workflow jobs/tasks will be picked up by individual workers as needed.

