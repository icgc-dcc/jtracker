# JTracker

JTracker is a Python library for distributed job scheduling and tracking backed on Git repository.

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
```

## Start worker on a task execution host

Once installed, you can start a worker as follow, assuming workflow definition and job json files exist as specified.
```
jt -g 'git@github.com:icgc-dcc/jtracker-example-workflows' -n ega-file-transfer-to-collab -w 0.3.0 worker
```
