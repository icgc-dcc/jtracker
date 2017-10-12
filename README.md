# JTracker

JTracker is a job tracking, scheduling and execution system with client-server architecture for distributed
computational workflows. All jobs are centrally managed by the JTracker server, the client (aka JTracker worker)
requests jobs/tasks from the server and execute them.

## Installation

JTracker client needs to be installed on a workflow task execution host. It may be a VM in a cloud environment, an
HPC node, or may be just your laptop.

```
# clone the source code
git clone https://github.com/icgc-dcc/jtracker.git

cd jtracker

git checkout 0.8-dev

# install packages
pip3 install -r requirements.txt

# install JT client
python3 setup.py install

```

## Config JT client
You need to configure JT client so it connects to an appropriate JT server, this can be done by editing `.jt/config`.

## Run it
```
jt --help
```
