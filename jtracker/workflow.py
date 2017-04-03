import yaml

class Workflow(object):
    def __init__(self, workflow_yaml=None):
        self._workflow_yaml = workflow_yaml
        self._workflow_dict = yaml.load(self.workflow_yaml)
        print self.workflow_dict
        self._main = self.workflow_dict.get('main')

    @property
    def workflow_yaml(self):
        return self._workflow_yaml

    @property
    def workflow_dict(self):
        return self._workflow_dict

    @property
    def main(self):
        return self._main

