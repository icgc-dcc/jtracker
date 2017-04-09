import yaml

class Workflow(object):
    def __init__(self, workflow_yaml_file=None):
        with open(workflow_yaml_file, 'r') as stream:
            self._workflow_dict = yaml.load(stream)

    @property
    def workflow_dict(self):
        return self._workflow_dict


    # this class needs to expand more
