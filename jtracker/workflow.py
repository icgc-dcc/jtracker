import yaml

class Workflow(object):
    def __init__(self, workflow_yaml_file=None):
        with open(workflow_yaml_file, 'r') as stream:
            self._workflow_dict = yaml.load(stream)

        self._name = self.workflow_dict.get('workflow').get('name')
        self._version = self.workflow_dict.get('workflow').get('version')

        self._workflow_steps = {}
        self._get_workflow_steps()


    @property
    def name(self):
        return self._name


    @property
    def version(self):
        return self._version


    @property
    def workflow_dict(self):
        return self._workflow_dict


    @property
    def workflow_steps(self):
        return self._workflow_steps


    def _get_workflow_steps(self):
        for st in self.workflow_dict.get('tasks'):
            self._workflow_steps[st.pop('name')] = {
                'depends_on': st.pop('depends_on'),
                'constraint': st.pop('constraint'),
                'descriptor': st
            }

