import yaml
import json

class Workflow(object):
    def __init__(self, workflow_yaml_file=None):
        with open(workflow_yaml_file, 'r') as stream:
            self._workflow_dict = yaml.load(stream)

        self._name = self.workflow_dict.get('workflow').get('name')
        self._version = self.workflow_dict.get('workflow').get('version')

        self._get_workflow_calls()
        #print json.dumps(self.workflow_calls)  # debug

        self._add_default_runtime_to_tasks()


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
    def workflow_calls(self):
        return self._workflow_calls


    def _get_workflow_calls(self):
        calls = self.workflow_dict.get('workflow', {}).get('calls', {})

        # converting sub_calls under scatter calls to top level, this way
        # it's easier to just handle flattened one level calls, at runtime
        # sub_calls will be instantiated with multiple parallel calls with
        # previously defined interations (ie, field defined in 'with_items')
        scatter_calls = []
        sub_calls = {}
        for c in calls:
            if '.' in c or '@' in c:
                print "Workflow definition error: call name canot contain '.' or '@', offending name: '%s'" % c
                raise
            if calls[c].get('scatter'): # this is a scatter call
                scatter_input = calls[c].get('scatter', {}).get('input', {})

                # quick way to verify the syntax is correct, more thorough validation is needed
                # it's possible to support two level of nested scatter calls,
                # for now one level only
                if not len(scatter_input) == 1 and 'with_items' in scatter_input:
                    print "Workflow definition error: invalid scatter call definition in '%s'" % c
                    raise  # better exception handle is needed

                scatter_calls.append(c)
                # expose sub calls in scatter call to top level
                for sc in calls[c].get('calls', {}):
                    if '.' in c or '@' in sc:
                        print "Workflow definition error: call name canot contain '.' or '@', offending name: '%s'" % sc
                        raise
                    if sub_calls.get(sc):
                        print "Workflow definition error: call name duplication detected '%s'" % sc
                        raise

                    sub_calls[sc] = calls[c]['calls'][sc]
                    sub_calls[sc]['scatter'] = calls[c]['scatter']  # assign scatter definition to under each sub call
                    sub_calls[sc]['scatter']['name'] = c  # add name of the scatter call here

        # now delete the top level scatter calls
        for sc in scatter_calls:
            calls.pop(sc)

        # merge sub_calls into top level calls
        duplicated_calls = set(calls).intersection(set(sub_calls))
        if duplicated_calls:
            print "Workflow definition error: call name duplication detected '%s'" % ', '.join(duplicated_calls)
            raise

        calls.update(sub_calls)

        for c in calls:
            task_called = calls[c].get('task')
            if not task_called:
                calls[c]['task'] = c

        self._workflow_calls = calls


    def _add_default_runtime_to_tasks(self):
        for t in self.workflow_dict.get('tasks', {}):
            if not 'runtime' in t:  # no runtime defined in the task, add the default one
                self.workflow_dict['tasks'][t]['runtime'] = self.workflow_dict.get('workflow', {}).get('runtime')
