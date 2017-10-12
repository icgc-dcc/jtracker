from .. import __version__
from functools import lru_cache
import re
from copy import deepcopy


class Job(object):
    def __init__(self, workflow, job_json):
        self._workflow = workflow
        self._job_json = job_json

    @property
    def workflow(self):
        return self._workflow

    @property
    def job_json(self):
        return self._job_json

    @property
    @lru_cache(maxsize=None)
    def job_with_task_execution_plan(self):
        tasks = []
        scatter_tasks = dict()
        for task_name in self.workflow.workflow_tasks:  # workflow tasks defined to call tools

            call_input = self.workflow.workflow_tasks[task_name].get('input')

            called_tool = self.workflow.workflow_tasks[task_name].get('tool')
            depends_on = self.workflow.workflow_tasks[task_name].get('depends_on')
            task_dict = {
                'task': task_name,
                'input': {},
                'tool': called_tool,
                'depends_on': depends_on,
                'command': self.workflow.workflow_dict.get('tools').get(called_tool).get('command'),
                'runtime': self.workflow.workflow_dict.get('tools').get(called_tool).get('runtime')
            }

            # need to find out whether this is a task with scatter call
            scatter_setting = self.workflow.workflow_tasks[task_name].get('scatter')
            if scatter_setting:
                # TODO: if bad things happen here due to error in workflow definition or input Job JSON
                #       we will need to be able to return proper error message
                scatter_task_name = scatter_setting.get('name')
                input_variable = list(scatter_setting.get('input').keys())[0]  # must always have one key (at least for
                # now), will do error checking later
                task_suffix_field = scatter_setting.get('input').get(input_variable).get('task_suffix')
                if not task_suffix_field: task_suffix_field = input_variable

                with_items_field = scatter_setting.get('input').get(input_variable).get('with_items')

                # the with_items_field must be in the original job JSON, at least for now
                if not self.job_json.get(with_items_field):
                    # TODO: error out with clear error message
                    print('Error: can not find with_items field')
                else:
                    task_suffix_set = set([])
                    for item in self.job_json.get(with_items_field):
                        task_suffix = None
                        if isinstance(item, dict):
                            suffix_fields = task_suffix_field.split('.')
                            if len(suffix_fields) != 2 or suffix_fields[0] != input_variable:
                                print('Error: error in scatter call definition')
                            else:
                                task_suffix = item.get(suffix_fields[1])

                        elif isinstance(item, str) or isinstance(item, int):
                            task_suffix = str(item)

                        else:
                            print('Error: item from withitems field must be dict, string or int')

                        task_suffix = re.sub('[^0-9a-zA-Z]+', '_', task_suffix)  # need to avoid special character
                        task_suffix_set.add(task_suffix)

                        original_depends_on = task_dict['depends_on']
                        if original_depends_on and not isinstance(original_depends_on, list):
                            print('Error: depends_on setting invalid: depends_on must be list')

                        if original_depends_on:
                            updated_depends_on = []
                            for parent_call in original_depends_on:
                                parts = parent_call.split('@')
                                original_task_name = parts[1]
                                if original_task_name not in scatter_tasks:
                                    scatter_tasks[original_task_name] = set()

                                parts[1] = '%s.%s' % (parts[1], task_suffix)

                                scatter_tasks[original_task_name].add(parts[1])

                                updated_depends_on.append('@'.join(parts))

                            task_dict['depends_on'] = updated_depends_on

                        for i in call_input:
                            if '@' in call_input[i]:
                                value = '{{%s.%s}}' % (call_input[i], task_suffix)
                            elif len(call_input[i].split('.')) == 2:
                                if call_input[i].split('.')[0] == input_variable:
                                    value = item.get(call_input[i].split('.')[1])
                                else:
                                    value = self.job_json.get(call_input[i].split('.')[0]).get(call_input[i].split('.')[1])
                            else:
                                value = self.job_json.get(call_input[i])

                            task_dict['input'][i] = value

                        task_dict['task'] = '%s.%s' % (task_name, task_suffix)
                        tasks.append(deepcopy(task_dict))

                        # reset for the next iteration
                        task_dict['input'] = {}
                        task_dict['depends_on'] = depends_on

                    if len(task_suffix_set) < len(self.job_json.get(with_items_field)):
                        print('duplicated task suffix detected')

            else:
                for i in call_input:
                    if '@' in call_input[i]:
                        value = '{{%s}}' % call_input[i]
                    else:
                        value = self.job_json.get(call_input[i])

                    task_dict['input'][i] = value

                tasks.append(task_dict)

        # TODO: scan all tasks to update dependent tasks that are scattered tasks
        print(scatter_tasks)

        workflow_meta = {
            "language": "JTracker",
            "version": __version__,
        }

        job_with_task_execution_plan = deepcopy(self.job_json)
        job_with_task_execution_plan['tasks'] = tasks
        job_with_task_execution_plan['workflow_meta'] = workflow_meta

        return job_with_task_execution_plan
