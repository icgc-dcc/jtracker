import os
import yaml


class Config(object):
    def __init__(self, config_file):
        if config_file:
            try:
                with open(config_file, 'r') as f:
                    self._dict = yaml.safe_load(f)
                if self.dict.get('jt_home') is None:
                    self.dict['jt_home'] = os.path.join(os.environ['HOME'], 'jthome')
            except:
                raise Exception("Couldn't open config file: %s" % config_file)
        else:  # otherwise default configuration is used
            raise Exception("Must provide configuration YAML file")

        # some validation
        if 'jt_home' not in self.dict:
            raise Exception("Config file: %s must set 'jt_home'" % config_file)
        elif not os.path.isabs(self.dict.get('jt_home')):
            raise Exception("'jt_home' in config: %s must be set with absolute path" % config_file)

    @property
    def dict(self):
        return self._dict
