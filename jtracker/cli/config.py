import yaml


class Config(object):
    def __init__(self, config_file):
        if config_file:
            try:
                with open(config_file, 'r') as f:
                    self._dict = yaml.safe_load(f)
            except:
                raise "Couldn't open config file: %s" % config_file
        else:  # otherwise default configuration is used
            self._dict = {
                'jt_home': '~/jtracker'
            }

    @property
    def dict(self):
        return self._dict

