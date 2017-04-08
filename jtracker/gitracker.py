import os
import git
import uuid
import shutil
import tempfile


class GiTracker(object):
    def __init__(self, git_repo_url=None, workflow_name=None, workflow_version=None):
        self._git_repo_url = git_repo_url
        self._local_git_path = tempfile.mkdtemp()  # TODO: we can allow user to choose where to clone git
        self.git_clone()
        self._gitracker_home = os.path.join(self.local_git_path,
                                            '.'.join([workflow_name, workflow_version, 'jtracker']))


    @property
    def gitracker_home(self):
        return self._gitracker_home


    @property
    def git_repo_url(self):
        return self._git_repo_url


    @property
    def local_git_path(self):
        return self._local_git_path


    def move_job(self, source=None, target=None, job_file=None, timeout=None):
        pass


    def get_job(self, job_state=None):
        os.path.join(gitracker_home, job_state)


    def git_clone(self):
        if self.git_repo_url.startswith('file://'): # for local testing only
            os.rmdir(self.local_git_path)
            shutil.copytree(self.git_repo_url.replace('file:/', ''), self.local_git_path)
            return

        repo = git.Repo.init(self.local_git_path)
        origin = repo.create_remote('origin', self.git_repo_url)
        origin.fetch()
        origin.pull(origin.refs[0].remote_head)

