from hamcrest.core.base_matcher import BaseMatcher
import yaml
import os
import sys

str_type = str if sys.version_info[0] > 2 else unicode


def str_ok(x):
    return type(x) == str_type and len(x) > 0


class MockArgs:
    def __init__(self, **kwargs):
        for kwarg, value in kwargs.items():
            setattr(self, kwarg, value)

def collect_stats(context, path):
    for base_dir, dirs, files in os.walk(path):
        if base_dir.endswith('.git'): continue
        for f in files:
            try:
                f_path = os.path.join(base_dir, f)
                stat = os.lstat(f_path)
                context.stats_after[f_path] = stat
            except FileNotFoundError:
                pass
        for d in dirs:
            if d == '.git': continue
            try:
                d_path = os.path.join(base_dir, d)
                stat = os.lstat(d_path)
                context.stats_after[d_path] = stat
            except FileNotFoundError:
                pass


# region Custom Matchers


class MakiExistsInDirectoryAndIsStatus(BaseMatcher):
    EMPTY = 0
    VALID = 1
    INVALID = 2

    VALID_GROUPS = ['spark']
    VALID_TYPES = ['scala', 'sql', 'python', 'r']

    def __init__(self, status):
        if status == self.EMPTY:
            self._content_matcher = self._content_empty
        elif status == self.VALID:
            self._content_matcher = self._content_valid
        elif status == self.INVALID:
            self._content_matcher = self._content_invalid
        self.status = status

    def _matches(self, directory):
        maki_path = os.path.abspath(os.path.join(directory, 'maki.yml'))
        if not os.path.exists(maki_path):
            return False
        with open(maki_path) as f:
            content = f.read()
        return self._content_matcher(content)

    def _content_empty(self, content):
        return content == '' or content is None

    def _content_valid(self, content):
        maki = yaml.load(content)
        first_level_ok = 'job_name' in maki and 'flow' in maki
        if not first_level_ok:
            return False
        job_name_ok = str_ok(maki['job_name'])
        flow_ok = type(maki['flow']) == list and len(maki['flow']) > 0
        flow_steps_ok = True
        for step in maki['flow']:
            step_name_ok = lambda: 'name' in step and str_ok(step['name'])
            step_runner_ok = lambda: 'runner' in step and type(step['runner']) == dict \
                                     and 'group' in step['runner'] and str_ok(step['runner']['group']) and step['runner']['group'] in self.VALID_GROUPS \
                                     and 'type' in step['runner'] and str_ok(step['runner']['type']) and step['runner']['type'] in self.VALID_TYPES
            file_ok = lambda: 'file' in step and str_ok(step['file'])
            step_ok = type(step) == dict and step_name_ok() and step_runner_ok() and file_ok()
            if not step_ok:
                flow_steps_ok = False
                break
        return job_name_ok and flow_ok and flow_steps_ok

    def _content_invalid(self, content):
        return not self._content_valid(content)

    def describe_to(self, description):
        description.append_text('Maki exists and is {}'.format(self.status))


has_valid_maki = MakiExistsInDirectoryAndIsStatus(MakiExistsInDirectoryAndIsStatus.VALID)
has_empty_maki = MakiExistsInDirectoryAndIsStatus(MakiExistsInDirectoryAndIsStatus.EMPTY)
has_invalid_maki = MakiExistsInDirectoryAndIsStatus(MakiExistsInDirectoryAndIsStatus.INVALID)


def noop(*args, **kwargs):
    pass

# endregion
