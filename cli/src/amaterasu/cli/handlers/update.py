"""
Update the repository FS based on the maki file.

Usage: ama update [<path>]

Options:
    -h --help       Show this screen

"""

import os

from .base import ValidateRepositoryMixin, MakiMixin, BaseRepositoryHandler, HandlerError


class UpdateRepositoryHandler(BaseRepositoryHandler, MakiMixin, ValidateRepositoryMixin):
    """
    Handler that updates a repository based on its Maki.yml file
    Currently, it fills in the src directory with templates for the specified source files
    If a source file exists in the src directory that is not specified in the Maki.yml file,
    the user will be prompted to take action.
    """

    def _validate_path(self):
        super(UpdateRepositoryHandler, self)._validate_path()
        validation_errors = self._validate_repository()
        if validation_errors:
            raise HandlerError('Repository structure isn\'t valid!', inner_errors=validation_errors)

    def _load_existing_sources(self):
        return set(os.listdir(os.path.join(self.dir_path, 'src')))

    def _load_maki_sources(self):
        maki = UpdateRepositoryHandler.load_maki(os.path.join(self.dir_path, 'maki.yml'))
        source_files = {step['file'] for step in maki['flow']}
        return source_files

    def _write_sources_to_fs(self, sources):
        for file in sources:
            with open(os.path.join(self.dir_path, 'src', '{}'.format(file)), 'w'):
                pass

    def _get_user_input_for_source_not_on_maki(self, source): # This was separated out from _handle_source_not_on_maki so we can mock it
        print("The following source file: \"{}\" doesn't exist in the maki.yml file.".format(source))
        decision = input("[k]eep [d]elete [A]ll (e.g.: \"dA\" delete all): ").strip()
        while decision not in ['k', 'd', 'kA', 'dA']:
            print('Invalid choice "{}"')
            decision = input("[k]eep [d]elete [A]ll (e.g.: \"dA\" delete all): ")
        return decision

    def _handle_sources_not_on_maki(self, sources):
        """
        We ask the user to give us answers about sources that are not in the maki file.
        Currently, we only support either keeping them, or deleting them.
        :param sources:
        :return:
        """
        sources_iter = iter(sources)
        for source in sources_iter:
            decision = self._get_user_input_for_source_not_on_maki(source)
            if decision == 'dA':
                os.remove(os.path.join(self.dir_path, 'src', '{}'.format(source)))
                break
            elif decision == 'kA':
                return
            else:
                if decision == 'd':
                    os.remove(os.path.join(self.dir_path, 'src', '{}'.format(source)))
                else:
                    continue
        else:
            return

        # In case the user decided to do delete the rest in bulk:
        for source in sources_iter:
            os.remove(os.path.join(self.dir_path, 'src', '{}'.format(source)))

    def handle(self):
        """
        The idea is as following:
        Find all the sources that are present in the repository
        Find all the sources that are mentioned in the maki file
        If a source is mentioned in the maki and doesn't exist in the repository, create it
        If a source exists in the repository and doesn't exist in the maki, ask for user intervention
        :return:
        """
        existing_sources = self._load_existing_sources()
        maki_sources = self._load_maki_sources()
        sources_not_in_fs = maki_sources.difference(existing_sources)
        sources_not_in_maki = existing_sources.difference(maki_sources)
        self._write_sources_to_fs(sources_not_in_fs)
        self._handle_sources_not_on_maki(sources_not_in_maki)


def get_handler(**kwargs):
    return UpdateRepositoryHandler