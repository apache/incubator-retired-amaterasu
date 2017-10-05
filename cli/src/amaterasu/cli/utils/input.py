import readline


def default_input(prompt, default=''):
    """
    This prints the default value next to the prompt and the value is editable.
    Important note!
    The default value cannot be displayed in the Mac OSX default shell.
    You can bypass this by installing zsh on Mac OSX.
    As far as tested on
    :param prompt:
    :param default:
    :return:
    """
    readline.set_startup_hook(lambda: readline.insert_text(default))
    try:
        return input(prompt)
    finally:
        readline.set_startup_hook()