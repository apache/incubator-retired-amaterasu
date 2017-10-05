"""
Compatibility utilities for support of Python 2.7 and Python 3.3+
"""
from __future__ import absolute_import
import six
import abc
import os
import sys
import subprocess


class _ABC(six.with_metaclass(abc.ABCMeta)):
    """
    Compatibility patching for Python 2
    """
    pass


abc.ABC = _ABC


def _makedirs(name, mode=0o777, exist_ok=False):
    """makedirs(name [, mode=0o777][, exist_ok=False])

    Super-mkdir; create a leaf directory and all intermediate ones.  Works like
    mkdir, except that any intermediate path segment (not just the rightmost)
    will be created if it does not exist. If the target directory already
    exists, raise an OSError if exist_ok is False. Otherwise no exception is
    raised.  This is recursive.


    ported from Python3 os module so we can use it in python 2
    """
    head, tail = os.path.split(name)
    if not tail:
        head, tail = os.path.split(head)
    if head and tail and not os.path.exists(head):
        try:
            os.makedirs(head, mode, exist_ok)
        except FileExistsError:
            # Defeats race condition when another thread created the path
            pass
        cdir = os.path.curdir
        if isinstance(tail, bytes):
            cdir = bytes(os.path.curdir, 'ASCII')
        if tail == cdir:           # xxx/newdir/. exists if xxx/newdir exists
            return
    try:
        os.mkdir(name, mode)
    except OSError:
        # Cannot rely on checking for EEXIST, since the operating system
        # could give priority to other errors like EACCES or EROFS
        if not exist_ok or not os.path.isdir(name):
            raise


os.makedirs = _makedirs


try:
    FileNotFoundError = FileNotFoundError
except NameError:
    FileNotFoundError = IOError

try:
    WindowsError = WindowsError
except NameError:
    WindowsError = OSError


def run_subprocess(*args, **kwargs):
    if sys.version_info.major >= 3 and sys.version_info.minor >= 5:
        return subprocess.run(*args, check=True, **kwargs)
    else:
        return subprocess.check_output(args)


__all__ = ['FileNotFoundError', 'WindowsError', 'run_subprocess']
