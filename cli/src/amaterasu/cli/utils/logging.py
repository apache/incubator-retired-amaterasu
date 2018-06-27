import os

from logging import handlers

class AllUsersTimedRotatingFileHandler(handlers.TimedRotatingFileHandler):

    def _open(self):
        prevumask = os.umask(0o000)
        rtv = super(AllUsersTimedRotatingFileHandler, self)._open()
        os.umask(prevumask)
        return rtv