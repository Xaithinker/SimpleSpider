#/usr/bin env python
# -*- coding:utf-8 -*-

import os
import logging

class log(object):
    showOnCmd = True
    loggingLevel = logging.INFO
    loggingFile = True
    formats = '%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s'
    loggingPath = 'spider.log'
    #datefmts = "%a, %d %b %Y %H:%M:%S"
    datefmts = None

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.NullHandler())

        self.logger.setLevel(self.loggingLevel)

        self.cmdHandler = logging.StreamHandler()
        self.logger.addHandler(self.cmdHandler)

        self.fileHandler = logging.FileHandler(os.path.join(self.loggingPath), 'a', encoding='utf-8')
        self.logger.addHandler(self.fileHandler)

    def set_logger(self, showOnCmd=True, loggingFile=True, loggingLevel=logging.INFO, formating=formats):

        if showOnCmd != self.showOnCmd:
            if showOnCmd:
                self.logger.addHandler(self.cmdHandler)
            else:
                self.logger.removeHandler(self.cmdHandler)
            self.showOnCmd = showOnCmd
        
        if loggingFile != self.loggingFile:
            if self.loggingFile is not None:
                self.logger.removeHandler(self.fileHandler)
                self.fileHandler.close()
            if loggingFile is not None:
                self.fileHandler = logging.FileHandler(os.path.join(loggingFile), 'a', encoding='utf-8')
                self.logger.addHandler(self.fileHandler)
            self.loggingFile = loggingFile
        
        if loggingLevel != self.loggingLevel:
            self.logger.setLevel(loggingLevel)
            self.loggingLevel = loggingLevel

        if formating is not None:
            self.cmdHandler.setFormatter(logging.Formatter(formating, self.datefmts))
            self.fileHandler.setFormatter(logging.Formatter(formating,datefmt=self.datefmts))
        else:
            self.cmdHandler.setFormatter(logging.Formatter(self.formats, datefmt=self.datefmts))
            self.fileHandler.setFormatter(logging.Formatter(self.formats, datefmt=self.datefmts))
        self.formats = formating
        return self.logger

logger = log().set_logger
 