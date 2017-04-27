from __future__ import print_function

import os
import signal
import logging

from WMCore.Services.WMArchive.WMArchive import WMArchive

#TODO Make basedir and WMArchive URL configurable
BASE_DIR = '/Users/mmasche/CERN/wmarchive'
WMARCHIVE_URL = ''
FJR_DIR = 'new'
PROCESSED_FJR_DIR = 'processed'
BULK_SIZE = 2

QUIT = False


def logsetup():
    """ Function that set up the log framework
    """
    #TODO add timestamp
    logging.basicConfig(filename='wmarchiveprocess.log',level=logging.DEBUG)


def quit():
    """ Catches kill signals and set up 
    """
    global QUIT
    QUIT = True


def main():
    """ Main loop
    """
    signal.signal(signal.SIGINT, mw.quit_)
    signal.signal(signal.SIGTERM, mw.quit_)

    logsetup()
    logger = logging.getLogger()

    wmarchiver = WMArchive(WMARCHIVE_URL)

    while not QUIT:
        reports = os.listdir(FJR_DIR)
        docs = []
        for rep in reports[:BULK_SIZE]:
            with open(rep) as fd:
                docs.append(json.load(rep))
            #rename the file and add .tmp to mark that we are working on it
            repdir = os.path.dirname(rep)
            os.rename(rep, )
        
        response = wmarchiver.archiveData(docs)

        # Partial success is not allowed either all the insert is successful or none is
        if response[0]['status'] == "ok" and len(response[0]['ids']) == len(docs):
            logger.info("Successfully uploaded %d docs", len(docs))
        else:
            logger.warning("Upload failed and it will be retried in the next cycle: %s: %s.",
                            response[0]['status'], response[0]['reason'])
            


if __name__ == '__main__':
        main()
