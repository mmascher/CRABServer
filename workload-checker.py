# /bin/python
# -*- coding: utf-8 -*-
# Author : Justas Balcas justas.balcas@cern.ch
# Date : 2014-01-29
import os, os.path, sys
from optparse import OptionParser
import urllib, urllib2, httplib
from urllib2 import HTTPError, URLError
from urlparse import urljoin
import json
import hashlib
import time
import random
from RESTInteractions import HTTPRequests
import urllib

def getHashLfn(lfn):
    """
    Provide a hashed lfn from an lfn.
    """
    return hashlib.sha224(lfn).hexdigest()


server = HTTPRequests('balcas-crab2.cern.ch', '/data/certs/servicecert.pem', '/data/certs/servicekey.pem')


lfn_base = '/store/temp/user/%s/my_cool_dataset/file-%s-%s.root'

sites = ['T2_AT_Vienna', 'T2_CH_CERN']

def submitFile(user, id, i):
    user = 'jbalcas'
    id = 1
    i = 1
    now = int(time.time())
    last_update = now
    job_end_time = now
    _id=getHashLfn(lfn_base % (user, id, i))
    data = {}
# ['id', 'user', 'taskname', 'group', 'role', 'destination', 'destination_lfn'
#        116                     'source', 'source_lfn', 'size', 'publish', 'transfer_state', 'publication_state',
#        117                     'job_id', 'job_retry_count', 'type', 'rest_host', 'rest_uri']:
    file_doc = {'id': '%s' %(_id) ,
                'username': 'jbalcas',
                'taskname': 'justastaskname',
                'destination': 'T2_CH_CERN',
                'destination_lfn': lfn_base % (user, id, i),
                'source': 'T2_CH_CERN',
                'source_lfn': lfn_base % (user, id, i),
                'filesize': random.randint(1, 9999),
                'publish': 1,
                'transfer_state': 'NEW',
                'publication_state': 'NEW',
                'job_id': id,
                'job_retry_count': i,
                'type': 'log',
                'rest_host': 'cmsweb.cern.ch',
                'rest_uri': '/crabserver/prod/'
    }
    file_doc = file_doc.items()
    print file_doc
    server.put('/crabserver/dev/fileusertransfers', data=urllib.urlencode(file_doc))


submitFile('jbalcas', 1, 1)

