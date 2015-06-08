#!/usr/bin/python

import os
import sys
import datetime

class TestPostJob():
    def get_defer_num(self, job_id, dag_retry):
        try:
            os.mkdir('defer_info')
        except OSError as ose:
            if ose.errno != 17: #ignore the "Directory already exists error"
                print str(ose)

        DEFER_INFO_FILE = 'defer_info/defer_num.%d.%d.txt' % (job_id, dag_retry)
        defer_num = 0

        #read retry number
        if os.path.exists(DEFER_INFO_FILE):
            try:
                with open(DEFER_INFO_FILE) as fd:
                    defer_num = int(fd.readline().strip())
            except IOError as e:
                print "I/O error({0}): {1}".format(e.errno, e.strerror)
            except ValueError:
                print "Could not convert data to an integer."
            except:
                print "Unexpected error:", sys.exc_info()[0]
                raise

        #update retry number
        try:
            with open(DEFER_INFO_FILE, 'w') as fd:
                fd.write(str(defer_num + 1))
        except IOError as e:
            print "I/O error({0}): {1}".format(e.errno, e.strerror)
        except:
            print "Unexpected error:", sys.exc_info()[0]
            raise

        return defer_num


    def execute(self, *args, **kw):
        dag_retry = int(args[2])
        job_id = int(args[5])
        postjob_log_file_name = "testpostjob.%d.%d.txt" % (job_id, dag_retry)
        fd_postjob_log = os.open(postjob_log_file_name, os.O_RDWR | os.O_CREAT | os.O_APPEND, 0644)
        os.chmod(postjob_log_file_name, 0644)
        ## Redirect stdout and stderr to the post-job log file.
        if os.environ.get('TEST_DONT_REDIRECT_STDOUT', False):
            print "Post-job started with no output redirection."
        else:
            os.dup2(fd_postjob_log, 1)
            os.dup2(fd_postjob_log, 2)
            msg = "Post-job started with output redirected to %s." % (postjob_log_file_name)
            print(msg)

        defer_num = self.get_defer_num(job_id, dag_retry)

        print("Time %s" % datetime.datetime.utcnow())
        print("Postjob deferred execution number: %s" % defer_num)

        if defer_num == 0:
            print("Here it is where I will do init actions")

        if defer_num <3:
            print("And here I will check ASO status and exit 4 instead of sleeping")
            return 4

        print("That will be the equivalent of a postjob timeout. I will probably implement it as a regular timeout instead of basing timeout on postjob deferrals number")
        return 2 #will see if have to exit 2 or exit 1 in case of postjob timeouts. I do not remember.

#print(TestPostJob().execute(0,0,0,0,0,0,0))
