description = """utilities for sending lvalert messages as part of the FakeDb infrastructure"""
author      = "reed.essick@ligo.org"

#-------------------------------------------------

import os
import sys

import subprocess as sp
import multiprocessing as mp

import tempfile

import time

#-------------------------------------------------

def alert2line( node, message ):
    '''
    prints an alert to a file in a standardized way
    specifically designed to be read out by a FileMonitor
    '''
    return "%s|%s"%(node, message)

def line2alert( line ):
    '''
    given a line from a file, does the inverse of alert2line
    '''
    return line.split("|")

#-------------------------------------------------

def forked_wait(cmd, file_obj):
    """
    used with the "--dont-wait" option to avoid zombie processes via a double fork
    main process will wait for this function to finish (quick), send the "wait" signal
    which removes this function from the process table, and then moves on.
    the forked process that this function creates become orphaned, and will automatically
    be removed from the process table upon completion.
    """
    sp.Popen(cmd, stdin=file_obj, stdout=sys.stdout, stderr=sys.stderr)

#-------------------------------------------------

def alert2listener( node, message, node2cmd={}, verbose=False, dont_wait=False ):
    '''
    forks a process via subprocess
    used within lvalertTest_listen
    '''
    if node2cmd.has_key(node):
        if dont_wait:
            file_obj = tempfile.SpooledTemporaryFile(mode="w+r", max_size=1000)
            file_obj.write(message)
            file_obj.readlines() ### bug fix for "alert_type"=="new" events
                                 ### without this, the position in file_obj gets messed up
                                 ### no idea why, but it might be related to long messages becoming multiple stanzas
            file_obj.seek(0, 0)
            p = multiprocessing.Process(target=forked_wait, args=(node2cmd[node], file_obj))
            p.start()
            p.join()
            file_obj.close()

        else:
            print sp.Popen( node2cmd[node], stdin=sp.PIPE, stdout=sp.PIPE ).communicate(message)[0] ### we don't capture the output because lvalert_listen does not

def alert2server( node, message, username=None, netrc=None, server='lvalert.cgca.uwm.edu', resource=None, max_attempts=None, verbose=False ):
    '''
    actually send the alert to the server
    used within lvalertTest_overseer
    '''
    ### set up tmpfile
    tmpfile = 'lvalert_overseer-tmp.json'
    file_obj = open(tmpfile, 'w')
    file_obj.write( message )
    file_obj.close()

    ### set up command
    cmd = ['lvalert_send', "-s", server, '-n', node, '-p', tmpfile]
    if username:
        cmd += ['-a', username]
    if netrc:
        cmd += ['-N', netrc]
    if resource:
        cmd += ['-r', resource]
    if max_attempts:
        cmd += ['-m', "%d"%max_attempts]

    ### run command
    sp.Popen( cmd, stdout=sys.stdout, stderr=sys.stderr ).wait()
    os.remove(tmpfile)

def alert2interactiveQueue( node, message, node2proc={}, verbose=False):
    '''
    pushes alert through multiprocessing connection to child process
    used within lvalertTest_listenMP
    '''
    if node2proc.has_key(node):
        proc, conn, mp_child_name = node2proc[node]
        if not proc.is_alive():
            for proc, conn, mp_child_name in node2proc.values():
                proc.terminate()
            raise RuntimeError("childProc=%s died!"%mp_child_name)

        conn.send( (message, time.time()) )

        for proc, conn, mp_child_name in node2proc.values():
            if not proc.is_alive():
                for proc, conn, mp_child_name in node2proc.values():
                    proc.terminate()
                raise RuntimeError("childProc=%s died!"%mp_child_name)

#-------------------------------------------------

class LVAlertBuffer():
    '''
    a class that wraps around a lvalert.out file produced by FakeDb
    provides some basic querying and manipulations for scripts that send lvalert messages
    '''

    def __init__(self, filenames):
        if isinstance(filenames, str):
            filenames = [filenames]
        if not filenames:
            raise ValueError("must specify at least one file to monitor!")
        self.fileMonitors = [FileMonitor(filename) for filename in filenames]

    def monitor(self, foo, cadence=0.1, **kwargs):
        '''
        monitors the file, and when a change is detected we extract the call foo with signature:
        for node, message in self.extract():
            foo( node, message, **kwargs )
        '''
        while True:
            for fileMonitor in self.fileMonitors:
                t = time.time()

                if fileMonitor.wasTouched():
                    fileMonitor.setTimestamp() ### update

                    for node, message in fileMonitor.extract():
                        foo( node, message, **kwargs )

            wait = cadence - (time.time()-t)
            if wait>0:
                time.sleep(wait)

class FileMonitor():
    '''
    wraps around a file and knows how to monitor it for changes as well as extract those changes
    WARNING: holds an open file object in 'r' mode. This may cause issues if we have too many of these things...
    '''

    def __init__(self, filename):
        if not os.path.exists(filename):
            raise ValueError('could not find filename=%s'%filename)
        self.filename = filename
        self.file_obj = open(filename, 'r')
        self.file_obj.seek(0, 2) ### go to end of file
        self.setTimestamp()

    def getTimestamp(self):
        '''
        queries the timestamp associated with this file
        '''
        return os.path.getmtime(self.filename)

    def setTimestamp(self):
        '''
        updates the timestamp for this file
        '''
        self.timestamp = self.getTimestamp()

    def wasTouched(self):
        '''
        determines whether the file has been modified
        '''
        return self.timestamp!=self.getTimestamp()

    def extract(self):
        '''
        extracts the new messages and returns them
        '''
        line = self.file_obj.readline().strip()
        nodeMessage = []
        while line:
            nodeMessage.append( line2alert(line) )
            line = self.file_obj.readline().strip()

        return nodeMessage
