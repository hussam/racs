# Copyright 2010 Cornell University. All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions 
# are met:
# 
#   1. Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# 
#   2. Redistributions in binary form must reproduce the above 
# copyright notice, this list of conditions and the following 
# disclaimer in the documentation and/or other materials provided 
# with the distribution.
# 
#   3. Neither the name of the University nor the names of its 
# contributors may be used to endorse or promote products derived 
# from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY CORNELL UNIVERSITY  ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL CORNELL UNIVERSITY OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR 
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY 
# OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE 
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
# DAMAGE.
# 
# The views and conclusions contained in the software and documentation
# are those of the authors and should not be interpreted as representing 
# official policies, either expressed or implied, of Cornell University.
# 

import traceback
from threading import Thread, Lock
import sys, time
from util import *

Aborted = object()

MAX_THREADS=15

class ParallelQuery(object):


    finished = False  # set to true if quorum or antiquorum has been reached

    def __init__(self, query_func, parameters, quorum=None, abort_on_exception=False, 
                 quorum_handler=None, exception_handler=None, completion_handler=None, 
                 anti_quorum_handler=None, rollback_handler=None,
                 supplementary_parameters = None, termination_handler = None,
                 n_concurrent = None
                 ):
        # query_func takes a single argument.  parameters is a list of such arguments.
        # quorum_handler takes a single argument, which is this parallelquery instance
        # anti_quorum_handler takes a single argument, which is this parallelquery instance.  It is called if a quorum cannot be reached due to exceptions.
        # exception_handler takes three arguments: the parallelquery instance, the parameter being run, and the exception thrown
        # completion_handler takes three arguments: the parallelquery instance, the parameter being run, and the result
        #         completion_handler is called when each single query is finished
        # rollback_handler takes three arguments: (parallelquery, param, result)
        # termination_handler is called when all queries have returned or failed
        #    it takes one argument, the parallelquery instance
        # n_concurrent is the maximum concurrent queries we want to run

        if n_concurrent is None:
            n_concurrent = len(parameters)

        assert(n_concurrent > 0)

        self.n_concurrent = n_concurrent 

        if abort_on_exception:
            original_query_func = query_func
            def qf(*args):
                if self.abort:
                    return Aborted
                else:
                    return original_query_func(*args)
            query_func = qf

        self.rollback_handler = rollback_handler
        self.query_func = query_func
        self.abort_on_exception = abort_on_exception
        self.parameters = parameters
        if supplementary_parameters is None:
            supplementary_parameters = [()] * len(self.parameters)
        supplementary_parameters = map(tuplify,supplementary_parameters)
        self.supplementary_parameters = dict(zip(parameters,supplementary_parameters))
        self.abort = False
        self.termination_handler = termination_handler
        if quorum is None:
            quorum = len(parameters)
        self.exception_handler = exception_handler
        self.completion_handler = completion_handler
        self.quorum = quorum
        self.quorum_handler = quorum_handler
        self.anti_quorum_handler = anti_quorum_handler
        self.results = {} # maps parameter => result, only for completed parameters
        self.exceptions = {}  # maps paramter => exception, only for those that have thrown exceptions
        self.run()

    def run(self):
        self.tasks = []
        self.n_running_tasks = 0

        for param in self.parameters:
            supp = self.supplementary_parameters[param]
            self.tasks.append(dict(
                f = self.query_func, 
                args = (param,)+supp, 
                callback = lambda result,p=param: self.pq_callback(p,result), 
                exception_handler = lambda x,p=param: self.pq_exception_handler(p,x)))

        self.launch_tasks()

    @serialize
    def launch_tasks(self):
        if self.abort or self.finished:
            return
        j = self.n_concurrent - self.n_running_tasks
        if j > 0 and not self.abort:
            temp = self.tasks[:j]
            self.tasks = self.tasks[j:]
            thread_manager.queue_tasks(temp)
            self.n_running_tasks += len(temp)

    def pq_rollback(self, param):
        if self.rollback_handler:
            try:
                self.rollback_handler(self, param, self.results[param])
            except Exception, f:
                print >> sys.stderr, "Suppressing exception raised by rollback handler %s for param %s: %s" % (self.rollback_handler, param, f)
                
    def pq_callback(self, param, return_value):
        self.n_running_tasks -= 1
        self.launch_tasks()

        if return_value is Aborted:
            return

        self.results[param] = return_value

        if self.completion_handler:
            try:
                self.completion_handler(self, param, return_value)
            except Exception, f:
                print >> sys.stderr, "Suppressing exception raised by completion handler %s for param %s: %s" % (self.completion_handler, param, f)
        
        self.check_quorum()

        if self.abort:
            self.pq_rollback(param)
        
        if len(self.results) + len(self.exceptions) == len(self.parameters):
            self.pq_terminate() # finished!

    def pq_terminate(self):
        if self.termination_handler:
            try:
                self.termination_handler(self)
            except Exception, f:
                print >> sys.stderr, "Suppressing exception raised by termination handler %s: %s" % (self.termination_handler,f)

    @serialize
    def check_quorum(self):
        if self.finished: 
            return
            
        if self.abort:
            self.pq_anti_quorum()

        n_succeed = len(self.results)
        n_fail = len(self.exceptions)
        n_total = len(self.parameters)
        if n_succeed >= self.quorum:
            self.pq_quorum()
        elif n_total - n_fail < self.quorum:
            self.pq_anti_quorum()

    def pq_quorum(self):
        self.finished = True
        if self.quorum_handler:
            try:
                self.quorum_handler(self)
            except Exception, f:
                import traceback
                traceback.print_exc()
                print >> sys.stderr, "(printed) Suppressing exception raised by quorum handler %s: %s" % (self.quorum_handler, f)


    def pq_exception_handler(self, param, exception):


        self.n_running_tasks -= 1
        self.launch_tasks()

        self.exceptions[param] = exception
        
        if thread_manager.verbose:
            print "[%s: %s .. win: %d  fail: %d  ?: %d  running: %d]" % (param, exception.__class__.__name__, len(self.results), len(self.exceptions), len(self.parameters) - len(self.exceptions) - len(self.results), self.n_running_tasks)

        if self.exception_handler:
            try:
                self.exception_handler(self, param, exception)
            except Exception, f:
                print >> sys.stderr, "Suppressing exception raised by exception handler %s for param %s: %s" % (self.exception_handler, param, f)
        
        if self.abort_on_exception:
            self.abort = True
        self.check_quorum()

        if len(self.results) + len(self.exceptions) == len(self.parameters):
            self.pq_terminate() # finished!

    def pq_anti_quorum(self):
        self.finished = True
        if self.anti_quorum_handler:
            try:
                self.anti_quorum_handler(self)
            except Exception, f:
                traceback.print_exc()
                print >> sys.stderr, "Suppressing exception raised by anti_quorum_handler %s: %s" % (self.anti_quorum_handler, f)

        # rollback whatever was completed
        for param in self.results.keys():
            self.pq_rollback(param)


class PoolThread(Thread):
    def __init__(self, manager):
        Thread.__init__(self)
        self.manager = manager
        self.daemon = True

    def run(self):
        while True:
            task = self.manager.get_task()
            if task is None:
                break
            f, args, keywords, callback, exception_handler = task
            try:
                result = f(*args, **keywords)
            except Exception, e:
                if exception_handler:
                    try:
                        exception_handler(e)
                    except Exception, f:
                        print >> sys.stderr, "Suppressing exception handler error [%s] in %s" % (f,exception_handler)
                continue
            if callback:
                try:
                    callback(result)
                except Exception, f:
                    print >> sys.stderr, "Suppressing callback error [%s] with %s" % (f,exception_handler)
                
class ThreadManager(Thread):
    verbose = False

    def __init__(self, max_threads = MAX_THREADS):
        Thread.__init__(self)
        self.daemon = True
        self.max_threads = max_threads
        self.threads = set()
        self.tasks = []
        self.interval = 1.0
        self.running = False
        self.lock = Lock()
    
    def queue_task(self, f, args=(), keywords={}, callback=None, exception_handler=None):
        self.lock.acquire()
        self.tasks.append( (f, args, keywords, callback, exception_handler) )
        self.lock.release()
        self.maintain()

    def queue_tasks(self, tasks): 
        def task(f, args=(), keywords={}, callback=None, exception_handler=None):
            return  (f, args, keywords, callback, exception_handler)

        self.lock.acquire()
        for t in tasks:
            self.tasks.append(task(**t))
        self.lock.release()
        self.maintain()
    
    def run(self):
        self.running = True
        while self.running:
            time.sleep(self.interval)
            self.maintain()

    _lst = None

    @lock('lock')
    def maintain(self):
            
        self.reap()
        nthreads = len(self.threads)
        ntasks = len(self.tasks)
        if ntasks > nthreads and nthreads < self.max_threads:
            for i in xrange(ntasks-nthreads):
                self.spawn()

        if self.verbose:
            nxt = int(time.time() /3)
            if self._lst != nxt:
                self._lst = nxt
                print "[%d task threads running]" % (len(self.threads))

        
    def spawn(self):
        # create a new pool thread
        t = PoolThread(self)
        self.threads.add(t)
        t.start()
    
    @lock('lock')
    def get_task(self):
        if len(self.tasks) == 0:
            return None
        else:
            return  self.tasks.pop(0)

    def reap(self):
        junk = []
        for t in list(self.threads):
            if not t.isAlive():
                junk.append(t)
                self.threads.remove(t)
        for t in junk:
            t.join()

thread_manager = ThreadManager()
