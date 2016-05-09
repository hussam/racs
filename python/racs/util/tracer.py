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

import inspect, sys

# Used to instrument third-party libraries with statistics gathering 
# Runs up the call stack until it finds a frame of interest

class AbortTrace(Exception):
    pass

class Tracer(object):
    suppress_failure = True
    verbose = False
    _cache = None

    def throw(self,objs=[]):
        f = inspect.currentframe().f_back

        if self.accept_args(*objs):
            self.apply()
            return

        while f is not None:
            if self.verbose:
                try:
                    slf = f.f_locals['self']
                    try:
                        st = slf.server.stats
                    except:
                        st = None
                except:
                    slf = None
                    st = None

                print "trace: ", f.f_code.co_name, repr(slf), st

            try:
                if self.accept(f):
                    self.apply()
                    break
            except AbortTrace:
                break
            f = f.f_back

        if f is None:
            self.not_found()

    def accept(self, frame):
        return False

    def apply(self):
        # not implemented
        pass

    def not_found(self):
        if not self.suppress_failure:
            # called if we have inspected the entire call stack without accepting any frame
            raise Exception("Tracer failed: %s" % repr(self))
#        else:
#            print >> sys.stderr,"suppress fail %s" % repr(self)

    # For use as a decorator, or for throwing (with no function argument)
    def __call__(self, f=None, suppress_failure=True):
        if not f:
            self.throw()
        else:
            self.func = f
            self.suppress_failure = suppress_failure
            def wrapper(*args,**keywords):
                self.throw(args)
                return self.func(*args, **keywords)
            return wrapper
    
    def accept_args(self, *objs):
        return False
