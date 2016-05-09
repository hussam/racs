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

import time, os

class LogStreamWrapper(object):
    def __init__(self, stream):
        self.stream = stream

    def write(self, msg):
        if not msg.strip():
            return
        msg = msg + "\n"
        msg = msg.split("\n")
        stamp = "[%s] " % int(time.time())
#        self.stream.write(stamp + msg[0])
        pfx = " " * len(stamp)
        def f(m):
            if m:
                return pfx + m
            else:
                return m

        msg = [stamp + msg[0]] + map(f,msg[1:])
        self.stream.write("\n".join(msg))
        
class NullOutStream(object):
    def write(self, m):
        pass


class FileAppendOutStream(object):
    def __init__(self, filename, overwrite=True):
        if overwrite:
            if os.path.exists(filename):
                os.remove(filename)
        self.filename = filename

    def write(self, msg):
        f = open(self.filename,'a')
        f.write(msg)
        f.close()
            
            
        
