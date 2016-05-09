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

import sys, os, time, re, hashlib, binascii
from threading import Lock

_url_unquote_re = re.compile('%([0123456789abcdef][0123456789abcdef])')

def url_unquote(s):
    # e.g., replace %24 -> $
    return _url_unquote_re.sub(_sfunc,s)

def _sfunc(match):
    return chr(eval("0x"+match.group(1)))

def tuplify(x):
    if not isinstance(x, tuple) or isinstance(x, list):
        return (x,)
    else:
        return x

# decorator, works like Java serialized keyword
def serialize(func):
    lock = Lock()
    def serialized_func(*args, **keywords):
        lock.acquire()
        try:
            return func(*args, **keywords)
        finally:
            lock.release()
    return serialized_func

# decorator
def lock(lock_attr):
    def locking_decorator(func):
        def locked_func(self, *args, **keywords):
            lock = getattr(self,lock_attr)
            lock.acquire()
            rval = func(self, *args, **keywords)
            lock.release()
            return rval
        return locked_func
    return locking_decorator

def format_amz_date(d):
    if isinstance(d, str) or isinstance(d, unicode):
        return d
    elif isinstance(d, time.struct_time):
        # FIXME look up correct codes for minute, second
        return time.strftime("%Y-%m-%dT%H:00:00.000Z",d)  # FIXME
    else:
        raise Exception("unrecognized time format")

#def parse_path(p):
    # Parse a resource path of the form /path/to/resource?keyword1=value1;keyword2=value2;arg1;arg2 etc
#    parts = p.split('?')
#    parameters = {}
    #args = []
#    if len(parts) == 0:
#        raise PathParseError
#    elif len(parts) == 2:
#        for x in parts[1].split(';'):
#            if '=' in x:
#                k,v = x.split('=',1)
#                try:
#                    v = int(v)
#                except:
#                    pass
#                parameters[k.replace('-','_')] = v
#            else:
#                parameters[x.replace('-','_')] = True
#    return parts[0], parameters

def compute_etag(value):
    value = str(value)
    m = hashlib.md5()
    m.update(value)
    md5 = m.hexdigest()
    return '"%s"' % md5

def compute_md5_64(value):
    value = str(value)
    m = hashlib.md5()
    m.update(value)
    h= m.digest()
    import base64
    return base64.b64encode(h)

def etag_header(value):
    return {'Etag':compute_etag(value)}
