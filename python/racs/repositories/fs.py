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

from racs.s3_entities import *
from racs.repository import *
from racs.exceptions import *
from racs.util.s3 import *
from racs.util.misc import *
from racs.util.stats import *

import cPickle as pickle
import os, sys, re
from os.path import abspath, join
#from os import mkdir, rmdir, remove, listdir

for f in [
    os.path.exists,
    os.mkdir,
    os.rmdir,
    os.remove,
    os.listdir,
    open,
    ]:
    n = f.__name__
    exec "%s = record_event('fs:%s')(f,suppress_failure=True)" % (n,n)

_special_chars = ['%','/','.']

record = record_event_decorator('fsrepo')

def _fs_quote(s):
    return _quote_re.sub(_quote_replace, s)

_quote_re = re.compile('[%s]'%(''.join(_special_chars)))
def _quote_replace(match):
    return "%%%02x" % ord(match.group(0))

def _fs_unquote(s):
    return _unquote_re.sub(_unquote_replace,s)

_unquote_re = re.compile('%([0123456789abcdef][0123456789abcdef])')
def _unquote_replace(match):
    return chr(eval("0x%s" % match.group(1)))

class FSRepository(Repository):
    Repository.register("FSRepository")

    def __init__(self, server, name, base_directory, active=True):
        Repository.__init__(self, server, name, active)
        self.base = abspath(base_directory)
        if not exists(self.base):
            raise Exception("FSRepository: Base directory does not exist %s" % self.base)

    def bucket_path(self, bucket_name, must_exist=False):
        bp = join(self.base, _fs_quote(bucket_name))
        if must_exist and not exists(bp):
            raise NoSuchBucket()
        return bp

    def key_path(self, bucket_name, key, must_exist=False):
        kp = join(self.bucket_path(bucket_name,must_exist),_fs_quote(key))
        if must_exist and not exists(kp):
            raise NotFound()
        return kp

    @record
    def create_bucket(self, bucket):
        bp = self.bucket_path(bucket)
        if not exists(bp):
            mkdir(bp)
        
    @record
    def delete_bucket(self, bucket):
        elapsed = Stopwatch()
        try:
            rmdir(self.bucket_path(bucket,True))
        except OSError, e:
            if e.errno == 39:
                # Not empty
                raise BucketNotEmpty()
            else:
                raise e
        record_operation("fsrepo:delete_bucket",elapsed=elapsed())

    @record
    def put_object(self, bucket, key, data, content_type=None, headers={}):
        # meta headers = http headers dictionary. Note: x-amz-meta- should NOT be supplied. 
        #
        # Overwrites objects if they already exist.
        # Throws NoSuchBucket if bucket does not exist
        elapsed = Stopwatch()

        self.bucket_path(bucket,True)
        kp = self.key_path(bucket,key)
        f = open(kp,'wb')
        f.write(data)
        f.close()
        
        etag = compute_etag(data)
        owner = None
        self.write_meta(bucket,key,content_type,headers, etag, owner)

        record_operation("fsrepo:put_object",
                         elapsed = elapsed(),
                         bytes = len(data))

    def meta_path(self, bucket, key):
        kp = self.key_path(bucket,key)
        return kp + ".meta"

    def write_meta(self, bucket, key, content_type, headers, etag, owner):
#        print >> self.server.log, "Write meta: b:%s k:%s content_type:%s header:%s" % (bucket,key,content_type,headers)
        meta = (content_type, headers, etag, owner)
        mp = self.meta_path(bucket, key)
        f = open(mp,'wb')
        pickle.dump(meta,f)
        f.close()
    
    def read_meta(self, bucket, key):

        # returns (content_type, headers, etag, owner)
        mp = self.meta_path(bucket, key)
        f = open(mp,'rb')        
        meta = pickle.load(f)
        f.close()
        
        content_type,headers,etag,owner = meta
#        print >> self.server.log, "Read meta: b:%s k:%s content_type:%s header:%s" % (bucket,key,content_type,headers)

        return meta

    @record
    def get_object(self, bucket, key):
        elapsed = Stopwatch()

        kp = self.key_path(bucket,key)
        if not exists(kp):
            raise NotFound()
        f = open(kp,'rb')
        data = f.read()
        f.close()
        content_type, headers, etag, owner = self.read_meta(bucket,key)

        record_operation("fsrepo:get_object",
                         elapsed = elapsed(),
                         bytes = len(data))

        return data, content_type, headers

    @record
    def head(self, bucket, key):
        elapsed = Stopwatch()

        kp = self.key_path(bucket,key)
        if not exists(kp):
            raise NotFound()
        content_type, metadata, etag, owner = self.read_meta(bucket,key)
        stats = os.stat(kp)
        last_modified = stats.st_mtime
        size = stats.st_size        
        headers = {
            'Content-Type': content_type,
            'Last-Modified' : format_timestamp(last_modified),
            'Etag' : '"%s"' % etag,
            'Content-Length': size,
            }
        headers.update(metadata)

        record_operation("fsrepo:head",elapsed=elapsed())

        return headers

    @record
    def delete_object(self, bucket, key):
        elapsed = Stopwatch()

        bp = self.bucket_path(bucket,True)
        kp = self.key_path(bucket,key)
        if exists(kp):
            remove(kp)
        kpm = self.meta_path(bucket,key)
        if exists(kpm):
            remove(kpm)

        record_operation("fsrepo:delete_object",elapsed=elapsed())

    @record
    def get_bucket_contents(self, bucket, prefix=None, marker=None, delimiter=None, max_keys=None):

        elapsed = Stopwatch()

        bp = self.bucket_path(bucket,True)
        keys = [_fs_unquote(k) for k in listdir(bp) if not k.endswith('.meta')]

        keys, common_prefixes = select_keys(keys, prefix, marker, delimiter, max_keys)
        entries = []

        for p in common_prefixes:
            entries.append(Prefix(bucket,p))

        for k in keys:
            content_type, headers, etag, owner = self.read_meta(bucket,k)
            stats = os.stat(self.key_path(bucket,k))
            last_modified = stats.st_mtime
            size = stats.st_size
            md = ObjectMetaData(k,last_modified,etag,size,owner,metadata=headers)
            entries.append(md)

        record_operation("fsrepo:get_bucket_contents",
                         elapsed=elapsed(),
                         n_objects = len(entries))

        return entries

    @record
    def get_all_buckets(self):
        elapsed = Stopwatch()
        d = listdir(self.base)
        record_operation("fsrepo:get_all_buckets",
                         elapsed=elapsed(),
                         n_buckets = len(d))
        return d

    # ----------- extra unit tests ---------------------
    def test0a_quoting(self):
        
        for raw,expected in [
            ("test%name","test%25name"),
            ("test/name","test%2fname"),
            ("test%%/%name","test%25%25%2f%25name"),
            ("end%","end%25"),
            ("/start","%2fstart"),
            ]:
            actual = _fs_quote(raw)
            if actual != expected:
                raise Exception("quote of %s expected %s got %s" % (repr(raw), repr(expected), repr(actual)))

    def test0b_unquoting(self):
        for expected,raw in [
            ("test%name","test%25name"),
            ("test/name","test%2fname"),
            ("test%%/%name","test%25%25%2f%25name"),
            ("end%","end%25"),
            ("/start","%2fstart"),
            ]:
            actual = _fs_unquote(raw)
            if actual != expected:
                raise Exception("unquote of %s expected %s got %s" % (repr(raw), repr(expected), repr(actual)))
