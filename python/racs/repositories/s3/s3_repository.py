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

import sys, os, httplib

try:
    import boto
except ImportError, e:
    print >> sys.stderr, """
Warning: Cannot import boto.  S3 repositories will not be available.
To download boto, please see: http://code.google.com/p/boto/
"""
    raise e

from boto.s3.connection import S3ResponseError

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import StringIO
from racs.fec import ShareMeta, FECMeta
import boto.s3.prefix
from racs.util import *
from racs.util.stats import *

# Statistics recording decorator
record = record_event_decorator('s3repo')

# We need to serialize all access to S3 because the Boto library is 
# not thread-safe.  Specifically, Boto tries to be clever and 
# reuse TCP connections, but has no way to tell if a connection it's about
# to reuse is still in use by a concurrent thread! 
synchronized = ReentrantSynchronizationDecorator()

# If you want to live dangerously, without the 
# synchronized = lambda f: f

class S3Repository(Repository):
    Repository.register('S3Repository')

    def __init__(self, server, name, s3_access_key_id, s3_secret_access_key, 
                 s3_bucket_prefix='', host_base='s3.amazonaws.com', 
                 host_bucket='%s.s3.amazonaws.com', active=True):
        Repository.__init__(self, server, name, active)
        self.s3_access_key_id = s3_access_key_id 
        self.s3_secret_access_key = s3_secret_access_key
        self.s3_bucket_prefix = s3_bucket_prefix
        self.host_base = host_base
        self.host_bucket = host_bucket
        self.conn = None
        self.establish_connection()
        
    def establish_connection(self):
        print >> self.server.log, "%s connecting" % repr(self)
        self.conn = S3Connection(self.s3_access_key_id, 
                                 self.s3_secret_access_key, 
                                 is_secure = False,
                                 proxy = self.server.proxy_host, 
                                 proxy_port = self.server.proxy_port)

    def s3(self):
        return self.conn


    def bucket_name(self, bucket):
        return self.s3_bucket_prefix + bucket

    @record
    @synchronized
    def create_bucket(self, bucket):
        return self.s3().create_bucket(self.bucket_name(bucket))

    @record
    @synchronized
    def delete_bucket(self, bucket):
        elapsed = Stopwatch()
        try:
            r = self.s3().delete_bucket(self.bucket_name(bucket))
            record_operation("s3repo:delete_bucket",elapsed=elapsed())
            return r
        except S3ResponseError, e:
            if e.status == httplib.NOT_FOUND:
                raise NoSuchBucket()
            elif e.status == httplib.CONFLICT:
                raise BucketNotEmpty()
            else:
                # Shouldn't happen
                raise e


    @record
    @synchronized
    def put_object(self, bucket, key, data, content_type=None, headers={}):
        elapsed = Stopwatch()

        h2 = {}
        for k,v in headers.items():
            if k.startswith('x-amz-meta-'):
                raise Exception("Warning: put_object headers dictionary keys " + 
                                "should not begin with x-amz-meta; it will be " + 
                                "automatically prepended")
            if k == 'Content-Type':
                raise Exception("Warning: headers dict is for x-amz-meta- fields; " + 
                                "this is not the correct way to put Content-Type. Use" + 
                                " the content_type field of put_object")
            h2['x-amz-meta-'+k] = v

        if content_type:
            h2['Content-Type'] = content_type

        s3key = Key(self.get_bucket(bucket))
        s3key.key = key
        try:
            s3key.set_contents_from_string(data, headers=h2)
        except S3ResponseError, e:
            if e.status == httplib.NOT_FOUND:
                raise NoSuchBucket()
            else:
                # Shouldn't happen
                raise e
        
        record_operation("s3repo:put_object",
                         elapsed = elapsed(),
                         bytes = len(data))
        
        
    @record
    @synchronized
    def get_object(self, bucket, key, headers={}):
        elapsed = Stopwatch()

        print >> self.server.log, "%s: get_object %s %s" % (id(self),bucket,key)
        s3key = Key(self.get_bucket(bucket))
        s3key.key = key
        print >> self.server.log, "%s: s3key %s, headers %s" % (id(self),s3key,headers)
        data = s3key.get_contents_as_string(headers=headers)
        metadata = s3key.metadata
        if data is None:
            ld = "(NONE!)"
        else:
            ld = len(data)
        print >> self.server.log, "%s: get_object returning %s bytes" % (self,ld)

        record_operation("s3repo:get_object",
                         elapsed = elapsed(),
                         bytes = len(data))

        return data, s3key.content_type, metadata
    
    @record
    @synchronized
    def delete_object(self, bucket, key):
        elapsed = Stopwatch()
        try:
            b = self.get_bucket(bucket)
            b.delete_key(key)
        except S3ResponseError, e:
            if e.status == httplib.NOT_FOUND:
                raise NoSuchBucket()
            else:
                # shouldn't happen
                raise e
        record_operation("s3repo:delete_object",elapsed=elapsed())

    @synchronized
    def get_bucket(self, bucket):
        return self.s3().get_bucket(self.bucket_name(bucket),validate=False)

    @record
    @synchronized
    def get_all_buckets(self):
        elapsed = Stopwatch()
        bucket_names = []
        for b in self.s3().get_all_buckets():
            if b.name.startswith(self.s3_bucket_prefix):
                bucket_names.append(b.name[len(self.s3_bucket_prefix):])

        record_operation("s3repo:get_all_buckets",
                         elapsed=elapsed(),
                         n_buckets = len(bucket_names))
        return bucket_names

    @record
    @synchronized
    def get_bucket_contents(self, bucket, prefix=None, marker=None, delimiter=None, 
                            max_keys=None):
        elapsed = Stopwatch()
        if max_keys is not None:
            raise NotImplementedError("max_keys")

        if prefix is None:
            prefix = ''
        if marker is None:
            marker = ''
        if delimiter is None:
            delimiter = ''
        orig_bucket = bucket
        bucket = self.get_bucket(bucket)

        blist = bucket.list(prefix=prefix,
                            delimiter = delimiter, 
                            marker = marker)
        
        contents = []
        i = 0
        for botokey in blist:
            i += 1
            if botokey.__class__.__name__.endswith('Prefix'):
                contents.append(Prefix(orig_bucket,botokey.name))
            else:
                contents.append(self._botokey_to_ObjectMetaData(botokey))

        record_operation("s3repo:get_bucket_contents",
                         elapsed=elapsed(),
                         n_objects = len(contents))

        return contents

    @record
    @synchronized
    def head(self, bucket, key):
        elapsed = Stopwatch()
        # returns headers dict with Content-Type, Etag, Content-Length
        b = self.get_bucket(bucket)
        k = b.get_key(key)
        if k is None:
            return None
        k.open_read()
        k.close()

        headers = {}
        headers['Content-Type'] = k.content_type
        headers['Last-Modified'] = k.last_modified
        headers['Etag'] = k.etag
        headers['Content-Length'] = k.size
        headers.update(k.metadata)
        record_operation("s3repo:head",elapsed=elapsed())
        return headers

    @synchronized
    def get_range(self, bucket, key, bytes, start=0):
        b = self.get_bucket(bucket)
        k = b.get_key(key)
        headers = {
            'Range':'bytes=%d-%d' % (start, start+bytes-1)
        }
        k.open('r', headers)
        fp = StringIO.StringIO()
        for bytes in k:
            fp.write(bytes)
        k.close()
        fp.seek(0)
        return fp.read()

    def _botokey_to_ObjectMetaData(self, botokey):
        try:
            lastmod = botokey.last_modified 
        except:
            botokey.bucket.get_key(botokey.name)
            lastmod = botokey.last_modified

        botokey.open_read()
        botokey.close()
        
        try:
            encoded_meta = botokey.metadata[FECMeta.short_header]
            fecmeta = None
        except Exception, e:
            print >> self.server.log, "Error getting FECMeta in botokey_to_ObjectMetaData"
            print >>self.server.log, "Metadata is: %s" %  botokey.metadata
            class Ug:
                pass
            fecmeta = Ug()
            fecmeta.size = 23
            fecmeta.md5 = str(e)
            
        if fecmeta is None:
            fecmeta = FECMeta.read(encoded_meta)

        return ObjectMetaData(
                    key = botokey.key,
                    last_modified = lastmod,
                    etag = '"%s"' % fecmeta.md5,
                    size = fecmeta.size,
                    owner = self._botouser_to_user(botokey.owner))

    def _botouser_to_user(self, botouser):
        return User(botouser.id, botouser.display_name)


