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

from racs.util.stats import *

# Insert custom cloudfiles into the python path
import sys, os, httplib
rs_repo_dir = os.path.abspath(os.path.split(__file__)[0])
sys.path.insert(0,rs_repo_dir)

try:
    import cloudfiles
except ImportError, e:
    print >> sys.stderr, """Warning: Cannot import cloudfiles package.  
You will not be able to use Rackspace Cloudfiles repositories.
To obtain the Rackspace API, see: http://cloudfiles.rackspacecloud.com/index.php/Python_API_Installation
"""
    raise e

from cloudfiles.connection import ResponseError

_special_chars = ['/','#']

def _rs_quote(s):
    return _quote_re.sub(_quote_replace, s)

_quote_re = re.compile('[%s]'%(''.join(_special_chars)))
def _quote_replace(match):
    return "#%02x" % ord(match.group(0))

def _rs_unquote(s):
    return _unquote_re.sub(_unquote_replace,s)

_unquote_re = re.compile('#([0123456789abcdef][0123456789abcdef])')
def _unquote_replace(match):
    return chr(eval("0x%s" % match.group(1)))

record = record_event_decorator('rsrepo')

class RSRepository(Repository):
    Repository.register('RSRepository')
    def __init__(self, server, name, username, api_key, container_prefix, active=True):
        self.username = username
        self.api_key = api_key
        self.container_prefix = container_prefix
        Repository.__init__(self, server, name, active=active)
        
    def container_name(self, bucket_name):
        return self.container_prefix + _rs_quote(bucket_name)

    def object_name(self, key_name):
        return _rs_quote(key_name)

    def get_connection(self):
        return cloudfiles.get_connection(self.username, self.api_key)

    # --------- overridden repository methods
    @record
    def create_bucket(self, bucket):
        # Raises an exception only if bucket creation failed.
        # If bucket already exists, create has no effect but succeeds
        conn = self.get_connection()
        cn = self.container_name(bucket)
        conn.create_container(cn)

    @record
    def delete_bucket(self, bucket):
        elapsed = Stopwatch()
        # If bucket does not exists: raise NoSuchBucket 
        # If bucket is not empty: raises BucketNotEmpty
        conn = self.get_connection()
        cn = self.container_name(bucket)
        try:
            conn.delete_container(cn)        
        except Exception, e:
            if e.__class__.__name__ == 'ResponseError':
                if e.status == httplib.NOT_FOUND:
                    raise NoSuchBucket()
                else:
                    raise e
            elif e.__class__.__name__ =='ContainerNotEmpty':
                raise BucketNotEmpty()      
            else:
                raise e
        record_operation("rsrepo:delete_bucket",elapsed=elapsed())

    @record
    def put_object(self, bucket, key, data, content_type=None, headers={}):
        elapsed = Stopwatch()
        # meta headers = http headers dictionary. Note: x-amz-meta- should NOT be supplied. 
        #
        # Overwrites objects if they already exist.
        # Throws NoSuchBucket if bucket does not exist
        conn = self.get_connection()
        cn = self.container_name(bucket)
        on = self.object_name(key)
        try:
            obj = conn.get_container(cn).create_object(on)
        except Exception, e:
            if e.__class__.__name__ == 'NoSuchContainer':
                raise NoSuchBucket()
            else:
                raise e
        if content_type:
            obj.content_type = content_type
        obj.write(data, verify=True)
        obj.metadata = headers
        obj.sync_metadata()
        record_operation("rsrepo:put_object",
                         elapsed = elapsed(),
                         bytes = len(data))
    @record
    def get_object(self, bucket, key):
        elapsed = Stopwatch()
        # Returns a triple (data, content-type, metadata dict)
        # Throws NotFound if bucket/path does not exist
        conn = self.get_connection()
        cn = self.container_name(bucket)
        on = self.object_name(key)        
        obj = conn.get_container(cn).get_object(on)
        data = obj.read()
        content_type = obj.content_type
        headers = obj.metadata
        record_operation("rsrepo:get_object",
                         elapsed = elapsed(),
                         bytes = len(data))
        return data, content_type, headers

    @record
    def delete_object(self, bucket, key):
        elapsed = Stopwatch()
        # Succeeds if object never existed in the first place
        # Raises NoSuchBucket if bucket does not exist
        conn = self.get_connection()
        cn = self.container_name(bucket)
        on = self.object_name(key)    
        try:
            conn.get_container(cn).delete_object(on)        
        except Exception, e:
            if e.__class__.__name__ == 'NoSuchContainer':
                raise NoSuchBucket()
            elif e.__class__.__name__ == 'ResponseError' and e.status == httplib.NOT_FOUND:
                # Ignore this error.
                pass
        record_operation("rsrepo:delete_object",elapsed=elapsed())

    @record
    def get_bucket_contents(self, bucket, prefix=None, marker=None, delimiter=None, max_keys=None):
        elapsed = Stopwatch()
        # returns an iterable of s3_entries.ObjectMetaData instances.  (No Prefix objects!)
        # prefix, marker, and delimiter correspond to Amazon's meanings
        # Raises NoSuchBucket if buckets do not exist
        #
        # prefix: Limits the response to keys that begin with a certain prefix
        # delimiter: Used to group keys. See http://docs.amazonwebservices.com/AmazonS3/2006-03-01/API/
        # marker: key-marker in amazon docs. Specifies key to start with when listing.  
        # max_keys: Currently not used by RACS. Does not need to be implemented... yet.

        if prefix is None:
            prefix = ''
            
        if marker is None:
            marker = ''
        
        if delimiter is None:
            delimiter = ''

        prefix = _rs_quote(prefix)
        marker = _rs_quote(marker)
        delimiter = _rs_quote(delimiter)

        conn = self.get_connection()
        cn = self.container_name(bucket)
        container = conn.get_container(cn)

        kw = {}
        if prefix:
            kw['prefix'] = prefix
        if marker:
            kw['marker'] = marker
        if max_keys:
            kw['limit'] = max_keys

        infos = container.list_objects_info(**kw)
        
        entries = []
        # info is a list of dictionaries containing keys:
        #   name, hash, size, type

        for info in infos:
            key = _rs_unquote(info['name'])
            etag = info['hash']
            size = info['bytes']
            last_modified = info['last_modified']
            md = ObjectMetaData(key, last_modified, etag, size)
            entries.append(md)

        record_operation("rsrepo:get_bucket_contents",
                         elapsed=elapsed(),
                         n_objects = len(entries))
        # FIXME does NOT return Prefix objects for common prefixes!
        return entries

    @record
    def get_all_buckets(self):
        elapsed = Stopwatch()
        # Returns a list of available bucket names
        conn = self.get_connection()
        temp = []
        for name in conn.list_containers():
            if not name.startswith(self.container_prefix):
                continue
            name = name[len(self.container_prefix):]
            temp.append(_rs_unquote(name))
        record_operation("rsrepo:get_all_buckets",
                         elapsed=elapsed(),
                         n_buckets = len(temp))
        return temp

    @record
    def head(self, bucket, key):
        elapsed = Stopwatch()
        # returns headers dict with Content-Type, Etag, Content-Length, and meta-info headers
        conn = self.get_connection()
        cn = self.container_name(bucket)
        on = self.object_name(key)   
        try:
            obj = conn.get_container(cn).get_object(on)
        except Exception,e:
            if e.__class__.__name__ == 'NoSuchObject':
                raise NotFound()
            else:
                raise e

        # s3cmd insists on this timestamp format
        lastmod = obj.last_modified.split(".")[0] + ".000Z"

        headers = {
            'Content-Type': obj.content_type,
            'Last-Modified' : lastmod, 
            'Etag' : '"%s"' % obj.etag,
            'Content-Length': obj.size,
            }
        headers.update(obj.metadata)
        record_operation("rsrepo:head",elapsed=elapsed())
        return headers
