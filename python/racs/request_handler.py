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

#from boto.exception import *
import sys

from s3_request_handler import *
from s3_entities import *
from thread_manager import *
from fec import *
from racs.util.head_cache import *
from racs.exceptions import *
from racs.util.stats import *

class RACSHTTPRequestHandler(S3HTTPRequestHandler):
    # May introduce inconsistency if multiple RACS proxies are working on the same
    # objects. If this is the case, should be set to None
    head_cache = HeadCache()
    release_lock = None

    # debuggery
    def write(self, msg):
        if not self.server.verbose:
            return
        sys.stdout.write(msg)

    def handle_create_bucket(self):
        self.op = 'racs:create_bucket'
        self.server.stats.record("racs:create_bucket")

        ParallelQuery(
            query_func = lambda r: r.create_bucket(self.bucket),
            parameters = self.server.get_repositories(),
            abort_on_exception = True,
            quorum_handler = self.typical_success,
            anti_quorum_handler = self.typical_failure,
            rollback_handler = lambda repo,result: repo.delete_bucket(self.bucket),
            exception_handler = self.debugging_exception_handler
        )


    def handle_delete_bucket(self):
        self.op = 'racs:delete_bucket'
        self.server.stats.record("racs:delete_bucket")
        ParallelQuery(
            query_func = lambda r: r.delete_bucket(self.bucket),
            parameters = self.server.get_repositories(),
            abort_on_exception = False,
            quorum_handler = self.typical_success,
            anti_quorum_handler = self.typical_failure,
            #rollback_handler = lambda query,repo,result: repo.create_bucket(self.bucket),
            exception_handler = self.debugging_exception_handler
        )

    op = "???"
    opargs = {}

    def get_head(self, repository, bucket, key):
        self.op = 'racs:head'
        if not self.head_cache:
            return repository.head(bucket,key)
        else:
            try:

                v= self.head_cache.get(bucket,key)
                return v
            except KeyError:
                h = repository.head(bucket,key)
                self.head_cache.put(bucket,key,h)
                return h

    def typical_success(self, query, headers={}):
        if thread_manager.verbose:
            print "[Success: 200]"
        self.send_response(200)
        self.send_id_headers()
        self.send_header('Connection','close')
        self.send_header('Content-Length','0')
        for k,v in headers.items():
            self.send_header(k,v)
        self.end_headers()

        if self.release_lock:
            self.release_lock()
        record_operation(self.op,elapsed=self.elapsed(),**self.opargs)

    def debugging_exception_handler(self, query, param, exception):
        import traceback
        tb = traceback.format_exc()
        try:
            exception.status
        except:
            # Not an S3 exception?
            # FIXME: check for this better
            print >> sys.stderr, "ParallelQuery exception thrown for %s" % repr(param)
            print >> sys.stderr, tb

    def typical_failure(self, query=None, headers={}, status=None):

        try:
            exception = query.exceptions.values()[0]
            status = exception.status
        except AttributeError:
            status = 500
        except IndexError:
            if status is None:
                status = 500
        if thread_manager.verbose:
            print "[Failure: %s]" % status

        self.send_response(status)
        self.send_id_headers()
        self.send_header('Connection','close')
        for k,v in headers.items():
            self.send_header(k,v)
        self.end_headers()

        if self.release_lock:
            self.release_lock()
        record_operation(self.op+"_fail",elapsed=self.elapsed(),**self.opargs)


    def handle_put_object(self, cache_control=None, content_type=None, content_length=None, 
                          content_md5=None, content_disposition=None, content_encoding=None, 
                          expires=None, x_amz_acl=None, x_amz_meta=None, acl=None):
        
        headers = {}
        self.op = 'racs:put_object'


        if acl:
            print >> self.server.log, "Warning: handle_put_object received acl=%s. ACL not implemented; ignored" % acl

        if x_amz_meta:
            for k,v in x_amz_meta.items():
                if not k.startswith('x-amz-meta-'):
                    raise Exception("santiy check: handle_put_object expects x_amz_meta keys to begin with x-amz-meta")
                headers[k[11:]] = v


        if cache_control:
            return self.handle_not_implemented("handle_put_object - cache_control")
        
        data = self.read(content_length=content_length, content_encoding=content_encoding)
        etag = etag_header(data)
        if content_md5:
            # FIXME return proper error
            m = compute_md5_64(data)
            if content_md5 != m:
                print >> sys.stderr, "put error: optional content_md5 does not match etag! (%s vs etag %s)" % (content_md5,m)
                self.warn("content_md5 does not match!")
                self.typical_failure(status=400)
                return
                
        print >> self.server.log, "Handle put b:%s k:%s bytes:%s etag:%s" % (self.bucket,self.key, len(data), etag['Etag'])

        self.server.stats.record("racs:put_object")


        shares, fecmeta = self.server.encode(data)
        
        headers[FECMeta.short_header] = str(fecmeta)
        
        self.opargs = {
            "bytes" : len(data),
            }
        cachekey = (str(self.bucket),str(self.key))
        self.server.racs_metacache[cachekey] = ('"%s"'%fecmeta.md5,fecmeta.size)

#        print >> self.server.log, "Handle put: %s %s type:%s headers:%s" % (self.bucket,self.key,content_type,headers)

        def dorollback(*args, **keywords):
            print "Error: put_object rollback activated, args %s kw %s" % (args,keywords)
            print "FIXME: this handler was having errors, it is now disabled"
            
        if self.server.use_zookeeper:
            elapsed = Stopwatch()
            self.release_lock = self.server.zk.request_write_lock(self.bucket,self.key)
            record_operation("zk:request_write_lock",elapsed=elapsed())
        else:
            self.release_lock = None

        ParallelQuery(
            query_func = lambda r,share: r.put_object(self.bucket, self.key, share, content_type=content_type, headers=headers),
            parameters = self.server.get_repositories(),
            #n_concurrent = 1, # should be temporary!
            supplementary_parameters = shares, #self.server.encode(data),
            abort_on_exception = True,
            quorum_handler = lambda query: self.typical_success(query,etag),
            anti_quorum_handler = self.typical_failure,
            rollback_handler = dorollback,#lambda repo,result: repo.delete_object(self.bucket, self.key),
            exception_handler = self.debugging_exception_handler
        )


    def handle_get_object(self, *args, **kw):
        if self.bucket == 'racs':
            self.handle_racs_get(*args, **kw)
        else:
            self._handle_get_object(*args, **kw)

    def _handle_get_object(self, range=None, if_modified_since=None,
                          if_unmodified_since=None, if_match=None, if_none_match=None):
        self.op = 'racs:get_object'

        self.server.stats.record("racs:get_object")

        if if_modified_since or if_unmodified_since or if_match or \
                if_none_match:
            return self.handle_not_implemented("Optional arguments")
        
        self.range = range

        repositories = self.server.choose_repositories(self.bucket, self.key, self.server.get_repositories(), self.server.k)
        n = len(repositories)
        extras = self.server.redundant_repositories(repositories)
        
        print >> self.server.log, "rq%s Handle get %s/%s.  Request from %s repositories." % (id(self),self.bucket,self.key, n)

        if self.server.use_zookeeper:
            elapsed = Stopwatch()
            self.release_lock = self.server.zk.request_read_lock(self.bucket,self.key)
            record_operation("zk:request_read_lock",elapsed=elapsed())
        else:
            self.release_lock = None

        rs = repositories + extras
        rs = rs[:self.server.k]

        ParallelQuery(
            query_func = lambda r: r.get_object(self.bucket, self.key),
            parameters = rs, 
            n_concurrent = n,
            abort_on_exception = False,
            exception_handler = self.debugging_exception_handler,
            quorum_handler = self.finish_get_object,
            anti_quorum_handler = self.finish_get_object,
        )

    def finish_get_object(self, query):
        if self.release_lock:
            elapsed = Stopwatch()
            self.release_lock()
            record_operation("zk:release_lock",elapsed=elapsed())

        k = self.server.k
        if len(query.results) >= k:
            # success!
            try:
                self.decode_object(query)
            except Exception, e:
                import traceback
                print >> self.server.log, "%s: error decoding object" % self
                traceback.print_exc()
                self.typical_failure()
        else:
            print >> self.server.log, "rq%s finish get object.  Results size = %s.  Failure." % (id(self),len(query.results))
            self.typical_failure(query)

    def __str__(self):
        return "rq%s" % id(self)

    def decode_object(self, query):
        shares = [share for share,ctype,meta in query.results.values()[:self.server.k]]
        share1, mime_type, metadata = query.results.values()[0]

        print >> self.server.log, "%s decode %s shares.  mt=%s md=%s" % (self,len(shares),mime_type,metadata)

        if metadata is None:
            print >> self.server.log, "Cannot decode; no metadata. Fail."
            raise Exception("Cannot decode: No metadata attached to object %s/%s" % (self.bucket,self.key))
        #if not metadata.contains(FECMeta.short_header
        fm = metadata.get(FECMeta.short_header,None)
        if fm is None:
            print >> self.server.log, "%s decode: No FECMeta header in metadata!" % self
            raise Exception()
        
        fecmeta = FECMeta.read(fm)

        cachekey = (str(self.bucket),str(self.key))
        self.server.racs_metacache[cachekey] = ('"%s"'%fecmeta.md5,fecmeta.size)

        data = self.server.decode(shares, fecmeta)
        etag = '"%s"'% fecmeta.md5
        
        # Verify
        actual_etag = compute_etag(data)
        if actual_etag != etag:
            print >>self.server.log, "%s ERROR Computed etag %s does not match recorded etag %s" % (self,actual_etag, etag)
        else:
            print >> self.server.log, "%s OK etag %s verified" % (self,etag)
            
        if fecmeta.size != len(data):
            print >> self.server.log, "%s ERROR actual size %d does not match recorded size %d" % (self, len(data), fecmeta.size)

        if self.range:
            m = re.compile('bytes:\s*(\d+)\s*-\s*(\d+)').match(self.range.strip())
            if not m:
                return self.handle_not_implemented("Unknown range format \"%s\""%self.range)
            a = int(m.group(1))
            b = int(m.group(2))
            data = data[a:b+1]
            self.warn('GET range queries are not efficiently implemented!!!')

        print >> self.server.log, "%s: responding %s bytes" % (self,len(data))
        self.send_response(200)
        self.send_id_headers()
        self.send_header('ETag',etag) 
        self.send_header('Content-Type',mime_type)  # FIXME
        for key, value in metadata.items():
            if key != FECMeta.short_header:
                self.send_header('x-amz-meta-'+key,value)
        if self.range:
            self.send_header('Content-Range',self.range)
        self.send_header('Content-Length',str(len(data)))
        self.send_header('Connection','close')
        self.end_headers()
        self.wfile.write(data)
        self.wfile.close()
        record_operation(self.op,elapsed=self.elapsed(),bytes=len(data))        
        print >> self.server.log, "%s: responded %s bytes" % (self,len(data))   

    def handle_delete_object(self):
        # Can't roll back
        self.server.stats.record("racs:delete_object")
        ParallelQuery(
            query_func = lambda r: r.delete_object(self.bucket,self.key),
            parameters = self.server.get_repositories(),
            abort_on_exception=False,
            quorum_handler = self.typical_success,
            anti_quorum_handler = self.typical_failure,
            exception_handler = self.debugging_exception_handler
        )
        
    def handle_get_buckets(self):
        self.server.stats.record("racs:get_all_buckets")
        self.opargs= {'n_buckets':0}
        if self.server.verify_listings_consistent:
            # Ask all repositories
            return self.handle_not_implemented()
        else:
            ParallelQuery(
                query_func = lambda r: r.get_all_buckets(),
                parameters = self.server.get_repositories(),
                n_concurrent = 1,
                quorum = 1,
                abort_on_exception = False,
                exception_handler = self.debugging_exception_handler,
                quorum_handler = self.finish_get_buckets,
                anti_quorum_handler = self.typical_failure,
            )
        
        
    def finish_get_buckets(self, query):
        self.send_response(200)
        self.send_id_headers()
        self.send_header('Connection','close')
        result = map(str,query.results.values()[0])
        buckets = [Bucket(bname) for bname in result]
        x = ListAllMyBucketsResult(self.get_owner(), buckets)
        self.send_xml_response(x)
        record_operation(self.op,elapsed=self.elapsed(),n_buckets=len(buckets))        

    def get_owner(self):
        #self.warn("Not implemented: get_owner")
        return User('00001','notimplemented')

    def handle_get_bucket(self, prefix=None, marker=None, max_keys=None, delimiter=None):
        self.op = "racs:get_bucket_contents"
        self.server.stats.record("racs:get_bucket_contents")
        if max_keys is not None:
            return self.handle_not_implemented("max_keys")

        self.prefix = prefix
        self.delimiter = delimiter
        
        if self.server.verify_listings_consistent:
            # Ask all repositories
            return self.handle_not_implemented()
        else:
            ParallelQuery(
                query_func = lambda r: r.get_bucket_contents(self.bucket, prefix, marker, delimiter),
                parameters = self.server.get_repositories(),
                n_concurrent = 1,
                quorum = 1,
                abort_on_exception = False,
                exception_handler = self.debugging_exception_handler,
                quorum_handler = self.finish_get_bucket,
                anti_quorum_handler = self.typical_failure,
            )
        
    def finish_get_bucket(self, query):
        contents = []
        common_prefixes = []

        def racsify(omd):
            # FIXME horribly inefficient
            # A head request for each object!!!
            # But we need to get the racs meta information out
            # to compute the correct size and etag!
            
            # ---------------- Now we need to get etag and size
            if omd.metadata and FECMeta.short_header in omd.metadata:#is None or FECMeta.short_header not in omd.metadata:
                fecmeta_raw = omd.metadata[FECMeta.short_header]
                fecmeta = FECMeta.read(fecmeta_raw)
                omd.etag = '"%s"' % fecmeta.md5
                omd.size = fecmeta.size
            else:
                cachekey = (str(self.bucket),str(omd.key))
                try:
                    raise KeyError()
                    omd.etag, omd.size = self.server.racs_metacache[cachekey]
                except KeyError:
                    r = self.server.get_repositories()[0]
                    # OUCH -- huge performance hit, needs to be parallelized
                    headers = self.get_head(r,self.bucket,omd.key)
                    fecmeta_raw = headers[FECMeta.short_header]
                    fecmeta = FECMeta.read(fecmeta_raw)
                    omd.etag = '"%s"' % fecmeta.md5
                    omd.size = fecmeta.size
                    self.server.racs_metacache[cachekey] = (omd.etag,omd.size)
    
        for x in query.results.values()[0]: 
            if isinstance(x, ObjectMetaData):
                racsify(x)
                contents.append(x)
            elif isinstance(x, Prefix):
                common_prefixes.append(x)
            else:
                raise Exception

        self.send_response(200)    
        self.send_id_headers()
        self.send_header('Transfer-Encoding','identity')
        self.send_header('Connection','close')
        record_operation(self.op,elapsed=self.elapsed(),n_objects=len(contents))        
        
        kw = {}
        if self.prefix is not None:
            kw['prefix'] = Prefix(self.bucket,self.prefix)
        if self.delimiter is not None:
            kw['delimiter'] = self.delimiter

        xml_content = ListBucketResult(
            name = self.bucket, 
            contents = contents,
            common_prefixes = common_prefixes,
            **kw
            )
        self.send_xml_response(xml_content)

    def version_string(self):
        return "RACS"

    def handle_head_object(self):
        self.op = "racs:head"
        print "DEBUG: handle_head_object"
        def run_head(r):
            try:
                return r.head(self.bucket,self.key)
            except NotFound:
                return None

        ParallelQuery(
            query_func = run_head, # lambda r: r.head(self.bucket,self.key),
            parameters = self.server.get_repositories(),
            n_concurrent = 1,
            quorum = 1,
            quorum_handler = self.complete_head,
            anti_quorum_handler = lambda q: self.typical_failure(q,status=404),
            exception_handler = self.debugging_exception_handler
        )

    def complete_head(self, query):
        self.server.stats.record("racs:head")
        
        headers = query.results.values()[0]
        if headers is None:
            return self.typical_failure(query, status=404)

        h2 = {}
        found_meta = False
        for k,v in headers.items():
            if k in ('Content-Type','Last-Modified'):
                h2[k] = v
            elif k in ('Etag','Content-Length'):
                continue
            elif k == FECMeta.short_header:
                fecmeta = FECMeta.read(v)
                h2['Etag'] = '"%s"' % fecmeta.md5
                h2['Content-Length'] = str(fecmeta.size)
                found_meta = True
            else:
                # custom meta data
                h2['x-amz-meta-'+k] = v

        if not found_meta:
            return self.typical_failure(query, status=500)
        else:
            self.typical_success(query, h2)
        
    def handle_get_object_acl(self):
        self.send_response(200)
        self.send_id_headers()
        self.send_header('Connection','close')
        x = AccessControlPolicy() # FIXME: placeholder, doesn't return real values
        self.send_xml_response(x)
        
        
    def handle_racs_get(self,cmd=None):

        def urlquote(s):
            return s.replace('"',"'")


        if cmd:
            cmd = cmd.replace('%27',"'")
            eval(cmd)
        if True:
            def disp_priority(r):
                raise_cmd = "?cmd=self.server.get_repository(%s).increase_priority()" % urlquote(repr(r.name))
                lower_cmd = "?cmd=self.server.get_repository(%s).decrease_priority()" % urlquote(repr(r.name))
                return "%s <small><a href=\"%s\">+</a>|<a href=\"%s\">-</a></small>" % (r.priority,raise_cmd,lower_cmd)
            def disp_active(r):
                toggle_cmd = "?cmd=self.server.toggle_repository_active(self.server.get_repository(%s))" % urlquote(repr(r.name))
                return "%s <small><a href=\"%s\">toggle</a></small>" % (r.active,toggle_cmd)


            repo_info_rows = ["<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (r.name,r.__class__.__name__,disp_priority(r), disp_active(r)) for r in self.server.get_repositories(False)]
            repo_table = """
<h3>Repositories</h3>
<table border=1>
 <tr><td>Name</td><td>Class</td><td>Fetch Priority</td><td>Active</td></tr>
 %s
</table>""" % "\n".join(repo_info_rows)

            k = self.server.k
            m = self.server.m
            stats = self.server.stats.dump()
            self.send_html_response("""
<html>
<head>
<title>RACS server config</title>
</head>
<body>
<h1><a href='/racs'>RACS control</a></h1>
<b>k = %(k)s<br>
m = %(m)s</b><br>
%(repo_table)s
<h2>Stats</h2>
<pre>%(stats)s</pre>
<br><br>
<small><a href='?cmd=self.server.stats.reset()'>Reset stats</a></small>
</body>
</html>
""" % locals())


