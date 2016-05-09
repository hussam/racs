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

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import re, sys
from urlparse import urlparse

from racs.util import *
from racs.util.stats import *
from racs.s3_entities import *

S3_FQDN = 's3.amazonaws.com'

class S3HTTPRequestHandler(BaseHTTPRequestHandler):
    verbose = True
    protocol_version = 'HTTP/1.1'

    def __init__(self, *args, **keywords):
        BaseHTTPRequestHandler.__init__(self, *args, **keywords)
        
    _prepped = False
    _bucket_re = re.compile('^(.*)\.%s$' % S3_FQDN.replace('.','\.'))
    def prep(self):
        if self._prepped:
            return
        self._prepped = True

        if self.path.startswith('/'):
            self.path = 'http://racs.%s%s' % (S3_FQDN,self.path)

        parsed = urlparse(url_unquote(self.path))

        if parsed.scheme != 'http':
            self.handle_not_implemented('Expected http schemed URL, got: %s' % self.path)
            return

        # was this sent to a bucket subdomain?
        bucket_match = self._bucket_re.match(parsed.netloc)
        if bucket_match:
            # ... yes
            self.bucket = bucket_match.group(1)
            if parsed.path == "/":
                self.key = None
            else:
                self.key = parsed.path[1:]
        else:
            # ... no
            self.bucket = None
            self.key = None
        self.parameters = {}

        spl = parsed.query.split(';')
        spl2 = parsed.query.split('&')
        if len(spl) < len(spl2):
            spl = spl2
        for temp in spl:
            if len(temp) == 0:
                continue
            try:
                key, value = temp.split('=')
            except:
                self.parameters[temp] = True
                continue
                #print "key/value can't split param?", temp
                #return self.handle_not_implemented()
            try:
                value = int(value)
            except:
                pass
            self.parameters[key] = value
        
    # HACK assumes host is always Amazon
    # not necessary, when run as proxy we can get it from the url
#    def get_bucket_from_host(self):
#        m = self._bucket_re.match(self.headers['Host'])
#        if not m:
#            return None
#        return m.group(1)

    @record_event('proxy-in:GET')
    def do_GET(self):
        self.elapsed = Stopwatch()
        self.prep()
        if not self.key:
            if self.bucket:
                if 'location' in self.parameters:
                    self.handle_get_bucket_location()
                else:
                    self.handle_get_bucket(**self.parameters)
            else:
                self.handle_get_buckets() # no parameters
        else:
            self.headers_to_parameters([
                    'Range','If-Modified-Since','If-Unmodified-Since','If-Match', 'If-None-Match'
                    ])
            if 'acl' in self.parameters:
                del self.parameters['acl']
                self.handle_get_object_acl(**self.parameters)
            else:
                self.handle_get_object(**self.parameters)
                    
    @record_event('proxy-in:PUT')        
    def do_PUT(self):
        self.elapsed = Stopwatch()
        # make bucket:  bucket != None, path = /
        # put object:  bucket != None, path = /key
        self.prep()
        
        if not self.key:
            self.handle_create_bucket()
        else:
            if 'x-amz-copy-source' in self.headers:
                # copy operation
                self.headers_to_parameters([
                        'x-amz-copy-source',
                        'x-amz-metadata-directive',
                        'x-amz-copy-source-if-match',
                        'x-amz-copy-source-if-none-match', 
                        'x-amz-copy-source-if-unmodified-since', 
                        'x-amz-copy-source-if-modified-since'
                       ])
                self.handle_copy_object(**self.parameters)
            elif 'requestPayment' in self.parameters:
                self.handle_request_payment()
            else:
                self.headers_to_parameters([
                        'Cache-Control', 
                        'Content-Type', 
                        'Content-Length', 
                        'Content-MD5', 
                        'Content-Disposition', 
                        'Content-Encoding', 
                        'expires', 
                        'x-amz-acl', 
                    ])
                self.handle_put_object(**self.parameters)
            
    def do_POST(self):
        self.elapsed = Stopwatch()
        self.prep()
        self.handle_not_implemented()

    @record_event('proxy-in:DELETE')
    def do_DELETE(self):
        self.elapsed = Stopwatch()
        self.prep()
        if not self.key:
            self.handle_delete_bucket()
        else:
            self.handle_delete_object()

    @record_event('proxy-in:HEAD')            
    def do_HEAD(self):
        self.elapsed = Stopwatch()
        print >> sys.stderr, "DEBUG: do_HEAD"
        self.prep()
        print >> sys.stderr, "DEBUG:    do_HEAD prep OK, bucket=%s key=%s" % (self.bucket,self.key)
        self.handle_head_object()

    def warn(self, msg):
        print >> sys.stderr, 'Warning:',msg

    def dump(self, stack_offset = 0):
        import inspect
        frame = inspect.stack()[1+stack_offset]

        calling_func_name = frame[3]
    
        print '-'*40, calling_func_name

        attrs = ['command','client_address','bucket','path','parameters']
        for x in attrs:
            try:
                print "%s: %s "% (x,getattr(self,x))
            except:
                print "%s: no such attribute!" % x
        print "headers:"
        for k,v in self.headers.items():
            print "    %s: %s" % (k,v)

        print ""


    def handle_not_implemented(self,msg=None):

        if self.verbose:
            import inspect
            frame = inspect.stack()[1]
            calling_func_name = frame[3]
            self.dump(1)
            server_class = self.server.__class__.__name__
            print >> sys.stderr, "Error: (500) %s does not implement: %s" % (server_class, calling_func_name)

        self.send_response(500)
        self.send_header('Content-Length','0')
        self.end_headers()
        if msg != None:
            print >> sys.stderr, "Not implemented: %s" % msg

    def headers_to_parameters(self, header_names):
        for n in header_names:
            if n not in self.headers:
                continue
            m = n.lower().replace('-','_')
            v = self.headers[n]
            try:
                v = int(v)
            except:
                pass
            self.parameters[m] = v 
        meta = {}
        for k,v in self.headers.items():
            if k.startswith('x-amz-meta-'):
                meta[k] =v
        if meta:
            self.parameters['x_amz_meta'] = meta

    def send_id_headers(self):
        # send amazon request ID headers
        id2, request_id = self.get_request_id()
        self.send_header('x-amz-id-2', id2)
        self.send_header('x-amz-request-id', request_id)


    # Useful utility methods

    def send_xml_response(self, x):
        # x is an racs.xmlutil.XMLEntity object
        # This sends the following headers, calls end_headers, and writes the given object to wfile
        #   Content-Length
        #   Content-Type
        #   Transfer-Encoding: identity 
        
        x = render_xml(x.xml(),include_header=True)

        self.send_header('Content-Type', 'application/xml')
        self.send_header('Transfer-Encoding','identity')
        self.send_header('Content-Length', len(x))
        self.end_headers()
        self.wfile.write(x)

    def send_html_response(self, x):
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.send_header('Transfer-Encoding','identity')
        self.send_header('Connection','close')
        self.send_header('Content-Length', len(x))
        self.end_headers()
        self.wfile.write(x)

    def read(self, content_length, content_encoding=None):
        # FIXME
        assert(content_length!=None)
        if content_encoding is None:
            val = self.rfile.read(content_length)
        else:
            self.handle_not_implemented("content encoding %s" % content_encoding)
            return
        return val 

    # ------ Methods to override --------------------------------------
    def handle_request_payment(self):
        self.handle_not_implemented()

    def handle_get_buckets(self):
        self.handle_not_implemented()

    def handle_get_bucket(self, prefix=None, marker=None, max_keys=None, delimiter=None):
        self.handle_not_implemented()    
    
    def handle_get_object(self, range=None, if_modified_since=None,
                          if_unmodified_since=None, if_match=None, if_none_match=None):
        self.handle_not_implemented()

    def handle_get_object_acl(self):
        self.handle_not_implemented()

    def handle_create_bucket(self):
        self.handle_not_implemented()

    def handle_put_object(self, cache_control=None, content_type=None, content_length=None, 
                          content_md5=None, content_disposition=None, content_encoding=None, 
                          expires=None, x_amz_acl=None, x_amz_meta=None):
        self.handle_not_implemented()

    def handle_head_object(self):
        self.handle_not_implemented()

    def handle_delete_object(self):
        self.handle_not_implemented()        

    def handle_delete_bucket(self):
        self.handle_not_implemented()

    def handle_copy_object(self, x_amz_copy_source, x_amz_metadata_directive=None, x_amz_copy_source_if_match=None, x_amz_copy_source_if_none_match=None, x_amz_copy_source_if_unmodified_since=None, x_amz_copy_source_if_modified_since=None):
        self.handle_not_implemented()

    def handle_get_bucket_location(self):
        self.handle_not_implemented()

    def get_request_id(self):
        # returns a two-element tuple (x-amz-id-2, x-amz-request-id)
        # These are ID numbers specific to the request, created for reference by the server
        # Amazon uses them for debuggin.  For our purposes, they're arbitrary
        # Amazon's headers seem to follow the convention
        #   x-amz-id-2: 64 characters of [0-9a-zA-Z]
        #   x-amz-request-id: 16 characters upper-case hexadecimal
        return ('0'*60+'beef','000000000000BEEF') 

    def version_string(self):
        return 'Python Fake AmazonS3 Server'

    def get_owner(self):
        self.handle_not_implemented()

    
