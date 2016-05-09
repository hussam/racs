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

import sys
from racs.exceptions import *
from racs.s3_entities import *

class Repository(object):
    @classmethod
    def register(cls, name):
        Repository.all_repositories.append(name)

    all_repositories = []
    def __init__(self, server, name, active=True):
        self.name = name
        self.server = server
        self.priority = 5
        self.active = active

    # Interface methods: Override these to implement a new repository
    #--------------------------------------------------------------------


    def create_bucket(self, bucket):
        # Raises an exception only if bucket creation failed.
        # If bucket already exists, create has no effect but succeeds
        self.method_not_implemented("create_bucket")

    def delete_bucket(self, bucket):
        # If bucket does not exists: raise NoSuchBucket 
        # If bucket is not empty: raises BucketNotEmpty
        self.method_not_implemented("delete_bucket")

    def put_object(self, bucket, key, data, content_type=None, headers={}):
        # meta headers = http headers dictionary. Note: x-amz-meta- should NOT be supplied. 
        #
        # Overwrites objects if they already exist.
        # Throws NoSuchBucket if bucket does not exist
        
        self.method_not_implemented("put_object")

    def get_object(self, bucket, key):
        # Returns a triple (data, content-type, metadata dict)
        # Throws NotFound if bucket/path does not exist
        self.method_not_implemented("get_object")

    def head(self, bucket, key):
        # returns headers dict with Content-Type, Etag, Content-Length, and meta-info headers
        # raises NotFound if object doesn't exist
        # raises NoSuchBucket if bucket doesn't exist
        self.method_not_implemented("head")

    def delete_object(self, bucket, key):
        # Succeeds if object never existed in the first place
        # Raises NoSuchBucket if bucket does not exist
        self.method_not_implemented("delete_object")

    def get_bucket_contents(self, bucket, prefix=None, marker=None, delimiter=None, max_keys=None):
        # returns an iterable of s3_entries.ObjectMetaData instances.  (No Prefix objects!)
        # prefix, marker, and delimiter correspond to Amazon's meanings
        # Raises NoSuchBucket if buckets do not exist
        #
        # prefix: Limits the response to keys that begin with a certain prefix
        # delimiter: Used to group keys. See http://docs.amazonwebservices.com/AmazonS3/2006-03-01/API/
        # marker: key-marker in amazon docs. Specifies key to start with when listing.  
        # max_keys: Currently not used by RACS. Does not need to be implemented... yet.
        self.method_not_implemented("get_bucket_contents")

    def get_all_buckets(self):
        # Returns a list of available bucket names
        self.method_not_implemented("get_all_buckets")

    # ------ unit test overridable methods ------------------------------
    def initialize_tests(self):
        # For unit tests; be sure to call super if you override this
        self.test_bucket = "racs_unittest_bucket"

    def cleanup_tests(self):
        # For unit tests; be sure to call super if you override this
        del self.test_status
        del self.test_bucket

    #--------------------------------------------------------------------
    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)

    def increase_priority(self):
        self.priority += 1

    def decrease_priority(self):
        self.priority -= 1

    def method_not_implemented(self,msg=None):
        print >> sys.stderr, "In repository %s method not implemented: %s" % (self.name , msg)

    def unit_test(self):
        print "Unit test repository %s..." % repr(self)
        
        test_methods = sorted([x for x in dir(self) if x.startswith('test')])

        try:
            self.initialize_tests()
        except:
            pass
        
        self.test_status = dict([(m,None) for m in test_methods])

        abort_on_fail = True

        for m in test_methods:
            self.test_status[m] = self.run_test(m)
            if (not self.test_status[m]) and abort_on_fail:
                print >> sys.stderr, "Abort on Fail"
                sys.exit(1)

        try:
            self.cleanup_tests()
        except:
            pass

    def run_test(self, testname):
        func = getattr(self,testname)
        print '-'*80
        print ("    %s..." % testname),
        sys.stdout.flush()

        try:
            v = func()

            if v is None:
                v = True
            if v is False:
                print "FAIL"
            else:
                print "OK"
            return v
        except KeyboardInterrupt, e:
            print "INTERRUPTED"
            import traceback
            traceback.print_exc()
            return False
        except Exception, e:
            print "FAIL"
            import traceback
            traceback.print_exc()
            return False

    def test_00_check_clean(self):
        self.pretest_buckets = set(self.get_all_buckets())
        
        if self.test_bucket in self.pretest_buckets:
            # attempt to clean
            for obj in self.get_bucket_contents(self.test_bucket):
#                prefixes = [x for x in obj if isinstance(x, Prefix)]
                if isinstance(obj,ObjectMetaData):
                    print "-- cleanup: delete %s" % obj.key
                    self.delete_object(self.test_bucket,obj.key)
            self.delete_bucket(self.test_bucket)
            self.pretest_buckets = set(self.get_all_buckets())

    def test_01a_list_buckets(self):
        self.pretest_buckets = set(self.get_all_buckets())
        if self.test_bucket in self.pretest_buckets:
            print >> sys.stderr, "Warning: Previous test did not clean up correctly: %s bucket present.  Cleanup failed." % self.test_bucket
            sys.exit(1)

    def test_01b_create_bucket(self):
        self.create_bucket(self.test_bucket)

    def test_01c_list_buckets(self):
        self.posttest_buckets = set(self.get_all_buckets())

    def test_01d_check_created_bucket(self):
        # if this fails, then the bucket was not created correctly
        dif = self.posttest_buckets.difference(self.pretest_buckets)
        if dif != set([self.test_bucket]):
            print "dif = %s" % dif
            raise Exception("Error verifying created bucket")

    def test_01e_create_same_bucket_again(self):
        # should quietly succeed
        self.create_bucket(self.test_bucket)

    def test_02a_delete_nonexistent_bucket_raises_nosuchbucket(self):
        # should raise NoSuchBucket
        try:
            self.delete_bucket("does_not_exist")
            raise Exception
        except NoSuchBucket:
            pass

    def test_02b_delete_object_nonexistent_bucket(self):
        # should raise NoSuchBucket
        try:
            self.delete_object("non_existent_bucket","foo")
            raise Exception
        except NoSuchBucket:
            pass

    def test_02c_delete_nonexistent_object(self):
        # key doesn't exists, but should work without raising exception
        self.delete_object(self.test_bucket, "non-existent-key")

    def test_03_put_object_to_nonexistent_bucket(self):
        # expected to raise nosuchbucket
        try:
            self.put_object("non_existent_bucket", "key", "test data")
            raise Exception
        except NoSuchBucket:
            pass

    def test_04a_put_object_small(self):
        self.test_key_small = "test_key_small"
        self.test_data_small = """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent ultrices suscipit lorem nec suscipit. Aliquam sit amet sapien ipsum, quis volutpat ligula. Maecenas nec convallis diam. Nunc in enim non neque euismod tempus. Cras interdum vehicula blandit. Nunc in leo non nisi congue vestibulum. Aliquam ac tellus ac arcu malesuada convallis. Ut non velit ligula, fermentum ornare nunc. Fusce nec risus sed erat tincidunt laoreet. Aenean consectetur porta neque, eget interdum sem congue id. Nunc mattis tortor eget augue pulvinar molestie vehicula magna consequat. Aenean arcu eros, faucibus id pretium a, euismod et augue. Vivamus vitae est enim, quis porttitor eros."""
        self.put_object(self.test_bucket, self.test_key_small, self.test_data_small)

    def test_04b_get_object_small(self):
        self.test_data_return_small, content_type, headers = self.get_object(self.test_bucket, self.test_key_small)
        
    def test_04b_head_object_small(self):
        # FIXME should test to make sure custom metadata is returned
        # FIXME should also test that head() raises NotFound for missing objects
        # FIXME and NoSuchBucket for non-existent buckets
        headers = self.head(self.test_bucket, self.test_key_small)
        expected_headers = set([
            'Etag', 'Content-Length','Last-Modified','Content-Type'
            ])
        d = expected_headers.difference(set(headers.keys()))
        if len(d) > 0:
            raise Exception("Missing expectead headers %s" % ','.join(list(d)))
        et = headers['Etag']
        if not et.startswith('"'):
            raise Exception("Etag returned from head() should be in quotes")

    def test_04c_get_object_small_verify(self):
        if self.test_data_return_small != self.test_data_small:
            raise Exception

    def test_04d_delete_non_empty_bucket(self):
        try:
            self.delete_bucket(self.test_bucket)
            raise Exception
        except BucketNotEmpty:
            pass

    def test_04e_delete_object_small(self):
        self.delete_object(self.test_bucket, self.test_key_small)

    def test_04f_delete_object_small_verify(self):
        object_metadatas = self.get_bucket_contents(self.test_bucket)
        for obj in object_metadatas:
            if obj.key == self.test_key_small:
                # Object seems to not have been deleted
                raise Exception
        del self.test_data_return_small
        del self.test_data_small
        del self.test_key_small

    def test_06a_put_object_big(self):
        # One megabyte object upload
        self.test_key_big = "test_key_big"
        import random, struct
        bytes = 1024*1024
        n = bytes/8
        # Generate lots of random junk
        self.test_data_big = struct.pack('d'*n,*([random.random() for i in xrange(n)]))
        self.put_object(self.test_bucket, self.test_key_big, self.test_data_big)

    def test_06b_get_object_big(self):
        self.test_data_return_big, content_type, headers = self.get_object(self.test_bucket, self.test_key_big)
        
    def test_06c_get_object_big_verify(self):
        if self.test_data_return_big != self.test_data_big:
            raise Exception

    def test_06d_delete_object_big(self):
        self.delete_object(self.test_bucket, self.test_key_big)
        del self.test_data_return_big
        del self.test_data_big
        del self.test_key_big

    def test_05a_put_object_small_headers(self):
        self.test_key_small = "test_key_small_headers"
        self.test_data_small = """Skub ipsum dolor sit amet, consectetur adipiscing elit. Praesent ultrices suscipit lorem nec suscipit. Aliquam sit amet sapien ipsum, quis volutpat ligula. Maecenas nec convallis diam. Nunc in enim non neque euismod tempus. Cras interdum vehicula blandit. Nunc in leo non nisi congue vestibulum. Aliquam ac tellus ac arcu malesuada convallis. Ut non velit ligula, fermentum ornare nunc. Fusce nec risus sed erat tincidunt laoreet. Aenean consectetur porta neque, eget interdum sem congue id. Nunc mattis tortor eget augue pulvinar molestie vehicula magna consequat. Aenean arcu eros, faucibus id pretium a, euismod et augue. Vivamus vitae est enim, quis porttitor eros."""
        self.test_headers = {
            'bar': 'test bar value',
            'foo': 'test foo value',
            }
        self.test_content_type = 'app/x-racs-test'
        self.put_object(self.test_bucket, 
                        self.test_key_small, 
                        self.test_data_small, 
                        content_type = self.test_content_type,
                        headers = self.test_headers)

    def test_05b_get_object_small_headers(self):
        self.test_data_return_small, self.return_content_type, self.return_headers = self.get_object(self.test_bucket, self.test_key_small)
        
    def test_05c_get_object_small_headers_verify(self):
        if self.test_data_return_small != self.test_data_small:
            raise Exception

    def test_05d_get_object_small_headers_verify_contenttype(self):
        if self.test_content_type != self.return_content_type:
            print >> sys.stderr, "content type does not match"
            print >> sys.stderr, "Expected: %s" % self.test_content_type
            print >> sys.stderr, "Received: %s" % self.return_content_type
            raise Exception

    def test_05e_get_object_small_headers_verify_headers(self):
        if self.return_headers != self.test_headers:
            def p(h):
                for k,v in sorted(h.items()):
                    print >> sys.stderr, "%-15s = %s" % (k,v)
            print >> sys.stderr, "Cannot verify headers.  Expected:"
            p(self.test_headers)
            print >> sys.stderr, "Received:"
            p(self.return_headers)
            raise Exception

    def test_05f_cleanup_small_headers_test(self):
        self.delete_object(self.test_bucket, self.test_key_small)
        del self.test_data_return_small
        del self.test_data_small
        del self.test_key_small
        del self.return_content_type
        del self.return_headers
        
    def test_07a_test_prefix_list_upload_keys(self):
        self.test_keys = ['fookey1','fookey2','fookey3','nonfoo1','nonfoo2']
        for k in self.test_keys:
            self.put_object(self.test_bucket, k, 'Lorem ipsum blah blah blah')
    
    def test_07b_get_prefix_list(self):
        self.return_contents = self.get_bucket_contents(self.test_bucket, prefix="foo")
        
    def test_07c_prefix_object_not_returned(self):
        prefixes = [x for x in self.return_contents if isinstance(x,Prefix)]
#        def f():
#            print >> sys.stderr, "Return contents:"
#            for o in self.return_contents:
#                print >> sys.stderr, repr(o)

        if len(prefixes) > 0:
            raise Exception # Shouldn't return prefixes with current repository semantics

#        if len(prefixes) == 0:
#            print >> sys.stderr, "No prefixes returned! Expected one named \"foo\""
#            f()
#            raise Exception
#        elif len(prefixes) > 1:
#            print >> sys.stderr, "Multiple prefixes returned! Expected exactly one named \"foo\""
#            f()
#            raise Exception
#        elif prefixes[0].name != "foo":
#            print >> sys.stderr, "One prefix returned, but it is not named foo!"
#            f()
#            raise Exception

    def test_07d_prefix_correct_objects_returned(self):
        objs = [x for x in self.return_contents if not isinstance(x,Prefix)]
        rkeys = [o.key for o in objs]
        # Should return exactly the keyset starting with "foo"
        correct_keys = [key for key in self.test_keys if key.startswith('foo')]
        if set(rkeys) != set(correct_keys):
            raise Exception
        
    def test_07e_cleanup_prefix_test(self):
        for k in self.test_keys:
            self.delete_object(self.test_bucket, k)
        del self.test_keys
        try:
            del self.return_contents
        except:
            pass

    def test_90a_delete_bucket(self):
        self.delete_bucket(self.test_bucket)
    
    def test_90b_list_buckets(self):
        self.post_delete_buckets = set(self.get_all_buckets())

    def test_90c_check_buckets(self):
        if self.post_delete_buckets != self.pretest_buckets:
            raise Exception
        del self.post_delete_buckets
        del self.pretest_buckets
        del self.posttest_buckets
        
    

