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

#!/usr/bin/python

import os, sys, re, user
from math import sqrt

from BaseHTTPServer import HTTPServer
from request_handler import RACSHTTPRequestHandler
import fec
from threading import Condition
from repositories import *
from racs.util.stats import *
from StringIO import StringIO

try:
    import zookeeper
except:
    pass

from racs.util import *

from racs.zk import ZK

class ConfigNotFound(Exception): pass

class RACSHTTPServer(HTTPServer):
    verbose = True

    def __init__(self, configfile):
        if not os.path.exists(configfile):
            raise ConfigNotFound(configfile,"Config file \"%s\" not found" % configfile)
        self.racs_metacache = {}
        self.stats = Stats()
        self.zk_host = 'localhost'
        self.zk_port = 2181
        self.zk_root_note = '/racs'
        self.zk_handle = None

        self.read_config(configfile)

        self.init_log()
        self.init_repositories()


        # Variables m and k are set according to the zfec package's conventions:
        # "The encoding is parameterized by two integers, k and m.  m is the total number
        #  of blocks produced, and k is how many of those blocks are necessary to
        #  reconstruct the original data.  m is required to be at least 1 and at most 256,
        #  and k is required to be at least 1 and at most m."
        if self.m is None:
            self.m = len(self.get_repositories())
        else:
            # de-activate some repositories
            reps = self.get_repositories()
            if len(reps) < self.m:
                print >> sys.stderr, "Not enough active repositories for m=%s" % self.m
                sys.exit(1)
            for r in reps[self.m:]:
                r.active = False

        if not self.unit_test_repositories:
            server_address = (self.host, self.port)

            msg = """RACS server at %s
Config file: %s
Repositories: %s
Tolerate up to %s failures
""" % (server_address, configfile, self.m, self.get_max_failures())

            if self.verbose:
                print msg
        
            print >> self.log, msg


        if self.unit_test_repositories:
            self.do_unit_tests()
            return

        #self.k = self.m - self.max_failures
 
        self.encode = fec.Encoder(self.k,self.m).encode
        self.decode = fec.Decoder(self.k,self.m).decode

        if self.use_zookeeper:
            self.zk = ZK(**self.zk_args)

        HTTPServer.__init__(self, server_address, RACSHTTPRequestHandler)
        import thread_manager
        thread_manager.thread_manager.start()

    def init_log(self):
        if self.logfile:
            self.log = LogStreamWrapper(FileAppendOutStream(self.logfile))
        else:
            self.log = NullOutStream()

    def get_max_failures(self):
        return self.m - self.k

    def get_repository(self, name):
        for r in self.repositories:
            if r.name == name:
                return r
        return None

    def toggle_repository_active(self, r):
        r.active = not r.active
        self.m = len(self.get_repositories())
        if self.m < self.k:
            print >> sys.stderr, "Warning: Too many inactive repositories, cannot reach k for any objects"
            print >> sys.stderr, "Warning: Too many inactive repositories, cannot reach k for any objects"
            print >> sys.stderr, "Warning: Too many inactive repositories, cannot reach k for any objects"
            print >> sys.stderr, "Warning: Too many inactive repositories, cannot reach k for any objects"

    def __str__(self):
        return "racs@%s:%s" % (self.host,self.port)

    def read_config(self, config_filename):
        self.zk_args = {'server':self}
        from ConfigParser import ConfigParser
        config = ConfigParser()
        successfully_read = config.read([config_filename])
        if len(successfully_read) == 0:
            print >> sys.stderr, "Error parsing config file", config_filename
            sys.exit(1)
        
        self.repositories = []
        RACS_read = False
        for section in config.sections():
            if section.startswith('Repository'):
                name = ' '.join(section.split()[1:])
                params = dict(config.items(section))
                cls = eval(params.pop('class'))
                try:
                    params['active'] = eval(params['active'])
                except:
                    pass
                self.repositories.append((cls,name,params))
            elif section == 'Zookeeper':
                for key, value in config.items(section):
                    #key = key
                    try:
                        value = int(value)
                    except:
                        pass
                    #setattr(self, key, value)
                    self.zk_args[key] = value
            elif section == 'RACS':
                RACS_read = True
                params = dict(config.items(section))                
                
                # Required keys for config file RACS section
#                required = set(['racs_access_key_id', 'racs_secret_access_key', 'k', 'host','port'])
                required = set(['k', 'host','port'])
                # Optional parameters with default values
                # (not passed through input xform functions)
                optional_parameters = dict(
                    minimize_latency_or_bandwidth = 'latency',
                    verify_listings_consistent = False,
                    proxy_host = None,
                    proxy_port = None,
                    m = None,
                    logfile = os.path.abspath,
                    unit_test_repositories = False,
                    use_zookeeper = False,
                    record_stats = False,
                )

                optional_set = set(optional_parameters.keys())
                for k,v in optional_parameters.items():
                    setattr(self, k, v)

                # Optional functions to transform input parameter strings
                xform = dict(
                    m = int,
                    k =  int, 
                    port = int, 
                    verify_listings_consistent = eval,
                    )

                identity = lambda x: x
                
                for k,v in params.items():
                    if k in required or k in optional_set:
                        setattr(self, k, xform.get(k,identity)(v))
                    else:
                        print >> sys.stderr, "Unrecognized RACS config key: %s" % k
                        sys.exit(1)
                
                if not self.unit_test_repositories:
                    for attr in required:
                        try:
                            getattr(self,attr)
                        except:
                            print >> sys.stderr, "Missing required RACS config key: %s" % attr
                            sys.exit(1) 
            else:
                print >> sys.stderr, "Unrecognized RACS section: %s" % section
                sys.exit(1)

        if not RACS_read:
            print >> sys.stderr, "Config file is missing RACS section"
            sys.exit(1)


    def init_repositories(self):
        self.repositories = [cls(self, name,**params) for cls, name, params in self.repositories]
        n = len(self.get_repositories())

        if n == 0:
            print >> sys.stderr, "Too few repositories specified in config file"
            sys.exit(1)                

        maxtol = n-1
        
        if self.unit_test_repositories:
            self.k = self.m

        if self.k > maxtol:
            print >> sys.stderr, "max_failures can be at most %s with %s repositories" % (maxtol,n)
            sys.exit(1)


    def choose_repositories(self, bucket, key, available_repositories = None, k=None):
        # return a list of repositories that should be tried to get k shares
        # may return more than k repositories
        if k is None:
            k = self.k
        if available_repositories is None:
            available_repositories = self.get_repositories()

        available_repositories = sorted(available_repositories, key = lambda r: r.priority)
        if self.minimize_latency_or_bandwidth == 'latency':
            return available_repositories  # try them all!
        else:
            # FIXME not very sophisticated right now...
            return available_repositories[:k]
        
    def redundant_repositories(self, spent_repositories, available_repositories=None):
        if available_repositories is None:
            available_repositories = self.get_repositories()
        return list(set(available_repositories).difference(set(spent_repositories)))


    # FIXME  --- implement

    def zk_acquire_read(self, bucket, key):
        pass

    def zk_release_read(self, lock_token):
        pass

    def zk_acquire_write(self, bucket, key):
        return None # returns lock token

    def zk_release_write(self, lock_token):
        pass

    def get_repositories(self, only_active=True):
        if only_active:
            return [r for r in self.repositories if r.active]
        else:
            return self.repositories
        
    def do_unit_tests(self):
        print "Unit test repositories..."
        for r in self.get_repositories():
            r.unit_test()

