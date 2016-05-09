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

from racs.util.tracer import *
from StringIO import StringIO
import re, sys, os
import time
from math import *

class record_event(Tracer):
    def __init__(self, event_name, value=1, etype="counter"):
        self.event_name = event_name
        self.value = value
        self.etype = etype

    def __repr__(self):
        return "<record %s>" % self.event_name

    def accept_args(self, *objs):
        for o in objs:
            try:
                self.stats = o.server.stats
                return True
            except:
                pass

    def accept(self, frame):
        try:
            if self.accept_args(frame.f_locals['self']):
                return True
        except KeyError:
            return False
        
    def apply(self):
        self.stats.record(self.event_name, self.value, self.etype)

class record_event_decorator(object):
    def __init__(self, prefix):
        self.prefix = prefix
    def __call__(self, f):
        return record_event("%s:%s" % (self.prefix,f.__name__))(f)

class record_operation(record_event):
    def __init__(self, opname, **keywords):
        record_event.__init__(self, opname, keywords, "list")
        self()

class Stopwatch:
    def __init__(self):
        self.start = time.time()
    def __call__(self):
        return time.time() - self.start

class Stats:
    def __init__(self):
        self.reset()

    def record(self, event, value=1, etype="counter"):
        if etype == "counter":
            if event not in self.operations:
                self.operations[event] = value
            else:
                self.operations[event] += value
        elif etype == "list":
            if event not in self.lists:
                self.lists[event] = [value]
            else:
                self.lists[event].append(value)
    
    def reset(self):
        self.lists = {}
        self.operations = {}
        
    def dump(self):
        f = StringIO()
        suppl = '/tmp/racs.data'
        sup = open(suppl,'w')
        print >> f, "Operations"
        for o,v in sorted(self.operations.items()):
            print >> f, "   %-35s %s" % (o,v)
            #print >> sup, "%s:%s\n" % (o,v)

        series = {}
        print >> f, "Data"
        for o,v in sorted(self.lists.items()):
            print >> f, "    %s" % o
            print >> sup,o
            self.dump_list(o, v, f, sup)
            print >> sup, ""
            try:
                sr = []
                for x in v:
                    sr.append( (x['bytes'],x['elapsed']))
                series[o] = sr
            except:
                pass

        import pickle
        p = open(suppl+'.p','w')
        pickle.dump( (series,self.lists), p)
        p.close()
        print >> f, "Supplementary data written to %s" % suppl


        sup.close()
        f.seek(0)
        dmp = f.read()

        f = open(suppl,'r')
        d = f.read()
        f.close()
        f = open(suppl,'w')
        f.write(dmp)
        f.write(d)
        f.close()

        return dmp

    def dump_list(self, name, events, out, sup):
        if len(events) == 0:
            print >> out,"        <empty>"
            return
        aggregators = {}

#        if 'bytes' in events[0] and 'elapsed' in events[0]:
            
        for key, value in events[0].items():
            aggregators[key] = NumericalAggregator()

        for event in events:
            for key, value in event.iteritems():
                try:
                    aggregators[key].add(value)
                except:
                    print "missing %s?" % key

        for key,ag in sorted(aggregators.items()):
            print >> out, "%30s %-10s   %s" % ("","[%s]"%key,ag)
            print >> sup, "%s:" % key,
            ag.dump(sup)

class NumericalAggregator:
    def __init__(self):
        self.avg = None
        self.n = 0
        self.max = None
        self.min = None
        self.total = 0
        self.data = []
    
    def dump(self, sup):
        print >> sup, ",".join(map(str,self.data))

    def add(self, x):
        self.data.append(x)
        if self.n == 0:
            self.avg = x
            self.stdev = 0
            self.n += 1
            self.max = x
            self.min = x
            self.total += x
            return
        self.n += 1
        self.total += x
        self.avg = ((self.avg * self.n) + x) / float(self.n+1)
        self.max = max(self.max,x)
        self.min = min(self.min,x)

    def compute_stdev(self):
        variance = sum([(self.avg - x)**2 for x in self.data]) / float(self.n)
        self.stdev = sqrt(variance)

    def __str__(self):
        self.compute_stdev()
        fields = ['n','min','max','avg','stdev']
        def fmt(x):
            if isinstance(x, int):
                return str(x)
            elif isinstance(x, float):
                return "%0.5f" % x
            else: 
                raise Exception
        
        def get(f):
            return "%s:%s" % (f,fmt(getattr(self,f)))

        return ' '.join(map(get,fields))

            
