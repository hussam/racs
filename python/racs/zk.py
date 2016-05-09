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

#!/usr/bin/env python
import sys


# Reader/writer lock at (bucket+key) granularity

from threading import *
try:
    import zookeeper as z
except:
    print "Warning: Can't import zookeeper module, not using Z"

# z interface
ZOO_OPEN_ACL_UNSAFE = {"unsafes":0x1f, "scheme":"world", "id" :"anyone"};
unsafe = [ZOO_OPEN_ACL_UNSAFE]

def wtf(m):
    def wtf(*args):
        print "WTF %s" % m,args
    return wtf


# basic global lock, based on
#   http://hadoop.apache.org/z/docs/r3.1.2/recipes.html#sc_recipes_Locks


# z.create(handle, node, data, unsafeissions, flags)
#
# z.get_children(handle,node,watcher)
#    --> callback (int, int, int, node)   (node is parent node, not newly created one)
#    must reset watcher
# 
# z.set(handle,node, data, version)
#
# z.get(handle, node, watcher)
#
# returns something like this:
#    ('data', 
#      {
#    'pzxid': 45L, 
#    'ctime': 1268945828183L, 
#    'aversion': 0, 
#    'mzxid': 34L, 
#    'numChildren': 7, 
#    'ephemeralOwner': 0L, 
#    'version': 1, 
#    'dataLength': 7, 
#    'mtime': 1268945962762L, 
#    'cversion': 7, 
#    'czxid': 32L
#      })
import random

class ZK(object):
    def __init__(self, server, root_node, host='localhost', port=2181):
        self.root = root_node
        self.host = host
        self.port = port
        self.counter = random.randint(0,2**30)
        self.server = server
        self.zcv = Condition() 

        def watcher(handle,type,state,path):
            print "Z connected (%s:%s)" % (self.host, self.port)
            self.zcv.acquire()
            self.connected = True
            self.zcv.notify()
            self.zcv.release()

        self.zcv.acquire()
        self.connected = False

        self.handle = z.init("%s:%s"%(self.host,self.port), watcher, 10000, 0)
        self.zcv.wait(10.0)

        if not self.connected:
            print "Connection to Z cluster timed out - is a server running on %s:%s?" % (self.host,self.port)
            self.connected = False
            self.zcv.release()
            return
        self.zcv.release()

        # make sure root node exists
        try:
            self.create(self.root,"RACS root node", [ZOO_OPEN_ACL_UNSAFE])
        except IOError, e:
            if e.args[0] != z.zerror(z.NODEEXISTS):
                raise e

    def request_write_lock(self, bucket, key):
        # block until it is safe to write to bucket/key
        # return a function that indicates we have finished
        print "acquire write lock %s/%s" % (bucket,key)
        try:
            return self._request_lock("write", bucket,key)
        except Exception, e:
            import traceback
            traceback.print_exc()

    def request_read_lock(self, bucket, key):
        print "acquire read lock %s/%s" % (bucket,key)
        try:
            return self._request_lock("read",bucket,key)
        except Exception, e:
            import traceback
            traceback.print_exc()


# 1. Call create( ) with a pathname of "_locknode_/lock-" and the sequence and ephemeral flags set.
# 2. Call getChildren( ) on the lock node without setting the watch flag (this is important to avoid the herd effect).
# 3. If the pathname created in step 1 has the lowest sequence number suffix, the client has the lock and the client exits the protocol.
# 4. The client calls exists( ) with the watch flag set on the path in the lock directory with the next lowest sequence number.
# 5. if exists( ) returns false, go to step 2. Otherwise, wait for a notification for the pathname from the previous step before going to step 2.
    def __str__(self):
        # my id
        return str(self.server)+":"+str(id(self))

    def _request_lock(self, ltype, bucket, key, request_uid=None):
        locknode = self.root + "/" + ("%s:%s" % (bucket, key)).replace("/","SLASH")
#        locknode = self.root + "/global_lock"
        try:
            self.create(locknode, str(self), unsafe)
        except IOError, e:
            # no big deal if node already exists
            if e.args[0] != "node exists":
                raise e

        lockbase = "%s/%s-" % (locknode,ltype)
        if request_uid is None:
            request_uid = self.counter 
            self.counter += 1
        childdata = str(self) + "-"+ str(request_uid)
        self.create(lockbase, childdata, unsafe, z.EPHEMERAL | z.SEQUENCE)
        print "--> created lock node %s" % locknode
        
        try:
            children = self.get_children(locknode)
        except TypeError: # "an integer is required".. no idea why this happens
            # But it works if tried again
            children = self.get_children(locknode)

        children.sort(key = lambda x: int(x.split('-')[1]))
            
        print "--> lock node children %s" % children
        
        # Which child did I create?
        i = len(children)-1
        for i in range(len(children)-1,-1,-1):
            child = children[i]
            if not child.split("/")[-1].startswith(ltype):
                continue
            data, meta = self.get(child)
            if data == childdata:
                break
        else:
            raise Exception("Can't find my child %s in locknode %s" % (self,locknode))

        print "   --> created %s" % child
        return self._check_request(child, ltype, locknode,children)

    def _check_request(self, child, ltype, locknode, children=None):
        if children is None:
            try:
                children = self.get_children(locknode)
            except TypeError: # "an integer is required".. no idea why this happens
                # But it works if tried again
                children = self.get_children(locknode)
            children.sort(key = lambda x: int(x.split('-')[1]))            

        if ltype == 'write':
            proceed = (children[0] == child)
            try:
                waitfor = children[children.index(child)-1]
            except:
                pass
        elif ltype == 'read':
            # proceed as long as no writes are ahead of us
            proceed = True
            for c in children:
                if c == child:
                    break
                if c.split("/")[-1].startswith('write'):
                    proceed = False
                    waitfor = c  # wait for /last/ read before us
            
        if proceed:
            print "%s... I own lock %s" % (self,child)
            print "%s... acquired lock! " % self
            return lambda: self._release_lock(child,locknode)
        else:
            try:
                holder,m = self.get(waitfor) # not actually necessary 
                print "%s... I own lock %s"% (self,child)
                print "%s... waiting for lock %s held by %s" % (self,waitfor,holder)
            except Exception, e: 
                import traceback
                traceback.print_exc()

            cond = Condition()
            cond.acquire()
            try:
                self.get(waitfor,lambda *args: self._my_turn(child,cond))
            except IOError, e:
                if e.args[0] == 'no node':
                    # our view is out of data already.. try again
                    cond.release()
                    return self._check_request(child, ltype, locknode)
                else:
                    raise e
            cond.wait(60.0)
            cond.release()
            return self._check_request(child, ltype, locknode)
            #return lambda: self._release_lock(child,locknode)

    def _release_lock(self, child, locknode):
        self.delete(child)
        try:
            self.delete(locknode)
        except IOError, e:
            if e.message != "not empty":
                raise e

    def _my_turn(self, child, condition):
        condition.acquire()
        condition.notify()
        condition.release()

    # ---- zookeeper convenience functions
    def get(self, node, watcher=None):
        assert (watcher is None or callable(watcher))
        return z.get(self.handle,node,watcher)

    def create(self, node, data, perms=unsafe, flags=0):
        assert isinstance(flags,int)
        return z.create(self.handle, node, data, perms, flags)

    def get_children(self, node, watcher=None):
        assert (watcher is None or callable(watcher))
        chils = z.get_children(self.handle, node, watcher)
        return ["%s/%s" % (node,x) for x in chils]

    def set(self, node, data, version):
        assert isinstance(version,int)
        return z.set(self.handle, node, data, version)

    def delete(self, node, version=0):
        assert isinstance(version,int)
        return z.delete(self.handle, node, version)

#---- testing/debugging ------------
if __name__ == '__main__':
    try:
        command, bucket, key = sys.argv[1:]
    except:
        command = sys.argv[1]
        if command != "clean":
            raise Exception

    root = "/test9"
    k = ZK("serv",root)

    if command == 'clean':
        for child in k.get_children(root):
            for ephem in k.get_children(child):
                k.delete(ephem)
            k.delete(child)
        sys.exit(0)
    elif command == 'write':
        finish = k.request_write_lock(bucket,key)
    elif command == "read":
        finish = k.request_read_lock(bucket,key)
    else:
        raise Exception
        
    print " --------------> %s !!!" % command
        
    try:
        input("pause")
    except:
        pass

    finish()
    
