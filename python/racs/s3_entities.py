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

# Note: Ubuntu installs elementtree in a weird place,
# /usr/share/python-support/python-elementtree
# ... this doesn't get put into the pythonpath automatically 

from xmlutil import *
from util import *
from lxml import objectify
import time

S3XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

def format_timestamp(t):
    # Example format u'2010-03-16T21:19:31.000Z'
    if isinstance(t, int) or isinstance(t, float):
        t = time.gmtime(t)
    if isinstance(t,time.struct_time):
        fmt = "%Y-%m-%dT%H:%M:%S.000Z"
        return time.strftime(fmt,t)
    else:
        t = str(t)
        if not t.endswith('Z'):
            try:
                t = t.split('.')[0]+ ".000Z"
            except:
                pass
        return t

class AccessControlPolicy(XMLEntity):
    def __init__(self):
        # PLACEHOLDER
        pass

    def xml(self):
        # can't figure out how to get lxml.etree to accept attributes with xml namespace prefixes, have to parse a string with objectify as a workaround...
        grantee =  objectify.fromstring('<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser" />')
        grantee.append(Element('ID','0001'))
        grantee.append(Element('DisplayName','placeholder'))

        return Element("AccessControlPolicy", 
                       xmlns=S3XMLNS,
                       children = [
                User('0001','placeholder'),
                Element('AccessControlList',
                        children = [
                        Element('Grant',
                                children = [
                                grantee,
                                Element('Permission','FULL_CONTROL'),
                                ])
                        ])
                ])
        
class User(XMLEntity):
    def __init__(self, id, display_name):
        self.id = id
        self.display_name = display_name
    def xml(self):
        return Element("Owner", children = [
                Element("ID",self.id),
                Element('DisplayName',self.display_name)
        ])

nobody = User(1,"not_implemented")

class Bucket(XMLEntity):
    def __init__(self, name, creation_date=None):
        self.name = name
        if creation_date == None:
            creation_date = time.gmtime()
        self.creation_date = creation_date

    def xml(self):
        return Element('Bucket', children = [
                Element('Name',self.name),
                Element('CreationDate',format_timestamp(self.creation_date)),
                ])


class ObjectMetaData(XMLEntityList):
    def __init__(self, key, last_modified, etag, size, owner=None, storage_class ='STANDARD', metadata=None):
        last_modified = format_timestamp(last_modified)
        if not last_modified.endswith('Z'):
            raise Exception("Bad time format %s" % last_modified)

        self.key = key
        self.last_modified = last_modified
        self.etag = etag
        self.size = size
        if owner is None:
            owner = nobody
        self.owner = owner # User XMLEntity instance
        self.storage_class = storage_class
        self.metadata = metadata

    def xml_list(self):
        et = repr(str(self.etag))
        return [
            Element("Key",self.key),
            Element("LastModified",format_amz_date(self.last_modified)),
            Element("ETag",et),
            Element("Size",str(self.size)),
            self.owner,
            Element("StorageClass",self.storage_class),
            ]

    def __repr__(self):
        return '<%s key=%s>' % (self.__class__.__name__, self.key)
        
class ListAllMyBucketsResult(XMLEntity):
    def __init__(self, owner, buckets):
        # buckets: a list of Bucket instances
        # owner: an Owner instance
        self.owner = owner
        self.buckets = buckets
        
    def __str__(self):
        return self.pretty_print()

    def pretty_print(self):
        return etree.tostring(self.xml(),pretty_print=True)

    def xml(self):
        return Element('ListAllMyBucketsResult',
                       xmlns=S3XMLNS,
                       children = [
                self.owner,
                Element("Buckets",
                        children=self.buckets)])
 
class Prefix(XMLEntity):
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name
    def xml(self):
        return Element('Prefix',self.name)
    def __repr__(self):
        return '<%s name=%s bucket=%s>' % (self.__class__.__name__, self.name,self.bucket)

NullPrefix = Prefix(None,'')

class ListBucketResult(XMLEntity):
    def __init__(self, name, prefix=NullPrefix, contents=[], common_prefixes=[], is_truncated='false', max_keys=None, delimiter="", marker=""):
        self.name = name
        self.prefix = prefix
        self.contents = contents
        self.common_prefixes = common_prefixes
        self.is_truncated = is_truncated
        self.max_keys = max_keys
        self.delimiter = delimiter
        self.marker = marker
    
    def xml(self):
        children = [
            Element("Name",self.name),
            Element("Prefix",self.prefix),
#            Element("Contents",children=self.contents),
            Element("IsTruncated",self.is_truncated),
            Element("MaxKeys",self.max_keys),
            Element("Delimiter",self.delimiter),

            ]
        
        for p in self.common_prefixes:
            children.append(Element("CommonPrefixes",children = [p]))
                        #Element("Prefix",p)
                        #]))

        for meta in self.contents:
            children.append(Element("Contents",children=[meta]))

        return Element('ListBucketResult',
                       xmlns = S3XMLNS,
                       children = children)

