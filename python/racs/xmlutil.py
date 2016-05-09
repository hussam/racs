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

from lxml import etree
import time, os, sys, re

class XMLEntity(object):
    def xml(self):
        # returns an xml representation of object
        raise NotImplementedError

    _strcache = None
    def __str__(self):
        if not self._strcache:
            self._strcache = render_xml(self.xml(),include_header=True)
        return self._strcache
        
    def __len__(self):
        return len(str(self))

class XMLEntityList(object):
    def xml_list(self):
        raise NotImplementedError

def render_xml(element, include_header=True):
    s = etree.tostring(element)
    if include_header:
         return '<?xml version="1.0" encoding="UTF-8" ?>\n' + s
    else:
        return s

def SubElement(*args, **keywords):
    return _element_helper(2, etree.SubElement, args, keywords)

def Element(*args, **keywords):
    return _element_helper(1, etree.Element, args, keywords)

def _element_helper(n_expected_args, func, args, keywords):
    
    if len(args) > n_expected_args:
        text = args[n_expected_args]
        args = args[:n_expected_args] + args[n_expected_args+1:]
    else:
        text = None

    try:
        children = keywords.pop('children')
    except:
        children = []
    try:
        extra_elements = keywords.pop('extra_elements')
    except:
        extra_elements = {}

    el = func(*args, **keywords)
    
    for k,v in extra_elements.items():
        el.set(k,v)

    if text != None:
        el.text = str(text)

    for c in _expand_children(children):
        el.append(c)
    return el


def _expand_children(seq):
    for c in seq:
        if isinstance(c, XMLEntity):
            yield c.xml()
        elif isinstance(c, XMLEntityList):
            for e in _expand_children(c.xml_list()):
                yield e
        else:
            yield c
