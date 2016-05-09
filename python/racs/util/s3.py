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

import re
from itertools import groupby

def select_keys(keys, prefix=None, marker=None, delimiter=None, max_keys=None):
    # applies prefix, marker, delimiter, max_keys to a list of keys
    # returns (keys, common_prefixes)
    
    # 1. downselect to only keys that share prefix. Also remove prefix, to 
    # be added later
    if prefix:
        keys = [k[len(prefix):] for k in keys if k.startswith(prefix)]

    if delimiter:
        # FIXME delimiter should be quoted to avoid regex characters
        # (not a priority to fix; in practice, only / gets used as a delimiter)
        r = re.compile(r"^(.*?)%s" % delimiter)
        def keyfunc(x):
            m = r.match(x)
            if not m:
                return None
            else:
                return m.group(1)
        
        entries = []
        cprefixes = set()
        for cpfx,items in groupby(keys,keyfunc):
            if cpfx is None:
                entries.extend(list(items))
            else:
                cprefixes.add(cpfx)
        cprefixes = list(cprefixes)
    else:
        entries = keys
        cprefixes = []

    if prefix:
        entries = [prefix+k for k in entries]

    entries.sort()

    if marker:
        entries = entries[entries.index(marker):]
    
    if max_keys:
        entries = entries[:max_keys]
            
    return entries, cprefixes
