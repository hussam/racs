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

import httplib

# Defined in httplib:
# client error
# BAD_REQUEST = 400
# UNAUTHORIZED = 401
#PAYMENT_REQUIRED = 402
#FORBIDDEN = 403
#NOT_FOUND = 404
#METHOD_NOT_ALLOWED = 405
#NOT_ACCEPTABLE = 406
#PROXY_AUTHENTICATION_REQUIRED = 407
#REQUEST_TIMEOUT = 408
#CONFLICT = 409
#GONE = 410
#LENGTH_REQUIRED = 411
#PRECONDITION_FAILED = 412
#REQUEST_ENTITY_TOO_LARGE = 413
#REQUEST_URI_TOO_LONG = 414
#UNSUPPORTED_MEDIA_TYPE = 415
#REQUESTED_RANGE_NOT_SATISFIABLE = 416
#EXPECTATION_FAILED = 417
#UNPROCESSABLE_ENTITY = 422
#LOCKED = 423
#FAILED_DEPENDENCY = 424
#UPGRADE_REQUIRED = 426

# server error
#INTERNAL_SERVER_ERROR = 500
#NOT_IMPLEMENTED = 501
#BAD_GATEWAY = 502
#SERVICE_UNAVAILABLE = 503
#GATEWAY_TIMEOUT = 504
#HTTP_VERSION_NOT_SUPPORTED = 505
#INSUFFICIENT_STORAGE = 507
#NOT_EXTENDED = 510

class HTTPException(Exception):
    error_code = None
    msg = None

    def __init__(self, code=None, msg=None):
        if code is None:
            code = self.error_code
            if code is None:
                raise Exception("No error code given for HTTPException")
        if msg is None:
            msg = self.msg
            if msg is None:
                msg = httplib.responses.get(code,"No information")
        Exception.__init__(self, code, msg)
        self.error_code = code
        self.msg = msg
        
class NoSuchBucket(HTTPException):
    error_code = httplib.NOT_FOUND
    msg = "No Such Bucket"


class NotFound(HTTPException):
    error_code = httplib.NOT_FOUND

class BucketNotEmpty(HTTPException):
    error_code = httplib.CONFLICT
    msg = "Bucket Not Empty"


