/*
Copyright (c) 2005-2012, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************/
/**
 *
 * @author dloy
 */
package org.cdlib.mrt.openstack.utility;
import java.util.Properties;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
    
public class XValues
{
    protected static final String NAME = "XValues";
    protected static final String MESSAGE = NAME + ": ";
    public String xAuthToken = null;
    public String xStorageUrl = null;
    public XValues(String xAuthToken, String xStorageUrl)
    {
        this.xAuthToken = xAuthToken;
        this.xStorageUrl = xStorageUrl;
    }
    public XValues(Properties responseProp)
        throws TException
    {
        this.xAuthToken = responseProp.getProperty("header.X-Auth-Token");
        this.xStorageUrl = responseProp.getProperty("header.X-Storage-Url");
        String statusS = responseProp.getProperty("response.status");
               
        if (StringUtil.isEmpty(statusS)) {
            System.out.println(PropertiesUtil.dumpProperties("Missing status", responseProp));
            throw new TException.USER_NOT_AUTHENTICATED(MESSAGE + "status not found");
        }
        int status = Integer.parseInt(statusS);
        if ((status < 200) || (status >= 400)) {
            System.out.println(PropertiesUtil.dumpProperties("Bad Auth status", responseProp));
            throw new TException.USER_NOT_AUTHENTICATED(MESSAGE + "Bad Auth status:" + status);
        }
        
        if (StringUtil.isEmpty(this.xAuthToken)) {
            System.out.println(PropertiesUtil.dumpProperties("Missing header.X-Auth-Token", responseProp));
            throw new TException.USER_NOT_AUTHENTICATED(MESSAGE + "X-Auth-Token not found");
        }
        if (StringUtil.isEmpty(this.xStorageUrl)) {
            System.out.println(PropertiesUtil.dumpProperties("Missing header.X-Storage-Url", responseProp));
            throw new TException.USER_NOT_AUTHENTICATED(MESSAGE + "xStorageUrl not found");
        }
    }
}