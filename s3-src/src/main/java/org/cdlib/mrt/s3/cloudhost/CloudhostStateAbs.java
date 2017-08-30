/*
Copyright (c) 2005-2010, Regents of the University of California
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
package org.cdlib.mrt.s3.cloudhost;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;

import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 *
 * @author dloy
 */
public class CloudhostStateAbs
        implements StateInf, Serializable
{

    public CloudhostStateAbs() { }

    protected static String getPropEx(String key, Properties prop)
        throws TException
    {
        if (StringUtil.isAllBlank(key)) {
            throw new TException.INVALID_OR_MISSING_PARM("getPropEx - key required");
        }
        String retValue = prop.getProperty(key);
        if (retValue == null) {
            throw new TException.INVALID_OR_MISSING_PARM("getPropEx - required value not found - key:" + key);
        }
        return retValue;
    }

    protected static String getProp(String key, Properties prop)
        throws TException
    {
        if (StringUtil.isAllBlank(key)) {
            throw new TException.INVALID_OR_MISSING_PARM("getPropEx - key required");
        }
        String retValue = prop.getProperty(key);
        return retValue;
    }
    
    

    protected static long getLongEx(String key, Properties prop)
        throws TException
    {
        String longS = getPropEx(key, prop);
        try {
            long retLong = Long.parseLong(longS);
            return retLong;
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM("getLongEx - long value invalid:" + longS);
        }
    }

    protected static Properties getProperties(File propFile)
        throws TException
    {
        if (propFile == null) {
            throw new TException.INVALID_OR_MISSING_PARM("getProperties - propFile required");
        }
        Properties prop = PropertiesUtil.loadFileProperties(propFile);
        return prop;
    }

}