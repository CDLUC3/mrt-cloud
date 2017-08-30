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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.io.File;
import java.io.Serializable;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LinkedHashList;
import org.cdlib.mrt.utility.StringUtil;

/**
 *
 * @author dloy
 */
public class CloudhostMetaState
        extends CloudhostStateAbs
        implements StateInf, Serializable
{
    protected static final String NAME = "CloudhostPropState";

    //protected LinkedHashList<String, String> list = new LinkedHashList();
    protected Properties prop = new Properties();
    protected String bucket = null;
    protected String key = null;
    protected String error = null;

    public CloudhostMetaState() { }
    
    public CloudhostMetaState(File propFile)
        throws TException
    {
        Properties prop = getProperties(propFile);
        anvlSet(prop);
    }
    
    public void anvlSet(Properties prop)
       throws TException
    {
        setBucket(getPropEx("bucket", prop));
        prop.remove("bucket");
        setKey(getPropEx("key", prop));
        prop.remove("key");
        setError(getProp("error", prop));
        prop.remove("error");
        this.prop = prop;
    }
    

    public String getKey() {
        return key;
    }

    public String getBucket() {
        return bucket;
    }

    public CloudhostMetaState setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public CloudhostMetaState setKey(String key) {
        this.key = key;
        return this;
    }

    public Properties getProp() {
        return prop;
    }

    public Map<String, String> getMeta() {
        if (prop == null) return null;
        HashMap<String, String> list = new HashMap();
        Set set = prop.keySet();
        for (Object keyO : set) {
            String key = (String)keyO;
            String value = prop.getProperty(key);
            list.put(key, value);
        }
        return list;
    }

    public CloudhostMetaState setProp(Properties prop) {
        this.prop = prop;
        return this;
    }

    public String getError() {
        return error;
    }

    public CloudhostMetaState setError(String error) {
        this.error = error;
        return this;
    }
    
    public String dump(String header)
    {
        return header + ":"
                + " - bucket=" + getBucket()
                + " - key=" + getKey()
                + " - " + PropertiesUtil.dumpProperties(key, prop)
            ;
    }

}

