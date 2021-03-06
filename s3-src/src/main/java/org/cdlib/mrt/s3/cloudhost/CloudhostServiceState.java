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
import java.util.Properties;
import java.io.Serializable;
import static org.cdlib.mrt.s3.cloudhost.CloudhostStateAbs.getProperties;
import org.cdlib.mrt.utility.GetProp;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 *
 * @author dloy
 */
public class CloudhostServiceState
        extends CloudhostStateAbs
        implements StateInf, Serializable
{
    protected static final String NAME = "CloudhostServiceState";

    protected long node = 0;
    protected String bucket = null;
    protected String error = null;
    protected Boolean ok = null;

    public CloudhostServiceState() { }
    
    public CloudhostServiceState(File propFile)
        throws TException
    {
        Properties prop = getProperties(propFile);
        anvlSet(prop);
    }
    
    public void anvlSet(Properties prop)
       throws TException
    {
        //System.out.println(PropertiesUtil.dumpProperties(NAME, prop));
        setBucket(getPropEx("bucket", prop));
        setNode(getLongEx("node", prop));
        if (getProp("ok", prop) != null) {
            Boolean okB = GetProp.getBool(prop, "ok");
            setOk(okB);
        }
        if (getProp("error", prop) == null) {
            setError(getProp("error", prop));
        }
    }
    


    public String getBucket() {
        return bucket;
    }

    public CloudhostServiceState setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public long getNode() {
        return node;
    }

    public CloudhostServiceState setNode(long node) {
        this.node = node;
        return this;
    }

    public String getError() {
        return error;
    }

    public CloudhostServiceState setError(String error) {
        this.error = error;
        return this;
    }

    public Boolean getOk() {
        return ok;
    }

    public CloudhostServiceState setOk(Boolean ok) {
        this.ok = ok;
        return this;
    }
    
    public String dump(String header)
    {
        header = header + ":"
                + " - bucket=" + getBucket()
                + " - node=" + getNode()
            ;
        if (ok != null) {
            header = header 
                + " - ok=" + getOk()
                + " - error=" + getError()
            ;
        }
        return header;
    }

}

