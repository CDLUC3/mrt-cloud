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
import static org.cdlib.mrt.s3.cloudhost.CloudhostStateAbs.getProperties;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 *
 * @author dloy
 */
public class CloudhostDeleteState
        extends CloudhostStateAbs
        implements StateInf, Serializable
{
    protected static final String NAME = "CloudhostDeleteState";

    protected boolean deleted = false;
    protected String bucket = null;
    protected String key = null;
    protected String error = null;

    public CloudhostDeleteState() { }
    
    public CloudhostDeleteState(File propFile)
        throws TException
    {
        Properties prop = getProperties(propFile);
        anvlSet(prop);
    }
    
    public void anvlSet(Properties prop)
       throws TException
    {
        String addedS = getPropEx("deleted", prop);
        setDeleted(StringUtil.argIsTrue(addedS));
        setBucket(getPropEx("bucket", prop));
        setKey(getPropEx("key", prop));
    }

    /**
     * Constructor - extract local beans from properties
     * @param storageProp Properties containing store-info.txt values
     * @throws TException process exception
     */
    protected CloudhostDeleteState(boolean deleted, String key)
        throws TException
    {
        this.deleted = deleted;
        this.key = key;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public CloudhostDeleteState setDeleted(boolean deleted) {
        this.deleted = deleted;
        return this;
    }

    public String getKey() {
        return key;
    }

    public String getBucket() {
        return bucket;
    }

    public CloudhostDeleteState setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public CloudhostDeleteState setKey(String key) {
        this.key = key;
        return this;
    }

    public String getError() {
        return error;
    }

    public CloudhostDeleteState setError(String error) {
        this.error = error;
        return this;
    }


    public String dump(String header)
    {
        return header + ":"
                    + " - deleted=" + isDeleted()
                    + " - key=" + getKey()
                    ;
    }

}

