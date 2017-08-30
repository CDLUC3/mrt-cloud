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
import java.util.Set;
import java.io.Serializable;
import static org.cdlib.mrt.s3.cloudhost.CloudhostStateAbs.getProperties;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LinkedHashList;
import org.cdlib.mrt.utility.MessageDigestType;
import org.cdlib.mrt.utility.StringUtil;

/**
 *
 * @author dloy
 */
public class CloudhostFixityState
        extends CloudhostStateAbs
        implements StateInf, Serializable
{
    protected static final String NAME = "CloudhostPropState";

    //protected LinkedHashList<String, String> list = new LinkedHashList();
    protected Properties prop = new Properties();
    protected String bucket = null;
    protected String key = null;
    protected String error = null;
    protected String testChecksum = null;
    protected long testLength = 0;
    protected String dataChecksum = null;
    protected long dataLength = 0;
    protected boolean ok = false;
    protected MessageDigestType checksumType = null;

    public CloudhostFixityState() { }
   
    public CloudhostFixityState(File propFile)
        throws TException
    {
        Properties prop = getProperties(propFile);
        anvlSet(prop);
    }
    
    public void anvlSet(Properties prop)
       throws TException
    {
        System.out.println(PropertiesUtil.dumpProperties("DUMPPROP", prop));
        String okS = getPropEx("ok", prop);
        setOK(StringUtil.argIsTrue(okS));
        setBucket(getPropEx("bucket", prop));
        setKey(getPropEx("key", prop));
        setError(getProp("error", prop));
        if (getError() == null) {
            setChecksumType(MessageDigest.getAlgorithm(getPropEx("checksumType", prop)));
            setDataChecksum(getPropEx("dataChecksum", prop));
            setTestChecksum(getPropEx("testChecksum", prop));
            setDataLength(getLongEx("dataLength", prop));
            setTestLength(getLongEx("testLength", prop));
        }
    }


    public String getKey() {
        return key;
    }

    public String getBucket() {
        return bucket;
    }

    public CloudhostFixityState setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public CloudhostFixityState setKey(String key) {
        this.key = key;
        return this;
    }

    public String getError() {
        return error;
    }

    public CloudhostFixityState setError(String error) {
        this.error = error;
        return this;
    }

    public String getTestChecksum() {
        return testChecksum;
    }

    public CloudhostFixityState setTestChecksum(String testChecksum) {
        this.testChecksum = testChecksum;
        return this;
    }

    public long getTestLength() {
        return testLength;
    }

    public CloudhostFixityState setTestLength(long testLength) {
        this.testLength = testLength;
        return this;
    }

    public String getDataChecksum() {
        return dataChecksum;
    }

    public CloudhostFixityState setDataChecksum(String dataChecksum) {
        this.dataChecksum = dataChecksum;
        return this;
    }

    public long getDataLength() {
        return dataLength;
    }

    public CloudhostFixityState setDataLength(long dataLength) {
        this.dataLength = dataLength;
        return this;
    }

    public boolean isOk() {
        return ok;
    }

    public CloudhostFixityState setOK(boolean ok) {
        this.ok = ok;
        return this;
    }

    public String getChecksumType() {
        return checksumType.getJavaAlgorithm();
    }

    public MessageDigestType retrieveChecksumType() {
        return checksumType;
    }

    public CloudhostFixityState  setChecksumType(String checksumTypeS) {
        this.checksumType = MessageDigest.getAlgorithm(checksumTypeS);
        return this;
    }

    public CloudhostFixityState  setChecksumType(MessageDigestType checksumType) {
        this.checksumType = checksumType;
        return this;
    }
    
    public String dump(String header)
    {
        return header + ":"
                + " - bucket=" + getBucket()
                + " - key=" + getKey()
                + " - error=" + getError()
                + " - checksumType=" + getChecksumType()
                + " - dataChecksum=" + getDataChecksum()
                + " - testChecksum=" + getTestChecksum()
                + " - dataLength=" + getDataLength()
                + " - testLength=" + getTestLength()
            ;
    }
}

