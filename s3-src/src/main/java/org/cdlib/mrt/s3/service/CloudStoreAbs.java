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
*********************************************************************/
package org.cdlib.mrt.s3.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.UUID;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.core.MessageDigest;


import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.FileUtil;

/**
 * General abstract class for cloud storage
 * @author dloy
 */
public abstract class CloudStoreAbs
{
    protected static final String NAME = "CloudStoreAbs";
    protected static final String MESSAGE = NAME + ": ";
    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = false;
    
    protected LoggerInf logger = null;
    protected String endpointHostname = null;
    private String awsAccessKey = null;
    private String awsSecretKey = null;
    protected Properties cloudProp = null;
    
    public CloudStoreAbs(InputStream propStream, LoggerInf logger)
        throws TException
    {
        this.logger = logger;
        setProperties(propStream);
    }
    
    public CloudStoreAbs(Properties cloudProp, LoggerInf logger)
        throws TException
    {
        this.logger = logger;
        this.cloudProp = cloudProp;
        setProperties();
    }
    
    public CloudStoreAbs(LoggerInf logger)
        throws TException
    {
        this.logger = logger;
    }
    
    public CloudStoreAbs(CloudStoreAbs cloudStoreAbs)
        throws TException
    {
        this.logger = cloudStoreAbs.logger;
        this.cloudProp = cloudStoreAbs.cloudProp;
    }
    
    private void setProperties(InputStream propStream)
        throws TException
    {
        
        try {
            cloudProp = new Properties();
            cloudProp.load(propStream);
            setProperties();
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    private void setProperties()
        throws TException
    {
        
        try {
            awsAccessKey = gPropEx(cloudProp, "access_key");
            awsSecretKey = gPropEx(cloudProp, "secret_key");
            endpointHostname = gPropEx(cloudProp, "host");
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    private static String gPropEx(Properties prop, String key)
        throws TException
    {
        if (StringUtil.isEmpty(key)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "gPropEx: key empty");
        }
        if (prop == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "gPropEx: Properties not provided");
        }
        String value = prop.getProperty(key);
        if (StringUtil.isEmpty(value)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "gPropEx: missing value - key=" + key);
        }
        return value;
    }
    
    public boolean isValidFile(File testFile)
    {
        if (testFile == null) return false;
        if (!testFile.exists()) return false;
        return true;
    }
    
    
    
    public void handleException(CloudResponse response, Exception exception)
    {
        if (DEBUG) System.out.println(MESSAGE + "handleException entered");
        response.setException(exception);
        StringBuffer buf = new StringBuffer();
        if (exception instanceof TException.REQUESTED_ITEM_NOT_FOUND) {
            buf.append("NAME=" + exception.getClass().getName());
            buf.append("Exception:" + exception);
            
        } else if (exception instanceof TException.REQUEST_ITEM_EXISTS) {
            buf.append("NAME=" + exception.getClass().getName());
            buf.append("Exception:" + exception);
            
        } else {
            exception.printStackTrace();
            buf.append("NAME=" + exception.getClass().getName());
            buf.append("Exception:" + exception);
            
        }
        response.setErrMsg(buf.toString());
        response.setStatus(CloudResponse.ResponseStatus.fail);
        if (DEBUG) {
            System.out.println(buf.toString());
        }
        logger.logError(buf.toString(), 5);
    }
    
    public CloudResponse validateMd5(String bucketName, String key, String inMd5)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = getObjectList(bucketName, key);
            if (response.getException() != null) {
                response.setMatch(false);
                return response;
            }
            CloudList objects = response.getCloudList();
            if ((objects == null) || (objects.size() == 0)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("item not found - key=" + key);
            }
            if (objects.size() != 1) {
                throw new TException.INVALID_OR_MISSING_PARM("key returns multiple objects - key=" + key
                        + " - count=" + objects.size());
            }
            CloudList.CloudEntry entry = objects.get(0);
            String retMd5 = entry.getEtag();
            if (DEBUG) System.out.println("RETMD5=" + retMd5);
            if (retMd5.equals(inMd5)) response.setMatch(true);
            else response.setMatch(false);
            response.setMd5(retMd5);
            return response;
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    public CloudResponse validateDigest(String bucketName, String key, MessageDigest digest, long length)
        throws TException
    {
        CloudResponse responseDigest = null;
        InputStream inStream = null;
        try {
            responseDigest = new CloudResponse(bucketName, key);
            
            inStream = getObject(bucketName, key, responseDigest);
            String checksumType = digest.getJavaAlgorithm();
            String checksum = digest.getValue();
            FixityTests test = new FixityTests(inStream, checksumType, logger);
            FixityTests.FixityResult result = test.validateSizeChecksum(checksum, checksumType, length);
            boolean match = result.checksumMatch && result.fileSizeMatch;
            responseDigest.setMatch(match);
            if (!match) {
                responseDigest.setErrMsg(result.dump("validateDigest"));
            }
            return responseDigest;
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex
                    + " - bucketName:" + bucketName
                    + " - key:" + key
            );
            handleException(responseDigest, ex);
            return null;
        }
    }

    public Properties getCloudProp() {
        return cloudProp;
    }
    
    abstract public InputStream getObject(String bucketName, String key, CloudResponse response)
            throws TException;
    
    abstract public CloudResponse getObjectList(String bucketName, String key)
            throws TException;
}

