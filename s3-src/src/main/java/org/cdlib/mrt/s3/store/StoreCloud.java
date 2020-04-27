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
package org.cdlib.mrt.s3.store;
//import org.cdlib.mrt.s3.service.*;



import org.cdlib.mrt.s3.service.*;

import org.cdlib.mrt.core.Identifier;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;



import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreAbs;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.DeleteOnCloseFileInputStream;


import java.util.List;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.core.DateState;
import static org.cdlib.mrt.s3.store.StoreClient.MESSAGE;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.MessageDigestValue;
import org.cdlib.mrt.utility.PropertiesUtil;

/**
 * Specific SDSC Storage Cloud handling
 * @author dloy
 */
public class StoreCloud
    extends CloudStoreAbs
    implements CloudStoreInf
{
    private static final boolean DEBUG = false;
    private static final boolean ALPHANUMERIC = false;
    protected static final String NAME = "StoreCloud";
    protected static final String MESSAGE = NAME + ": ";
    private String baseUrl = null;
    private Integer storeNode = null;

    public static StoreCloud getStoreCloud(
            String baseUrl,
            Integer node,
            LoggerInf logger)
        throws TException
    {
        StoreCloud cloud =  new StoreCloud(baseUrl, node, logger);
        return cloud;
    }
    
    protected StoreCloud(
            String baseUrl,
            Integer storeNode,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.baseUrl = baseUrl;
        this.storeNode = storeNode;
        if (StringUtil.isAllBlank(this.baseUrl)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getKey - baseUrl required");
        }
    }
    
    public CloudResponse putObject(
            CloudResponse response,
            File inputFile)
        throws TException
    {        
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "putObject - upload not supported");
    }
    
    
    @Override
    public CloudResponse putObject(
            String bucket,
            String key,
            File inputFile)
        throws TException
    { 
              
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "putObject - upload not supported");
    }
    
    @Override
    public CloudResponse putObject(
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID,
            File inputFile)
        throws TException
    {      
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "putObject - upload not supported");
    }
        
    @Override
    public CloudResponse putManifest(
            String bucketName,
            Identifier objectID,
            File inputFile)
        throws TException
    {      
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "putManifest - upload not supported");
    }
    
    @Override
    public CloudResponse deleteObject (
            String bucket,
            String key)
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "deleteObject - delete not supported");
    }

    @Override
    public CloudResponse deleteObject (
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID)
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "deleteObject - delete not supported");
    }

    @Override
    public CloudResponse deleteManifest (
            String bucketName,
            Identifier objectID)
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "deleteObject - delete not supported");
    }
    
    public void restoreObject(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "restoreObject - restore not supported");
    }
    
    public void getStoreObject(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        try {
            response.set(container, key);
            Integer nodeID = StoreClient.getNodeID(container);
            URL url = StoreClient.keyToURL(storeNode, key, baseUrl, "content", "fixity=no");
            FileUtil.url2File(logger, url, outFile, 3);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
    }
    
    public void getStoreManifest(
            String bucket,
            Identifier objectID,
            File outFile,
            CloudResponse response)
        throws TException
    {
        try {
            response.setManifest(bucket, objectID);
            Integer nodeID = StoreClient.getNodeID(bucket);
            URL url = StoreClient.getManifestURL(storeNode, objectID, baseUrl);
            FileUtil.url2File(logger, url, outFile, 3);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
    }
    @Override
    public InputStream getObject(
            String name,
            Identifier objectID,
            int versionID,
            String fileID,
            CloudResponse response)
        throws TException
    {
        try {
            response.set(name, objectID, versionID, fileID);
            return getObject(name, response.getStorageKey(), response);
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }

    @Override
    public InputStream getObject(
            String bucketName,
            String key,
            CloudResponse response)
        throws TException
    {
        try {
            response.set(bucketName, key);
            File tmpFile = FileUtil.getTempFile("clouttemp", ".txt");
            getStoreObject(bucketName, key, tmpFile, response);
            DeleteOnCloseFileInputStream is = new DeleteOnCloseFileInputStream(tmpFile);
            return is;
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    @Override
    public InputStream getObjectStreaming(
            String bucketName,
            String key,
            CloudResponse response)
        throws TException
    { 
              
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getObjectStreaming - not supported");
    }
    
    @Override
    public void getObject(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        try {
            getStoreObject(container, key, outFile, response);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
    }
    
    @Override
    public Properties getObjectMeta (
            String bucketName,
            String key)
        throws TException
    {
        Properties prop = new Properties();
        try {
            Integer nodeID = StoreClient.getNodeID(bucketName);
            URL url = StoreClient.keyToURL(storeNode, key, baseUrl, "state", "t=anvl");
            File tmpFile = FileUtil.getTempFile("tmp", ".properties");
            FileUtil.url2File(logger, url, tmpFile, 3);
            Properties outProp = PropertiesUtil.loadFileProperties(tmpFile);
            addProp(prop, "sha256", outProp.getProperty("sha256"));
            addProp(prop, "size", outProp.getProperty("size"));
            addProp(prop, "bucket", bucketName);
            addProp(prop, "key", key);
            addProp(prop, "modified", outProp.getProperty("created"));
            System.out.println(PropertiesUtil.dumpProperties("getObjectMeta", prop));
            return prop;
            
        } catch (Exception ex) {
            if (ex.toString().contains("404")) {
                return new Properties();
            }
            CloudResponse response = new CloudResponse(bucketName, key);
            awsHandleException(response, ex);
            return null;
        }
    }
    
    protected static void addProp(Properties prop, String key, String value)
    {
        if (value == null) return;
        prop.setProperty(key, value);
    }

    @Override
    public InputStream getManifest(
            String bucket,
            Identifier objectID)
        throws TException
    {
        InputStream stream = null;
        CloudResponse response =  new CloudResponse();
        response.setManifest(bucket, objectID);
        try {
            stream =  getManifest(bucket, objectID,  response);
            Exception exception = response.getException();
            if (exception != null) {
                if (exception instanceof TException.REQUESTED_ITEM_NOT_FOUND) {
                    return null;
                }
                throw response.getException();
            }
            return stream;
            
            
        } catch (Exception ex) {
            awsHandleException(response, ex);
            return null;
        }
    }

    @Override
    public InputStream getManifest(
            String bucket,
            Identifier objectID,
            CloudResponse response)
        throws TException
    {
        try {
            response.setManifest(bucket, objectID);
            return getObject(bucket, response.getStorageKey(), response);
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    @Override
    public CloudResponse getObjectList (
            String bucketName,
            String key)
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "restoreObject - restore not supported");
    }

    @Override
    public CloudResponse getObjectList (
            String bucketName)
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getObjectList - not supported");
    }
    
    @Override
    public CloudResponse getObjectList (
            String bucketName,
            String key,
            int limit)
        throws TException
    {
        throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getObjectList - not supported");
    }
        
    @Override
    public StateHandler.RetState getState (
            String bucketName)
        throws TException
    {
        StateHandler stateHandler = null;
        try {
            stateHandler = StateHandler.getStateHandler(this, bucketName, logger);
            return stateHandler.process();
            
        } catch (Exception ex) {
            return StateHandler.getError(bucketName, NAME, "Error getState:" + ex);
        }
    }
    
    @Override
    public Boolean isAlive(String bucketName)
    {
        return null;
    }
    
    @Override
    public CloudResponse getPreSigned (
        long expirationMinutes,
        String bucketName,
        String key,
        String contentType,
        String contentDisp)
    throws TException
    {
        CloudResponse response = new CloudResponse(bucketName, key);
        try {
            throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getPreSigned: this cloud repository does not support this function");
            
        } catch (Exception ex) {
            handleException(response, ex);
            return response;
        }
    }
    
    @Override    
    public CloudAPI getType()
    {
        return CloudAPI.STORE;
    }
    
    @Override
    public boolean isAlphaNumericKey() 
    {
        return ALPHANUMERIC;
    }
 


    public void awsHandleException(CloudResponse response, Exception exception)
        throws TException
    {
        if (exception instanceof com.amazonaws.services.s3.model.AmazonS3Exception) {
            String exvalue = exception.toString();
            if (exvalue.contains("Access Denied") || exvalue.contains("403")) {
                throw new TException.USER_NOT_AUTHENTICATED("AWS fails authentication");
            } else {
                throw new TException(exception);
            }
        }
        if ((exception instanceof TException.USER_NOT_AUTHENTICATED) 
                || (exception instanceof TException.USER_NOT_AUTHORIZED)) {
            throw (TException) exception;
        }
        if (exception instanceof TException.REQUESTED_ITEM_NOT_FOUND) {
            throw (TException) exception;
        }
        if (exception instanceof TException.REQUEST_ITEM_EXISTS) {
            throw (TException) exception;
        }
        if (exception instanceof TException.NEARLINE_RESTORE_IN_PROCESS) {
            throw (TException) exception;
        }
        
        handleException(response, exception);
    }
}

