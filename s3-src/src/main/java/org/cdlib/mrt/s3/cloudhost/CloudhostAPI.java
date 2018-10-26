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
package org.cdlib.mrt.s3.cloudhost;
//import org.cdlib.mrt.s3.service.*;



import org.cdlib.mrt.s3.service.*;

import org.cdlib.mrt.core.Identifier;
import java.io.File;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;



import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudStoreAbs;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.DeleteOnCloseFileInputStream;


import java.util.List;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.PropertiesUtil;

/**
 * Specific SDSC Storage Cloud handling
 * @author dloy
 */
public class CloudhostAPI
    extends CloudStoreAbs
    implements CloudStoreInf
{
    private static final boolean DEBUG = false;
    private static final boolean ALPHANUMERIC = false;
    protected static final String NAME = "CloudhostAPI";
    protected static final String MESSAGE = NAME + ": ";
    protected String base = null;

    public static CloudhostAPI getCloudhostAPI(
            String base,
            LoggerInf logger)
        throws TException
    {
        CloudhostAPI cloud =  new CloudhostAPI(base, logger);
        return cloud;
    }
    
    protected CloudhostAPI(
            String base,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.base = base;
        if (this.base == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "url missing");
        }
    }
    
    public CloudResponse putObject(
            CloudResponse response,
            File inputFile)
        throws TException
    {        
        try {
            if (!isValidFile(inputFile)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - file not valid");
            }
            String bucketName = response.getBucketName();
            String key = response.getStorageKey();
            if (DEBUG)System.out.println("***putObject: FILE " 
                    + " - size:" + inputFile.length() + "\n"
                    + " - bucket:" + bucketName + "\n"
                    + " - key:" + key + "\n"
            );
            Properties objectMeta = getObjectMeta(bucketName, key);
            if (objectMeta == null) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(MESSAGE + "putObject objectMeta null:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                );
            }
            String fileSha256 = CloudUtil.getDigestValue("sha256", inputFile, logger);
            if (objectMeta.size() > 0) {
                String storeSha256= objectMeta.getProperty("sha256");
                if ((storeSha256 != null) && fileSha256.equals(storeSha256))  {
                    if (DEBUG) System.out.println("***File match:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                            + " - sha256: "+ fileSha256
                    );
                    response.setFileMeta(cloudProp);
                    response.setFromProp();
                    return response;
                } else {
                    CloudResponse deleteResponse = deleteObject(bucketName, key);
                    System.out.println("***Existing file deleted- does not match:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                            + " - fileSha256: "+ fileSha256
                            + " - storeSha256: "+ storeSha256
                    );
                }
            }
            
            CloudhostAddState state = CloudhostClient.add(base, getNode(bucketName), key, inputFile, logger);
            throwCloudhostException(state.getError());
            if (!state.isAdded()) {
                response.setStatus(CloudResponse.ResponseStatus.fail);
                return response;
            } else {
                response.setStatus(CloudResponse.ResponseStatus.ok);
            }
            Properties putObjectMeta = null;
            //retries required because meta may not be available immediately
            for (int t=1; t<=5; t++) {
                putObjectMeta = getObjectMeta(bucketName, key);
                if (putObjectMeta.size() > 0) break;
                System.out.println("***getObjectMeta fails - sleep:" + (t*2000)
                            + " - bucket:" + bucketName
                            + " - key:" + key
                );
                Thread.sleep(t*2000);
            }
            if (putObjectMeta.size() == 0) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "putObject fails:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                );
            }
            
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("putdump", cloudProp));
            String outSha256 = putObjectMeta.getProperty("sha256");
            if (outSha256 == null) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "putObject no sha256 metadata:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                );
            }
            if (DEBUG) System.out.println("TransferManager"
                        + " - in:" + fileSha256
                        + " - out:" + outSha256
            );
            if (!fileSha256.equals(outSha256)) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "putObject sha256 mismatch"
                        + " - in:" + fileSha256
                        + " - out:" + outSha256
                );
            }
            String sizeS = putObjectMeta.getProperty("size");
            if (sizeS == null) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "putObject no size metadata:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                );
            }
            long sizeS3 = Long.parseLong(sizeS);
            
            if (DEBUG) System.out.println("Lengths"
                        + " - inputFile.length:" + inputFile.length()
                        + " - sizeS3:" + sizeS3
            );
            if (inputFile.length() != sizeS3) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "putObject sha256 mismatch"
                        + " - inputFile.length:" + inputFile.length()
                        + " - sizeS3:" + sizeS3
                );
            }
            response.setFromProp(putObjectMeta);
                        
        } catch (Exception ex) {
            System.out.println("ex1");
            ex.printStackTrace();
            handleException(response, ex);
            
        }
        return response;
    }
    
    
    @Override
    public CloudResponse putObject(
            String bucket,
            String key,
            File inputFile)
        throws TException
    { 
        CloudResponse response = CloudResponse.get(bucket, key);
        return putObject(response, inputFile);
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
        CloudResponse response = CloudResponse.get(bucketName, objectID, versionID, fileID);
        return putObject(response, inputFile);
    }
    
//    @Override
    public CloudResponse putObject(
            String bucketName,
            String key,
            File inputFile,
            Properties fileMeta)
        throws TException
    { 
        if (StringUtil.isEmpty(bucketName)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - bucketName not valid");
        }
        if (StringUtil.isEmpty(key)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - key not valid");
        }
        CloudResponse response = CloudResponse.get(bucketName, key);
        response.setFileMeta(fileMeta);
        return putObject(response, inputFile);
    }
    
//    @Override
    public CloudResponse putObject(
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID,
            File inputFile,
            Properties fileMeta)
        throws TException
    {
        CloudResponse response = CloudResponse.get(bucketName, objectID, versionID, fileID);
        response.setFileMeta(fileMeta);
        return putObject(response, inputFile);
    }
    
    @Override
    public CloudResponse putManifest(
            String bucketName,
            Identifier objectID,
            File inputFile)
        throws TException
    {
        CloudResponse response = CloudResponse.get(bucketName, objectID);
        return putObject(response, inputFile);
    }
    
    public CloudResponse cloudhostDelete (
            CloudResponse response)
        throws TException
    {
        try {
            String bucket = response.getBucketName();
            String key = response.getStorageKey();
            
            CloudhostDeleteState state = CloudhostClient.delete(base, getNode(bucket), key, logger);
            if (!state.isDeleted()) {
                response.setStatus(CloudResponse.ResponseStatus.fail);
            } else {
                response.setStatus(CloudResponse.ResponseStatus.ok);
            }
            throwCloudhostException(state.getError());
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }
    
    @Override
    public CloudResponse deleteObject (
            String bucket,
            String key)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = CloudResponse.get(bucket, key);
            return cloudhostDelete(response);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }

    @Override
    public CloudResponse deleteObject (
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = CloudResponse.get(bucketName, objectID, versionID, fileID);
            return cloudhostDelete(response);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }

    @Override
    public CloudResponse deleteManifest (
            String bucketName,
            Identifier objectID)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = CloudResponse.get(bucketName, objectID);
            return cloudhostDelete(response);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }
    
    
    public void cloudhostGet(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
     try {
            response.set(container, key);
            System.out.println("awsGet"
                    + " - container:" + container
                    + " - key:" + key
            );
            Properties objectProp = getObjectMeta(container, key);
            if (objectProp.size() > 0 ) {
                response.setFromProp(objectProp);
            } else {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Item not found:"
                            + " - bucket=" + container
                            + " - key=" + key
                );
            }
            CloudhostClient.getData(base, getNode(container), key, outFile, logger);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
        }
    }
    
    public void cloudhostRestore(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        cloudhostGet(container, key, outFile, response);
    }
    
    public void restoreObject(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        try {
            cloudhostRestore(container, key, outFile, response);
            
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
            cloudhostGet(bucketName, key, tmpFile, response);
            DeleteOnCloseFileInputStream is = new DeleteOnCloseFileInputStream(tmpFile);
            return is;
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    public InputStream getObjectStreaming(
            String bucketName,
            String key,
            CloudResponse response)
        throws TException
    {
        return getObject(bucketName, key, response);
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
            cloudhostGet(container, key, outFile, response);
            
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
            
            if (DEBUG) System.out.println("***getObjectMeta"
                    + " - bucketName:" + bucketName + "\n"
                    + " - key:" + key + "\n"
            );
            CloudhostMetaState state = CloudhostClient.getMeta(base, getNode(bucketName), key, logger);
            prop = state.getProp();
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("getObjectMeta", prop));
            return prop;
            
        } catch (Exception ex) {
            if (ex.toString().contains("404")) {
                return new Properties();
            }
            CloudResponse response = new CloudResponse(bucketName, key);
            cloudhostHandleException(response, ex);
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
            cloudhostHandleException(response, ex);
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
        CloudResponse response = null;
        try {
            throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getObjectList: this repository form does not support this function");
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    @Override
    public CloudResponse getObjectList (
            String bucketName,
            String key,
            int limit)
        throws TException
    {
        CloudResponse response = null;
        try {
            throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getObjectList: this repository form does not support this function");
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }

//    @Override
    public CloudResponse getObjectList (
            String bucketName)
        throws TException
    {
        CloudResponse response = null;
        try {
            throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getObjectList: this repository form does not support this function");
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    
    /**
     * Return status of cloud
     * @param bucketName s3 bucket - rackspace container
     * @return null=not supported or unable, true=running, false=failed condition occured
     * @throws TException 
     */
    public Boolean getStateLocal (
            String bucketName)
        throws TException
    {
        try {
            
            CloudhostServiceState state = CloudhostClient.getService(base, 0, logger);
            return state.getOk();
            
        } catch (Exception ex) {
            return null;
        }
    }
    
        
    @Override
    public StateHandler.RetState getState (
            String bucketName)
        throws TException
    {
        StateHandler stateHandler = null;
        try {
            stateHandler = StateHandler.getStateHandler(this, bucketName, base, logger);
            return stateHandler.process();
            
        } catch (Exception ex) {
            return StateHandler.getError(bucketName, NAME, "Error getState:" + ex);
        }
    }
    
    @Override
    public boolean isAlphaNumericKey() 
    {
        return ALPHANUMERIC;
    }
    
    
    public static long getNode(String nodeS)
        throws TException
    {
        try {
            return Long.parseLong(nodeS);
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM("getNode - invalid node:" + nodeS);
        }
    }
    
    public void  throwCloudhostException(String error)
        throws TException
    {
        if (error == null) return;
        Exception buildException = null;
        if (error.contains("REQUESTED_ITEM_NOT_FOUND")) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND(error);
        } else {
            throw new TException.GENERAL_EXCEPTION(error);
        }
    }
    
    public void cloudhostHandleException(CloudResponse response, Exception exception)
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
    
    public Boolean isAlive(String bucketName)
    {
        return isAliveTest(base);
    }
}

