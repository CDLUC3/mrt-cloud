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
package org.cdlib.mrt.s3v2.aws;
//import org.cdlib.mrt.s3.service.*;



import org.cdlib.mrt.s3.service.*;

import org.cdlib.mrt.core.Identifier;
import java.io.File;
import java.io.InputStream;
import java.util.Properties;
import java.net.URL;
import java.util.LinkedHashMap;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudStoreAbs;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.CloudResponse;


import org.cdlib.mrt.s3v2.action.*;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.utility.PropertiesUtil;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
//import static org.cdlib.mrt.s3.service.CloudStoreAbs.dumpException;
//import static org.cdlib.mrt.s3test.tasks.d190716_partial_result.UploadProgress.eraseProgressBar;
//import static org.cdlib.mrt.s3test.tasks.d190716_partial_result.UploadProgress.printProgressBar;

/**
 * Specific SDSC Storage Cloud handling
 * @author dloy
 */
public class AWSS3V2Cloud
    extends CloudStoreAbs
    implements CloudStoreInf
{
    //private static final boolean DEBUG = false;
    private static final boolean ALPHANUMERIC = false;
    protected static final String NAME = "AWSS3V2Cloud";
    protected static final String MESSAGE = NAME + ": ";
    
    private static final Logger log4j = LogManager.getLogger();
    public enum S3Type {aws, minio, wasabi};
    
    private V2Client v2Client = null;
    private S3Client s3Client = null;
    private S3AsyncClient s3AsyncClient = null;
    private S3Presigner s3Presigner = null;
    private StorageClass storageClass = null;
    private String endPoint = null;
    private S3Type s3Type = S3Type.aws;

    protected AWSS3V2Cloud(V2Client v2Client, LoggerInf log)
        throws TException
    {
        super(log);
        this.v2Client = v2Client;
        this.s3Client = v2Client.s3Client();
        this.s3AsyncClient = v2Client.s3AsyncClient();
        this.s3Presigner = v2Client.s3Presigner();
    }
    
    public static AWSS3V2Cloud getAWS(LoggerInf log)
        throws TException
    {  
        V2Client v2ClientTemp = V2Client.getAWS();
        AWSS3V2Cloud cloud = new AWSS3V2Cloud(v2ClientTemp, log);
        return cloud;
    }
    
    public static AWSS3V2Cloud getMinio(String accessKey, String secretKey, String endpoint, LoggerInf log)
        throws TException
    {
        V2Client minioClient = V2Client.getMinio(accessKey, secretKey, endpoint);
        AWSS3V2Cloud cloud = new AWSS3V2Cloud(minioClient, log);
        return cloud;
    }
    
    public static AWSS3V2Cloud getSDSC(String accessKey, String secretKey, String endpoint, LoggerInf log)
        throws TException
    {
        V2Client minioClient = V2Client.getSDSC(accessKey, secretKey, endpoint);
        AWSS3V2Cloud cloud = new AWSS3V2Cloud(minioClient, log);
        return cloud;
    }
    
    public static AWSS3V2Cloud getWasabi(String accessKey, String secretKey, String endpoint, LoggerInf log)
        throws TException
    {
        V2Client minioClient = V2Client.getWasabi(accessKey, secretKey, endpoint);
        AWSS3V2Cloud cloud = new AWSS3V2Cloud(minioClient, log);
        return cloud;
    }
    
    
    public CloudResponse putObject(
            CloudResponse response,
            File inputFile)
        throws TException
    {        
        String key = null;
        try {
            if (!isValidFile(inputFile)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - file not valid");
            }
            String bucketName = response.getBucketName();
            key = response.getStorageKey();
            log4j.trace("FILE " 
                    + " - size:" + inputFile.length()
                    + " - bucket:" + bucketName
                    + " - key:" + key
            );
            Properties objectMeta = GetObjectMeta.getObjectMeta(s3Client, bucketName, key);
            /*
            if (objectMeta == null) {
                CloudResponse exResponse = new CloudResponse(bucketName, key);
                Exception ex = new TException.REMOTE_IO_SERVICE_EXCEPTION("Metadata AWS service exception:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                );
                exResponse.setException(ex);
                return exResponse;
            }
            */
            String fileSha256 = CloudUtil.getDigestValue("sha256", inputFile, logger);
            if ((objectMeta != null) && (objectMeta.size() > 0)) {
                String storeSha256= objectMeta.getProperty("sha256");
                if ((storeSha256 != null) && fileSha256.equals(storeSha256))  {
                    log4j.trace("***File match:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                            + " - fileSha256: "+ fileSha256
                    );
                    //response.setFileMeta(cloudProp);
                    //System.out.println(PropertiesUtil.dumpProperties("&&&&&&skip", cloudProp));
                    response.setFromProp(objectMeta);
                    return response;
                } else {
                    CloudResponse deleteResponse = deleteObject(bucketName, key);
                    log4j.debug("***Existing file replaced - does not match:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                            + " - fileSha256: "+ fileSha256
                            + " - storeSha256: "+ storeSha256
                    );
                }
            }
            
            LinkedHashMap<String, String> metadata = new LinkedHashMap();
            metadata.put("sha256", fileSha256);
            
            MultiPartUpload.uploadFileParts(
                s3Client,
                bucketName,
                key, 
                inputFile.getAbsolutePath(),
                metadata);
            
            Properties putObjectMeta = null;
            //retries required because meta may not be available immediately
            long pow = 1;
            for (int t=1; t<=5; t++) {
                putObjectMeta = GetObjectMeta.getObjectMeta(s3Client, bucketName, key);;
                if (putObjectMeta.size() > 0) break;
                pow *= 2;
                String msg ="***getObjectMeta fails(" + t + "): - sleep:" + (pow*2000)
                            + " - bucket:" + bucketName
                            + " - key:" + key
                ;
                log4j.info(msg);
                System.out.println(msg);
                Thread.sleep(pow*2000);
            }
            if (putObjectMeta.size() == 0) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "putObject fails:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                );
            }
            
            log4j.trace(PropertiesUtil.dumpProperties("putdump", cloudProp));
            String outSha256 = putObjectMeta.getProperty("sha256");
            if (outSha256 == null) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "putObject no sha256 metadata:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                );
            }
            log4j.trace("TransferManager"
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
            
            log4j.trace("Lengths"
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
            log4j.error("putObject ex:" + ex, ex);
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
    
    public CloudResponse awsDelete (
            CloudResponse response)
        throws TException
    {
        try {
            String bucket = response.getBucketName();
            String key = response.getStorageKey();
            log4j.trace("delete"
                        + " - bucket:" + bucket
                        + " - key:" + key
            );
            DeleteObjectData.deleteS3Object(s3Client, bucket, key);
            
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
            return awsDelete(response);
            
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
            return awsDelete(response);
            
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
            return awsDelete(response);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }
    
    
    public void awsGet(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
     try {
            response.set(container, key);
            log4j.trace("awsGet"
                    + " - container:" + container
                    + " - key:" + key
            );
            Properties objectProp = getObjectMeta(container, key);
            if (objectProp == null) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("Unable to access service for metadata:"
                        + " - bucket:" + container
                        + " - key:" + key
                );
            };
            
            if (objectProp.size() > 0 ) {
                response.setFromProp(objectProp);
                String storageClass = objectProp.getProperty("storageClass");
                String expirationS = objectProp.getProperty("expiration");
                if ((storageClass != null) && storageClass.equals("GLACIER") && (expirationS == null)) {
                    throw new TException.REQUEST_ITEM_EXISTS("Requested item in Glacier:" 
                            + " - bucket=" + container
                            + " - key=" + key
                    );
                }
            } else {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Item not found:"
                            + " - bucket=" + container
                            + " - key=" + key
                );
            } 
            
            // This change uses single stream only
            // Both Minio (SDSC) and Wasabi failed using multipart TransferManager
            if ((s3Type == S3Type.minio) || (s3Type == S3Type.wasabi)) {
                GetObject.getObjectSync(s3Client, container, key, outFile.getCanonicalPath());
                log4j.trace("Minio file built(" + container + "):"  + outFile.getCanonicalPath());
                
            } else {
                GetObject.downloadObjectTransfer(s3AsyncClient, container, key, outFile.getCanonicalPath());
                log4j.trace("Non-Minio file built(" + container + "):" + outFile.getCanonicalPath());
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            if (ex.toString().contains("404")) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(ex.toString());
            }
            log4j.warn("awsGet Exception:" + ex
                        + " - bucket:" + container
                        + " - key:" + key, ex);
            //ex.printStackTrace();
            throw new TException(ex) ;
        }
    }    
    public InputStream awsGetInputStream(
            String container,
            String key,
            CloudResponse response)
        throws TException
    {
     try {
            response.set(container, key);
            log4j.trace("awsGet"
                    + " - container:" + container
                    + " - key:" + key
            );
            Properties objectProp = getObjectMeta(container, key);
            if (objectProp == null) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("Unable to access service for metadata:"
                        + " - bucket:" + container
                        + " - key:" + key
                );
            };
            
            if (objectProp.size() > 0 ) {
                response.setFromProp(objectProp);
                String storageClass = objectProp.getProperty("storageClass");
                String expirationS = objectProp.getProperty("expiration");
                if ((storageClass != null) && storageClass.equals("GLACIER") && (expirationS == null)) {
                    throw new TException.REQUEST_ITEM_EXISTS("Requested item in Glacier:" 
                            + " - bucket=" + container
                            + " - key=" + key
                    );
                }
            } else {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Item not found:"
                            + " - bucket=" + container
                            + " - key=" + key
                );
            } 
            
            InputStream is = GetObject.getObjectSyncInputStream (s3Client, container, key);
            return is;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            if (ex.toString().contains("404")) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(ex.toString());
            }
            log4j.warn("awsGet Exception:" + ex
                        + " - bucket:" + container
                        + " - key:" + key, ex);
            //ex.printStackTrace();
            throw new TException(ex) ;
        }
    }
    
    public void awsRestore(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        
        
        try {
            response.set(container, key);
            log4j.info("awsRestore"
                    + " - container:" + container
                    + " - key:" + key
            );
            Properties objectProp = getObjectMeta(container, key);
            // object found
            if (objectProp.size() > 0 ) {
                log4j.info(PropertiesUtil.dumpProperties("awsRestore", objectProp));
                String msg = "";
                response.setFromProp(objectProp);
                String storageClass = objectProp.getProperty("storageClass");
                String ongoingRestore = objectProp.getProperty("ongoingRestore");
                String expirationS = objectProp.getProperty("expiration");
                // current content in GLACIER
                if ((storageClass != null) && storageClass.equals("GLACIER") && (expirationS == null)) {
                    //no previous restore
                    if (ongoingRestore == null) {
                        log4j.info("values:\n"
                            + " - bucket=" + container + "\n"
                            + " - key=" + key + "\n"
                        );
                        /*
                        RestoreObjectRequest requestRestore = new RestoreObjectRequest(container, key, 2);
                        s3Client.restoreObject(requestRestore);
                        */ 
//                        RestoreObjectRequest requestRestore = new RestoreObjectRequest(container, key, 2);
//                        s3Client.restoreObjectV2(requestRestore);
                        RestoreObject.RestoreStat restoreStat = RestoreObject.restoreS3Object(s3Client, container, key, expirationS);
                        switch(restoreStat) {
                            case start:
                                msg = "Requested item in Glacier - restore issued" ;
                                throw new TException.NEARLINE_RESTORE_IN_PROCESS(msg + ":" 
                                    + " - bucket=" + container
                                    + " - key=" + key
                                );
                                
                            case inprocess:
                                msg = "Requested item in Glacier - restore in process" ;
                                throw new TException.NEARLINE_RESTORE_IN_PROCESS(msg + ":" 
                                    + " - bucket=" + container
                                    + " - key=" + key
                                );
                                
                            case notfound:
                                msg = "Requested item in Glacier - not found" ;
                                throw new TException.REQUESTED_ITEM_NOT_FOUND("Item not found:"
                                    + " - bucket=" + container
                                    + " - key=" + key
                                );
                        }
                    }
                    
                // content found
                } else {
                    awsGet( container,key,outFile,response);
                }
                
            // no content found
            } else {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Item not found:"
                            + " - bucket=" + container
                            + " - key=" + key
                );
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex) ;
        }
    }
    
    /**
     * Convert from one AWS storage class to another
     * @param bucket AWS bucket
     * @param key AWS key
     * @param targetStorageClass resulting storage class for conversion
     * @param response Broad response information
     * @return true=complete
     * @throws TException 
     */
    public boolean convertStorageClass(
            String bucket,
            String key,
            StorageClass targetStorageClass,
            CloudResponse response)
        throws TException
    {
        response.set(bucket, key);
        response.setTargetStorageClassRSC(targetStorageClass.toString());;
        return awsConvertStorageClass(bucket, key, targetStorageClass, response);
    }
    
    /**
     * Convert from one AWS storage class to another
     * @param bucket AWS bucket
     * @param key AWS key
     * @param targetStorageClass resulting storage class for conversion
     * @param response Broad response information
     * @return true=complete
     * @throws TException 
     */
    public boolean awsConvertStorageClass(
            String bucket,
            String key,
            StorageClass targetStorageClass,
            CloudResponse response)
        throws TException
    {
        
        try {
            response.set(bucket, key);
            log4j.trace("awsRestore"
                    + " - container:" + bucket
                    + " - key:" + key
            );
            response.setTargetStorageClassRSC(targetStorageClass.toString());
            if (response.getTargetStorageClassRSC() == CloudResponse.ResponseStorageClass.offline) {
                throw new TException.INVALID_OR_MISSING_PARM("ConvertStorageClass: May not convert to  offline:" + targetStorageClass.toString());
            }
            CloudResponse inResponse = getCloudResponse(bucket, key);
            
            StorageClass inputStorageClass = StorageClass.fromValue(inResponse.getStorageClassString());
            if (inputStorageClass == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("ConvertStorageClass: Input component not found:" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                );
            }
            
            if (inResponse.getStorageClassType() == CloudResponse.ResponseStorageClass.offline) 
            {
                throw new TException.REQUEST_ITEM_EXISTS("ConvertStorageClass: Input component Glcacier - may not be converterd" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                            + " - inputStorageClass=" + inputStorageClass
                );
            }
            if (inputStorageClass == targetStorageClass) {
                log4j.trace("ConvertStorageClass: Input component same as target - not converted" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                );
                return false;
            }
            
            retryAWSConvertStorageClass(bucket, key, targetStorageClass);
            
            CloudResponse outResponse = getCloudResponse(bucket, key);
            if (outResponse == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("ConvertStorageClass: Output component not found:" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                );
            }
            StorageClass outputStorageClass = getStorageClass(bucket, key);
            for (int t=1; t<=5; t++) {
                if (outputStorageClass == targetStorageClass) break;
                String msg = "Warning retry(" + t + "):ConvertStorageClass: Conversion fails on StorageClass:" 
                        + " - bucket=" + bucket
                        + " - key=" + key;
                log4j.warn(msg);
                try {
                    Thread.sleep(t * 3000L);
                } catch (Exception sex) { }
                outputStorageClass = getStorageClass(bucket, key);
            }
            if (outputStorageClass != targetStorageClass) {
                throw new TException.INVALID_DATA_FORMAT("ConvertStorageClass: Conversion fails on StorageClass:" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                );
            }
            
            if (inResponse.getMd5().contains("-") || outResponse.getMd5().contains("-")) {
                String msg = "Large format:"
                    + " - bucket=" + bucket
                    + " - key=" + key
                    + " - in etag=" + inResponse.getMd5()
                    + " - out etag=" + outResponse.getMd5()
                    + " - in size=" + inResponse.getStorageSize()
                    + " - out size=" + outResponse.getStorageSize()
                    + " - in SC=" + inResponse.getStorageClassString()
                    + " - out SC=" + outResponse.getStorageClassString()
                    ;
                //logger.logMessage(msg, 0, true);
                //System.out.println(msg);
                return true;
            }
            if (!inResponse.getMd5().equals(outResponse.getMd5())) {
                throw new TException.INVALID_DATA_FORMAT("ConvertStorageClass: Conversion fails on etag:" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                            + " - in etag=" + inResponse.getMd5()
                            + " - out etag=" + outResponse.getMd5()
                            + " - in size=" + inResponse.getStorageSize()
                            + " - out size=" + outResponse.getStorageSize()
                );
            }
            if (inResponse.getStorageSize() != outResponse.getStorageSize()) {
                throw new TException.INVALID_DATA_FORMAT("ConvertStorageClass: Conversion fails on size:" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                            + " - in size=" + inResponse.getStorageSize()
                            + " - out size=" + outResponse.getStorageSize()
                );
            }
            return true;
            
        } catch (TException tex) {
            tex.printStackTrace();
            response.setException(tex);
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            response.setException(ex);
            throw new TException(ex) ;
        }
    }
    
    
    public void retryAWSConvertStorageClass(
            String bucket,
            String key,
            StorageClass targetStorageClass)
        throws Exception
    {
        Exception runEx = null;
        for (int t=1; t<=5; t++) {
            runEx = null;
            try {
                CopyObject.copyObject(s3AsyncClient, bucket, key, bucket, key, targetStorageClass);
                return;
                
            } catch (Exception ex) {
                runEx = ex;
                String msg = "Warning Copy retry(" + t + "):ConvertStorageClass: Conversion fails on CopyObjectRequest:" 
                    + " - bucket=" + bucket
                    + " - key=" + key
                    + " - ex=" + ex;
                log4j.warn(msg);
                try {
                    Thread.sleep(t * 3000L);
                } catch (Exception sex) { }
            }
        }
        throw runEx;
    }
    
    public CloudResponse convertStorageClass(
            String bucket,
            String key,
            StorageClass targetStorageClass)
        throws TException
    {
        CloudResponse response = new CloudResponse(bucket, key);
        boolean storageClassConverted = awsConvertStorageClass(bucket,key,targetStorageClass, response);
        response.setStorageClassConverted(storageClassConverted);
        return response;
    }
    
    public void restoreObject(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        try {
            awsRestore(container, key, outFile, response);
            
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
            //AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider()); 
            InputStream stream = GetObject.getObjectSyncInputStream (s3Client, bucketName, key);
            return stream;
            // Process the objectData stream.
            //objectData.close();
            
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
        return getObject(
            bucketName,
            key,
            response);
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
            awsGet(container, key, outFile, response);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
    }
    
    @Override
    public Properties getObjectMeta (
            String bucketName,
            String key)
    {
        return getRetryObjectMeta (bucketName,key, 10);
    }
    
    public Properties getRetryObjectMeta (
            String bucketName,
            String key,
            int retryCnt)
    {
        
        
        for (int retry=1; retry<=retryCnt; retry++) {
            Properties prop = GetObjectMeta.getObjectMeta(s3Client, bucketName, key);
            if (prop != null) {
                return prop;
            } else {
                if (retry < retryCnt) {
                    try {
                        Thread.sleep(500 * retry);
                    } catch (Exception tex) { }
                }
            }
        }
        
        return null;
    }
  
    public StorageClass getStorageClass (
            String bucketName,
            String key)
        throws TException
    {
        return GetObjectMeta.getStorageClass(s3Client, bucketName, key);
    }
  
    public CloudResponse getCloudResponse (
            String bucketName,
            String key)
        throws TException
    {
        
        Properties prop = GetObjectMeta.getObjectMeta(s3Client, bucketName, key);
        if ((prop == null) || prop.isEmpty()) {
            return null;
        }
        CloudResponse inResponse = CloudResponse.get(prop);
        return inResponse;
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

    /**
     * List entries
     * @param bucket - non AWS bucket name
     * @param listPrefix what to truncate on as if aws prefix not there
     * @param maxEntries maximum entries to return
     * @param response filled CloudResponse
     * @throws TException 
     */
    protected void awsListPrefix (
            String bucket,
            String listPrefix,
            int maxEntries,
            CloudResponse response)
        throws TException
    {
        try {
            int entryCnt = 0;
            log4j.trace("awsList:\n"
                    + "bucket:" + bucket + "\n"
                    + "listPrefix:" + listPrefix + "\n"
                    + "bucket:" + bucket + "\n"
            );
            GetObjectList.awsListPrefix(s3Client, bucket, listPrefix, maxEntries, response);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        
    }
    

    public void awsListAfter(
            String bucketName,
            String startAfter, 
            int maxKeys,
            CloudResponse response) 
        throws TException 
    {
        try {
            GetObjectList.awsListAfter(s3Client, bucketName, startAfter, maxKeys, response);
            
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
//    @Override
    public CloudResponse getObjectList (
            String bucketName,
            Identifier objectID,
            Integer versionID,
            String fileID)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = CloudResponse.get(bucketName, objectID, versionID, fileID);
            String key = CloudUtil.getKey(objectID, versionID, fileID, ALPHANUMERIC);
            awsListPrefix(bucketName, key, -1,response);
            return response;
            
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
            response = new CloudResponse();
            CloudResponse.get(bucketName, key);
            awsListPrefix(bucketName, key, -1,response);
            return response;
            
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
            response = new CloudResponse(bucketName, key);
            response.setStorageKey(key);
            
            awsListPrefix(bucketName, key, limit,response);
            return response;
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    public CloudResponse getObjectListAfter (
            String bucketName,
            String afterKey,
            int limit)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(bucketName, afterKey);
            response.setStorageKey(afterKey);
            
            awsListAfter(bucketName, afterKey, limit, response);
            return response;
            
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
            response = new CloudResponse();
            response.setBucketName(bucketName);
            awsListPrefix(bucketName, "", -1,response);
            return response;
            
        } catch (Exception ex) {
            handleException(response, ex);
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
            stateHandler = StateHandler.getStateHandler(this, bucketName, logger);
            return stateHandler.process();
            
        } catch (Exception ex) {
            return StateHandler.getError(bucketName, NAME, "Error getState:" + ex);
        }
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(StorageClass storageClass) {
        this.storageClass = storageClass;
    }

    public void setStorageClass(String storageClassS) {
        StorageClass storageClass = StorageClass.valueOf(storageClassS);
        this.storageClass = storageClass;
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
                log4j.warn("awsHandleException", exception);
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
    
    @Override    
    public Boolean isAlive(String bucketName)
    {
        if (endPoint != null) {
            return isAliveTest(endPoint);
        }
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
            Properties metaProp = getObjectMeta(bucketName, key);
            if (metaProp.size() == 0) {
                TException tex = new TException.REQUESTED_ITEM_NOT_FOUND("Item not found "
                        + " - bucket:" + bucketName
                        + " - key:" + key
                    );
                response.setException(tex);
                response.setHttpStatus(tex.getHTTPResponse());
                response.setReturnURL(null);
                response.setStatus(CloudResponse.ResponseStatus.missing);
                return response;
                
            } else {
                response.setFromProp(metaProp);
                String storageClass = metaProp.getProperty("storageClass");
                String expirationS = metaProp.getProperty("expiration");
                if ((storageClass != null) && storageClass.equals("GLACIER") && (expirationS == null)) {
                    TException tex = new TException.REQUEST_ITEM_EXISTS("Requested item in Glacier:" 
                            + " - bucket=" + bucketName
                            + " - key=" + key);
                    response.setException(tex);
                    response.setHttpStatus(tex.getHTTPResponse());
                    response.setReturnURL(null);
                    response.setStatus(CloudResponse.ResponseStatus.dark);
                    return response;
                }
                
            }
            // Set the presigned URL to expire after one hour.
            java.util.Date expiration = new java.util.Date();
            long expTimeMillis = expiration.getTime();
            expTimeMillis = 1000 * 60 * expirationMinutes;
            expiration.setTime(expTimeMillis);
            System.out.println("***getPreSigned"
                    + " - bucketName:" + bucketName
                    + " - key:" + key
                    + " - expTimeMillis:" + expTimeMillis
                    + " - contentType:" + contentType
                    + " - contentDisp:" + contentDisp
            );
            String urlS = GetObjectPresign.getObjectPresign(s3Presigner, bucketName, key, expTimeMillis, contentType, contentDisp);
            System.out.println("***getPreSigned"
                    + " - urlS:" + urlS
            );
            
            //if (urlS.startsWith("http:") && (s3Type == S3Type.aws)) {
            if (false && urlS.startsWith("http:")) {
                urlS = "https" + urlS.substring(4);
                log4j.trace("Presign http to https:"
                            + " - s3Type:" + s3Type
                            + " - url:" + urlS
                    );
            }
            URL url = new URL(urlS);
            response.setReturnURL(url);
            response.setStatus(CloudResponse.ResponseStatus.ok);
            
        } catch (Exception ex) {
            System.out.println("getPreSigned exception:" +ex);
            ex.printStackTrace();
            String exc = "Exception bucketName:" + bucketName + " - key=" + key;
            if (ex.toString().contains("404")) {
                Exception returnException = new TException.REQUESTED_ITEM_NOT_FOUND(exc);
                response.setException(returnException);
            } else {
                response.setException(ex);
            }
            response.setReturnURL(null);
            response.setStatus(CloudResponse.ResponseStatus.fail);
        }
        return response;
    }

                
    public InputStream getRangeStream(
            String bucketName,
            String key,
            long start,
            long stop,
            CloudResponse response)
        throws TException
    {
        try {
            response.set(bucketName, key);
            //AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider()); 
            InputStream inputStream = GetObjectRange.getObjectRange(s3Client, bucketName, key, start, start);
            return inputStream;
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }  
    
    @Override    
    public CloudStoreInf.CloudAPI getType()
    {
        return CloudStoreInf.CloudAPI.AWS_S3;
    }

    public LoggerInf getLogger() {
        return logger;
    }

    public S3Type getS3Type() {
        return s3Type;
    }

    public void setS3Type(S3Type s3Type) {
        this.s3Type = s3Type;
    }
    
}

