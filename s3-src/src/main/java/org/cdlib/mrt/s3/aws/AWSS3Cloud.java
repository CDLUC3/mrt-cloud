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
package org.cdlib.mrt.s3.aws;
//import org.cdlib.mrt.s3.service.*;



import org.cdlib.mrt.s3.service.*;

import org.cdlib.mrt.core.Identifier;
import java.io.File;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.net.URL;
import java.util.List;


import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudStoreAbs;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.DeleteOnCloseFileInputStream;

import com.amazonaws.HttpMethod;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedBackoffStrategies.FullJitterBackoffStrategy;
import com.amazonaws.retry.RetryPolicy;

import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;

import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.transfer.Transfer;
//import static org.cdlib.mrt.s3test.tasks.d190716_partial_result.UploadProgress.eraseProgressBar;
//import static org.cdlib.mrt.s3test.tasks.d190716_partial_result.UploadProgress.printProgressBar;

/**
 * Specific SDSC Storage Cloud handling
 * @author dloy
 */
public class AWSS3Cloud
    extends CloudStoreAbs
    implements CloudStoreInf
{
    private static final boolean DEBUG = false;
    private static final boolean ALPHANUMERIC = false;
    protected static final String NAME = "AWSS3Cloud";
    protected static final String MESSAGE = NAME + ": ";
    
    public enum S3Type {aws, minio, wasabi};
    
    private AmazonS3Client s3Client = null;
    private StorageClass storageClass = null;
    private String endPoint = null;
    private S3Type s3Type = S3Type.aws;

    public static AWSS3Cloud getAWSS3Region(
            String storageClass,
            String regionS,
            LoggerInf logger)
        throws TException
    {
        Regions region = Regions.fromName(regionS);
        AWSS3Cloud cloud =  new AWSS3Cloud(region, logger);
        if (storageClass != null) {
            cloud.setStorageClass(storageClass);
        }
        return cloud;
    }
    
    // Depricated older storage method
    public static AWSS3Cloud getAWSS3(
            String storageClass,
            LoggerInf logger)
        throws TException
    {
        Regions region = Regions.US_WEST_2;
        AWSS3Cloud cloud =  new AWSS3Cloud(region, logger);
        if (storageClass != null) {
            cloud.setStorageClass(storageClass);
        }
        return cloud;
    }
     
    public static AWSS3Cloud getMinio(
            String accessKey,
            String secretKey, 
            String endPoint, 
            LoggerInf logger)
        throws TException
    {
        
        if (DEBUG) System.out.println("getMinio:"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
                + " - endPoint=" + endPoint
        );
        AmazonS3Client s3Client = amazonS3Client(
            accessKey,
            secretKey, 
            endPoint,
            null);
        AWSS3Cloud cloud =  new AWSS3Cloud(s3Client, endPoint, logger);
        cloud.setS3Type(S3Type.minio);
        return cloud;
    }
    
    
    public static AWSS3Cloud getWasabi(
            String accessKey,
            String secretKey, 
            String endPoint, 
            String regionName,
            LoggerInf logger)
        throws TException
    {
        
        if (DEBUG) System.out.println("getWasabi:"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
                + " - endPoint=" + endPoint
                + " - regionName=" + regionName
        );
        
        Regions region = Regions.fromName(regionName);
        AmazonS3Client s3Client = amazonS3Client(
            accessKey,
            secretKey, 
            endPoint,
            region);
        AWSS3Cloud cloud =  new AWSS3Cloud(s3Client, endPoint, logger);
        cloud.setS3Type(S3Type.wasabi);
        return cloud;
    }
    
    protected AWSS3Cloud(
            AmazonS3Client s3Client,
            String endPoint,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.endPoint = endPoint;
        this.s3Client =  s3Client;
    } 
    
    public static AWSS3Cloud getAWSS3(
            LoggerInf logger)
        throws TException
    {
        Regions region = Regions.US_WEST_2;
        return new AWSS3Cloud(region, logger);
    }
    
    protected AWSS3Cloud(
            Regions region,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        s3Client =  amazonS3ClientDefault(region);
    }
    
    public static AWSS3Cloud getDefault(
            String endPoint,
            LoggerInf logger)
        throws TException
    {
        return new AWSS3Cloud(endPoint, logger);
    }
    
    protected AWSS3Cloud( 
            String endPoint,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        s3Client =  amazonS3ClientDefault(endPoint);
    }   
    
    public TransferManager getTransferManager() 
    {
        TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        return transferManager;
    }
    
    public static AmazonS3Client amazonS3Client(
            String accessKey,
            String secretKey, 
            String endPoint,
            Regions regions) 
    {
        if (DEBUG) System.out.println("amazonS3Client:"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
                + " - endPoint=" + endPoint
        );
        if (regions == null) {
            regions = Regions.DEFAULT_REGION;
        }
        //********************************
        //AWSCredentials credentials = new BasicAWSCredentials("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY");
        AWSCredentials credentials =
                new BasicAWSCredentials(
                        accessKey,
                        secretKey);
        
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");
        clientConfiguration.withMaxErrorRetry (15)
            .withRetryPolicy(getS3BaseRetryPolicy())
            .withConnectionTimeout (43200_000)
            .withSocketTimeout (43200_000)
            .withTcpKeepAlive (true);
        clientConfiguration.setUseThrottleRetries(true);

        AmazonS3Client s3Client = (AmazonS3Client)AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(
                        endPoint,
                        regions.getName()))
        
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
        return s3Client;
    }
    
    
    public static AmazonS3Client amazonS3ClientDefault(Regions region) {

        ClientConfiguration clientConfig = new ClientConfiguration()
            .withMaxErrorRetry (15)
            .withRetryPolicy(getS3BaseRetryPolicy())
            //.withConnectionTimeout (10_000)
            //.withSocketTimeout (10_000)
            .withConnectionTimeout (600_000)
            .withSocketTimeout (600_000)
            .withTcpKeepAlive (true);
        clientConfig.setUseThrottleRetries(true);
        clientConfig.setProtocol(Protocol.HTTP);
        InstanceProfileCredentialsProvider credentialProvider 
                = InstanceProfileCredentialsProvider.getInstance();
        AmazonS3Client s3client = (AmazonS3Client) AmazonS3ClientBuilder.standard()
//                .withRegion("us-west-2")
                .withRegion(region)
                .withClientConfiguration(clientConfig)
                .withCredentials(credentialProvider)
                .build();
        
        return s3client;
    }    
    private static RetryPolicy getS3BaseRetryPolicy() {
        return new RetryPolicy(
                new PredefinedRetryPolicies.SDKDefaultRetryCondition(),
                new FullJitterBackoffStrategy(500, 120000),
                30,
                true
        );
    }
    public static AmazonS3Client amazonS3ClientDefault( 
            String endPoint) 
    {

        ClientConfiguration clientConfig = new ClientConfiguration()
            .withMaxErrorRetry (15)
            .withRetryPolicy(getS3BaseRetryPolicy())
            //.withConnectionTimeout (10_000)
            //.withSocketTimeout (10_000)
            .withConnectionTimeout (600_000)
            .withSocketTimeout (600_000)
            .withTcpKeepAlive (true);
        clientConfig.setUseThrottleRetries(true);
        clientConfig.setProtocol(Protocol.HTTP);
        InstanceProfileCredentialsProvider credentialProvider 
                = InstanceProfileCredentialsProvider.getInstance();
        AmazonS3Client s3client = (AmazonS3Client) AmazonS3ClientBuilder.standard()
                .withRegion(endPoint)
                .withClientConfiguration(clientConfig)
                .withCredentials(credentialProvider)
                .build();
        
        return s3client;
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
            if (DEBUG)System.out.println("FILE " 
                    + " - size:" + inputFile.length()
                    + " - bucket:" + bucketName
                    + " - key:" + key
            );
            Properties objectMeta = getObjectMeta(bucketName, key);
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
                    if (DEBUG) System.out.println("***File match:"
                            + " - bucket:" + bucketName
                            + " - key:" + key
                            + " - sha256: "+ fileSha256
                    );
                    //response.setFileMeta(cloudProp);
                    //System.out.println(PropertiesUtil.dumpProperties("&&&&&&skip", cloudProp));
                    response.setFromProp(objectMeta);
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
            
            ObjectMetadata om = new ObjectMetadata();
            om.addUserMetadata("sha256", fileSha256);
            PutObjectRequest putObjectRequest = new PutObjectRequest(
            		                 bucketName, key, inputFile)
                    .withMetadata(om);
            
            if (storageClass != null ) {
                putObjectRequest.withStorageClass(storageClass);
            }

            TransferManager tm = getTransferManager();
            Upload upload = tm.upload(putObjectRequest);
            try {
                long fileLen = inputFile.length();
                if (fileLen > 10_000_000_000L) {
                    showTransferProgress(upload, key,1800, fileLen);
                }
                upload.waitForCompletion();
            
            } catch(InterruptedException ex) {
                throw new TException.REMOTE_IO_SERVICE_EXCEPTION("AWS service exception:" + ex
                            + " - bucket:" + bucketName
                            + " - key:" + key
                );
            } catch( com.amazonaws.services.s3.model.AmazonS3Exception s3ex) {
                if (s3ex.toString().contains("upload may have been aborted or completed")) {
                    System.out.println(MESSAGE + "S3 upload error - continue processing:" + key);
                }
            } finally {
                tm.shutdownNow(false);
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
    
    public CloudResponse awsDelete (
            CloudResponse response)
        throws TException
    {
        try {
            String bucket = response.getBucketName();
            String key = response.getStorageKey();
            s3Client.deleteObject(new DeleteObjectRequest(bucket, key));
            
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
    
    public void setResponse(ObjectMetadata metadata, CloudResponse response)
        throws TException
    {
        String contentType = metadata.getContentType();
        String eTag = metadata.getETag();
        long length = metadata.getContentLength();
        response.setMd5(eTag);
        response.setStorageSize(length);
        response.setMimeType(contentType);
        
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
            if (DEBUG) System.out.println("awsGet"
                    + " - container:" + container
                    + " - key:" + key
            );
            Properties objectProp = getObjectMeta(container, key);
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
            
            // Minio has repeatedly failed using multipart TransferManager
            // This change uses single stream only when minio
            if (s3Type == S3Type.minio) {
                InputStream is = getObjectStreaming(container, key, response);
                if (response.getException() != null) {
                    throw response.getException();
                }
                FileUtil.stream2File(is, outFile);
                if (DEBUG) System.out.println("Minio file built(" + container + "):"  + outFile.getCanonicalPath());
                
            } else {
            ////////
            //TransferManager tm = new TransferManager();  
                TransferManager tm = getTransferManager();
                if (DEBUG) System.out.println("Start");
                GetObjectRequest gor = new GetObjectRequest(container, key);
                Download download = tm.download(gor, outFile, 86400000L);
                try {
                    download.waitForCompletion();
                } catch(InterruptedException ex) {
                    System.out.println("InterruptedException:" + ex.getMessage());
                } finally {
                    tm.shutdownNow(false);
                }
                if (DEBUG) System.out.print("Non-Minio file built(" + container + "):" + outFile.getCanonicalPath());
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
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
            System.out.println("awsRestore"
                    + " - container:" + container
                    + " - key:" + key
            );
            Properties objectProp = getObjectMeta(container, key);
            // object found
            if (objectProp.size() > 0 ) {
                System.out.println(PropertiesUtil.dumpProperties("awsRestore", objectProp));
                String msg = "";
                response.setFromProp(objectProp);
                String storageClass = objectProp.getProperty("storageClass");
                String ongoingRestore = objectProp.getProperty("ongoingRestore");
                String expirationS = objectProp.getProperty("expiration");
                // current content in GLACIER
                if ((storageClass != null) && storageClass.equals("GLACIER") && (expirationS == null)) {
                    //no previous restore
                    if (ongoingRestore == null) {
                        System.out.println("values:\n"
                            + " - bucket=" + container + "\n"
                            + " - key=" + key + "\n"
                        );
                        /*
                        RestoreObjectRequest requestRestore = new RestoreObjectRequest(container, key, 2);
                        s3Client.restoreObject(requestRestore);
                        */ 
                        RestoreObjectRequest requestRestore = new RestoreObjectRequest(container, key, 2);
                        s3Client.restoreObjectV2(requestRestore);
                        //s3Client.restoreObject(container, key, 2);
                        msg = "Requested item in Glacier - restore issued" ;
                        
                    // restore processing
                    } else {
                        msg = "Requested item in Glacier - restore in process" ;
                    }
                    throw new TException.NEARLINE_RESTORE_IN_PROCESS(msg + ":" 
                            + " - bucket=" + container
                            + " - key=" + key
                    );
                    
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
    
    public boolean awsRestore(
            String container,
            String key,
            CloudResponse response)
        throws TException
    {
        
        try {
            response.set(container, key);
            if (DEBUG) System.out.println("awsRestore"
                    + " - container:" + container
                    + " - key:" + key
            );
            Properties objectProp = getObjectMeta(container, key);
            // object found
            if (objectProp.size() > 0 ) {
                logger.logMessage(PropertiesUtil.dumpProperties("awsRestore", objectProp), 10,true);
                String msg = "";
                response.setFromProp(objectProp);
                String storageClass = objectProp.getProperty("storageClass");
                String ongoingRestore = objectProp.getProperty("ongoingRestore");
                String expirationS = objectProp.getProperty("expiration");
                // current content in GLACIER
                if ((storageClass != null) && storageClass.equals("GLACIER") && (expirationS == null)) {
                    //no previous restore
                    if (ongoingRestore == null) {
                        if (DEBUG) System.out.println("values:\n"
                            + " - bucket=" + container + "\n"
                            + " - key=" + key + "\n"
                        );
                        s3Client.restoreObject(container, key, 2);
                        msg = "Requested item in Glacier - restore issued" ;
                        
                    // restore processing
                    } else {
                        msg = "Requested item in Glacier - restore in process" ;
                    }
                    logger.logMessage("awsRestore:(" + key + "):"+ msg, 5,true);
                    return false;
                    
                                // content found
                } else {
                    msg = "no restore needed";
                    logger.logMessage("awsRestore:(" + key + "):"+ msg, 5,true);
                    return true;
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
    public boolean awsConvertStorageClass(
            String bucket,
            String key,
            StorageClass targetStorageClass,
            CloudResponse response)
        throws TException
    {
        
        try {
            response.set(bucket, key);
            if (DEBUG) System.out.println("awsRestore"
                    + " - container:" + bucket
                    + " - key:" + key
            );
            response.setTargetStorageClass(targetStorageClass);
            if (targetStorageClass == StorageClass.Glacier) {
                throw new TException.INVALID_OR_MISSING_PARM("ConvertStorageClass: May not convert to  Glacier:");
            }
            CloudResponse inResponse = getConvertInfo(bucket, key);
            if (inResponse == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("ConvertStorageClass: Input component not found:" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                );
            }
            StorageClass inputStorageClass = inResponse.getInputStorageClass();
            response.setInputStorageClass(inputStorageClass);
            if (inputStorageClass == StorageClass.Glacier) {
                throw new TException.REQUEST_ITEM_EXISTS("ConvertStorageClass: Input component Glcacier - may not be converterd" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                            + " - inputStorageClass=" + inputStorageClass
                );
            }
            if (inputStorageClass == targetStorageClass) {
                if (DEBUG) System.out.println("ConvertStorageClass: Input component same as target - not converted" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                );
                return false;
            }
            
            retryAWSConvertStorageClass(bucket, key, targetStorageClass);
            
            CloudResponse outResponse = getConvertInfo(bucket, key);
            if (outResponse == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("ConvertStorageClass: Output component not found:" 
                            + " - bucket=" + bucket
                            + " - key=" + key
                );
            }
            StorageClass outputStorageClass = outResponse.getInputStorageClass();
            for (int t=1; t<=5; t++) {
                if (outputStorageClass == targetStorageClass) break;
                String msg = "Warning retry(" + t + "):ConvertStorageClass: Conversion fails on StorageClass:" 
                        + " - bucket=" + bucket
                        + " - key=" + key;
                System.out.println(msg);
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
                    + " - in SC=" + inResponse.getInputStorageClass()
                    + " - out SC=" + outResponse.getInputStorageClass()
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
                CopyObjectRequest copyObjectRequest= 
                    new CopyObjectRequest(bucket, key, bucket, key)
                    .withStorageClass(targetStorageClass);
                s3Client.copyObject(copyObjectRequest);
                return;
                
            } catch (Exception ex) {
                runEx = ex;
                String msg = "Warning Copy retry(" + t + "):ConvertStorageClass: Conversion fails on CopyObjectRequest:" 
                    + " - bucket=" + bucket
                    + " - key=" + key
                    + " - ex=" + ex;
                System.out.println(msg);
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
            File tmpFile = FileUtil.getTempFile("clouttemp", ".txt");
            awsGet(bucketName, key, tmpFile, response);
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
        try {
            response.set(bucketName, key);
            //AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());        
            S3Object object = s3Client.getObject(
                  new GetObjectRequest(bucketName, key));
            return object.getObjectContent();
            // Process the objectData stream.
            //objectData.close();
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
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
        throws TException
    {
        Properties prop = new Properties();
        try {
            ObjectMetadata metadata = getRetryObjectMeta (bucketName,key, 10);
            /*
            GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucketName, key);
            ObjectMetadata metadata = s3Client.getObjectMetadata(request);
            */
            addProp(prop, "sha256", metadata.getUserMetaDataOf("sha256"));
            addProp(prop, "size", "" + metadata.getContentLength());
            addProp(prop, "bucket", bucketName);
            addProp(prop, "key", key);
            addProp(prop, "etag", metadata.getETag());
            Date date = metadata.getLastModified();
            String isoDate = DateUtil.getIsoDate(date);
            addProp(prop, "modified", isoDate);
            addProp(prop, "md5", metadata.getContentMD5());
            addProp(prop, "storageClass", metadata.getStorageClass());
            addProp(prop, "maxErrRetry", "" + getMaxErrRetry());
            Date expireDate = metadata.getExpirationTime();
            if (expireDate != null) {
                Long expireDateL = expireDate.getTime();
                addProp(prop, "expires", "" + expireDateL);
                Long current = new Date().getTime();
                if (current < expireDateL) {
                    addProp(prop, "expRemain", "" + (expireDateL - current));
                }
            }
            Boolean restoreFlag = metadata.getOngoingRestore();
            if ((restoreFlag != null) && restoreFlag) {
                addProp(prop, "ongoingRestore", "true");
            }
            Date expiration = metadata.getRestoreExpirationTime();
            //if (expiration == null) System.out.println("expiration is null");
            if (expiration != null) {
                addProp(prop, "expiration", "" + expiration.getTime());
            }
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("getObjectMeta", prop));
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
    
    public ObjectMetadata getRetryObjectMeta (
            String bucketName,
            String key,
            int retryCnt)
        throws Exception
    {
        
        Exception doException = null;
        for (int retry=0; retry<retryCnt; retry++) {
            try {
                GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucketName, key);
                ObjectMetadata metadata = s3Client.getObjectMetadata(request);
                return metadata;

            } catch (Exception ex) {
                if (ex.toString().contains("404")) {
                    if (DEBUG) System.out.println("404(" + retry + "):"+ ex);
                    throw ex;
                }
                String msg = "***getRetryObjectMeta Exception(" + retry + "):"
                    + " - bucketName:" + bucketName
                    + " - key:" + key
                    + " - ex:" + ex;
                if (DEBUG) System.out.println(msg);
                logger.logMessage(msg,
                    1, true
                );
                doException = ex;
                try {
                    Thread.sleep(500 * retry);
                } catch (Exception tex) { }
            }
        }
        
        throw doException;
    }
    
    public Integer getMaxErrRetry() 
    {
        try {
            ClientConfiguration config = s3Client.getClientConfiguration();
            RetryPolicy retryPolicy = config.getRetryPolicy();
            RetryPolicy.BackoffStrategy backoffStrategy = retryPolicy.getBackoffStrategy();
            return retryPolicy.getMaxErrorRetry();
        } catch (Exception ex) {
            return 0;
        }
    }
    
    public void dumpObjectMetadata (
           ObjectMetadata objectMetadata)
        throws Exception
    {
        int maxErrRetry = getMaxErrRetry();
        System.out.println("maxErrRetry:" + maxErrRetry);
        Map<String, Object> map = objectMetadata.getRawMetadata();
        
        Set<String> metaKeys = map.keySet();
        for (String key : metaKeys) {
            try {
                Object value  = map.get(key);
                if (value instanceof String) {
                    System.out.println("metaKey:" + key + "=" + (String)value);
                    
                } else if (value instanceof Integer) {
                    System.out.println("metaKey:" + key + "=" + (Integer)value);
                    
                } else {    
                    System.out.println("metaKey:" + key);
                }

            } catch (Exception ex) {
                System.out.println(ex);
            }
        }
        
        Map<String, String> mapUser = objectMetadata.getUserMetadata();
        Set<String> userKeys = mapUser.keySet();
        for (String userKey : userKeys) {
            try {
                String value = mapUser.get(userKey);
                System.out.println("mapUser:" + userKey + "=" + value);

            } catch (Exception ex) {
                System.out.println(ex);
            }
        }
        System.out.println("getPartCount:" + objectMetadata.getPartCount());
    }
    
    public Properties dumpMeta (
            String bucketName,
            String key)
        throws TException
    {
        Properties prop = new Properties();
        try {
            GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucketName, key);
            ObjectMetadata metadata = s3Client.getObjectMetadata(request);
            Map<String, String> userMeta = metadata.getUserMetadata();
            Set<String> userKeys = userMeta.keySet();
            for (String userKey : userKeys) {
                addProp(prop, "user|" + userKey, metadata.getUserMetaDataOf(userKey));
                System.out.println("addProp:"
                        + " - userKey:" + userKey
                        + " - user value:" + metadata.getUserMetaDataOf(userKey)
                );
            }
            Map<String, Object> rawMeta = metadata.getRawMetadata();
            Set<String> rawKeys = rawMeta.keySet();
            for (String rawKey : rawKeys) {
                Object rawData = rawMeta.get(rawKey);
                String rawOut="";
                if (rawData instanceof String) {
                    rawOut = (String)rawData;
                    addProp(prop, "raw|" + rawKey, rawOut);
                    System.out.println("addProp:"
                            + " - rawKey:" + rawKey
                            + " - rawOut:" + rawOut
                    );
                }
            }
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("getObjectMeta", prop));
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
  
    public StorageClass getStorageClass (
            String bucketName,
            String key)
        throws TException
    {
        StorageClass storageClass = null;
        try {
            GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucketName, key);
            ObjectMetadata metadata = s3Client.getObjectMetadata(request);
            String storageClassS = metadata.getStorageClass();
            if (storageClassS == null) {
                storageClass = StorageClass.Standard;
            } else {
                storageClass = StorageClass.fromValue(storageClassS);
            }
            return storageClass;
            
        } catch (Exception ex) {
            return null;
        }
    }
  
    public CloudResponse getConvertInfo (
            String bucketName,
            String key)
        throws TException
    {
        StorageClass storageClass = null;
        try {
            CloudResponse response = new CloudResponse(bucketName, key);
            GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucketName, key);
            ObjectMetadata metadata = s3Client.getObjectMetadata(request);
            String storageClassS = metadata.getStorageClass();
            if (storageClassS == null) {
                storageClass = StorageClass.Standard;
            } else {
                storageClass = StorageClass.fromValue(storageClassS);
            }
            response.setInputStorageClass(storageClass);
            response.setMd5(metadata.getETag());
            response.setStorageSize(metadata.getContentLength());
            if (DEBUG) {
                System.out.println("ConvertInfo:"
                        + " - bucket:" + response.getBucketName()
                        + " - key:" + response.getStorageKey()
                        + " - storageClass:" + response.getInputStorageClass()
                        + " - Md5:" + response.getMd5()
                        + " - size:" + response.getStorageSize()
                );
            }
            return response;
            
        } catch (Exception ex) {
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
            if (DEBUG) System.out.println("awsList:\n"
                    + "bucket:" + bucket + "\n"
                    + "listPrefix:" + listPrefix + "\n"
                    + "bucket:" + bucket + "\n"
            );
            ObjectListing list = s3Client.listObjects( bucket, listPrefix);

            do {
                List<S3ObjectSummary> summaries = list.getObjectSummaries();
                //System.out.println("sum size:" + summaries.size());
                for (S3ObjectSummary summary : summaries) {
                    entryCnt++;
                    if (maxEntries <= 0) {}
                    else if (entryCnt >= maxEntries) return;
                    response.addObject(setSummary(summary));
                }
                list = s3Client.listNextBatchOfObjects(list);

            } while (list.isTruncated());
            
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

            //System.out.println("Listing objects");

            // maxKeys is set to 2 to demonstrate the use of
            // ListObjectsV2Result.getNextContinuationToken()
            ListObjectsV2Request req = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withStartAfter(startAfter)
                    .withMaxKeys(maxKeys)
                    ;
            ListObjectsV2Result result;
            int cnt = 0;
            doBreak:
            do {
                result = s3Client.listObjectsV2(req);
                for (S3ObjectSummary summary : result.getObjectSummaries()) {
                    response.addObject(setSummary(summary));
                    cnt++;
                    if (cnt >= maxKeys) break doBreak;
                    //System.out.printf(" - %s (size: %d)\n", summary.getKey(), summary.getSize());
                }
                String token = result.getNextContinuationToken();
                if (DEBUG) System.out.println("Next Continuation Token: " + token + " - cnt=" + cnt);
                req.setContinuationToken(token);
            } while (result.isTruncated());
            
            
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process 
            // it, so it returned an error response.
            throw new TException(e);
            
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
    
    
    public CloudList.CloudEntry setSummary(S3ObjectSummary summary) 
    {
        CloudList.CloudEntry entry = new CloudList.CloudEntry();
        entry.setEtag(summary.getETag());
        entry.setContainer(summary.getBucketName());
        entry.setSize(summary.getSize());
        DateState dateModified = new DateState(summary.getLastModified());
        entry.lastModified = dateModified.getIsoDate();
        entry.setStorageClass(summary.getStorageClass());
        String key = summary.getKey();
        entry.setKey(key);
        return entry;
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
    
    @Override    
    public Boolean isAlive(String bucketName)
    {
        if (endPoint != null) {
            return isAliveTest(endPoint);
        }
        return null;
    }
    
    public void displayResourceUrl (
            String bucketName,
            String key)
        throws TException
    {
        String resourceUrl = s3Client.getResourceUrl(bucketName, key);
        System.out.println("RESOURCEURL:" +  resourceUrl);
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
            expTimeMillis += 1000 * 60 * expirationMinutes;
            expiration.setTime(expTimeMillis);
            
            GeneratePresignedUrlRequest generatePresignedUrlRequest =
                new GeneratePresignedUrlRequest(bucketName, key)
                            .withMethod(HttpMethod.GET)
                            .withExpiration(expiration);
            if (!StringUtil.isAllBlank(contentType) || !StringUtil.isAllBlank(contentDisp)) {
                ResponseHeaderOverrides headerOverrides = new ResponseHeaderOverrides();
                if (!StringUtil.isAllBlank(contentType)) {
                    headerOverrides.setContentType(contentType);
                }
                if (!StringUtil.isAllBlank(contentDisp)) {
                    headerOverrides.setContentDisposition(contentDisp);
                }
                generatePresignedUrlRequest.setResponseHeaders(headerOverrides);
            }
            
            URL url = s3Client.generatePresignedUrl(generatePresignedUrlRequest);
            String urlS = url.toString();
            if (urlS.startsWith("http:") && (s3Type == S3Type.aws)) {
                urlS = "https" + urlS.substring(4);
                if (DEBUG) {
                    System.out.println("Presign http to https:"
                            + " - s3Type:" + s3Type
                            + " - url:" + urlS
                    );
                }
            }
            url = new URL(urlS);
            response.setReturnURL(url);
            response.setStatus(CloudResponse.ResponseStatus.ok);
            
        } catch (Exception ex) {
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
            GetObjectRequest getObjectRequest =
                    new GetObjectRequest(bucketName, key);
            getObjectRequest.setRange(start, stop);
            S3Object object = s3Client.getObject(getObjectRequest);
            return object.getObjectContent();
            // Process the objectData stream.
            //objectData.close();
            
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

    public AmazonS3Client getS3Client() {
        return s3Client;
    }

    public LoggerInf getLogger() {
        return logger;
    }

    public void setS3Client(AmazonS3Client s3Client) {
        this.s3Client = s3Client;
    }

    // Prints progress while waiting for the transfer to finish.
    public void showTransferProgress(Transfer xfer, String key, long inSleepSec, long fileLen)
    {
        long sleepTime = inSleepSec * 1000;
        // print the transfer's human-readable description
        if (DEBUG) System.out.println("showTransferProgress:" + key);
        if (DEBUG) System.out.println(xfer.getDescription());
        long remaining = fileLen;
        long saveProgress = 0;
        if (false || logger.getMessageMaxLevel() < 1) {
            logger.logMessage("showTransferProgress not used for " + key, 0, true);
            return;
        }
        logger.logMessage("showTransferProgress used for " + key 
                + " - inSleepSec:" + inSleepSec 
                + " - getMessageMaxLevel:" + logger.getMessageMaxLevel()
                , 0, true);
        do {
            
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                return;
            }
            // Note: so_far and total aren't used, they're just for
            // documentation purposes.
            TransferProgress progress = xfer.getProgress();
            long so_far = progress.getBytesTransferred();
            long total = progress.getTotalBytesToTransfer();
            double pct = progress.getPercentTransferred();
            logger.logMessage("key:" + key
                    + " - so_far:" + so_far
                    + " - sleep:" + sleepTime
                    + " - pct:" + pct, 
                    0, true
            );
            remaining = fileLen - so_far;
            if (remaining  < (so_far - saveProgress)) {
                sleepTime = 180000;
            }
            saveProgress = so_far;
        } while (xfer.isDone() == false);
        // print the final state of the transfer.
        Transfer.TransferState xfer_state = xfer.getState();
        logger.logMessage("showTransferProgress key:" + key + "- state:" + xfer_state, 0, true);
    }
    public S3Type getS3Type() {
        return s3Type;
    }

    public void setS3Type(S3Type s3Type) {
        this.s3Type = s3Type;
    }
    
}

