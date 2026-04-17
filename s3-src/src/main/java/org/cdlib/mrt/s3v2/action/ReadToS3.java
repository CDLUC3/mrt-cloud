/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3v2.action;

/**
 *
 * @author loy
 */
import java.io.File;
import org.cdlib.mrt.s3.tools.S3Reader;
import org.cdlib.mrt.s3v2.action.*;
import org.cdlib.mrt.s3.staging.service.RetrieveResponse;
import software.amazon.awssdk.core.sync.RequestBody;
import java.util.List;
import java.util.ArrayList;

import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import java.nio.ByteBuffer;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.s3.staging.action.TagObject;
import org.cdlib.mrt.s3.staging.action.TagObject;
import org.cdlib.mrt.s3.tools.ChecksumHandler;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.TException;
import software.amazon.awssdk.services.s3.model.S3Exception;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class ReadToS3 {
    protected static final Logger log4j = LogManager.getLogger(); 
    
    final static Long MB = 1024L * 1024 * 1024;
    final static int BUFSIZE = 75 * 1024 * 1024;
    protected static final boolean DEBUG = false;
    
    private boolean exec = true;
    
    protected S3Client s3 = null;
    protected String bucketName = null;
    protected S3Reader s3Reader = null;
    protected String keyName = null;
    protected String [] digestTypesS = null;
    protected ChecksumHandler checksumHandler = null;
    protected long inObjectSize = 0;
    protected RetrieveResponse response = null;
    protected String fillSha256 = null;
    protected long fillSize = 0;
    
    // see https://www.baeldung.com/aws-s3-multipart-upload
    
    
    public static ReadToS3 getReadToS3 (
            S3Client s3,
            String bucketName,
            String keyName,
            String [] digestTypesS,
            S3Reader s3Reader)
        throws TException
    {
        return new ReadToS3(s3, bucketName, keyName, digestTypesS, s3Reader);
    }
    
    protected ReadToS3(
            S3Client s3,
            String bucketName,
            String keyName,
            String [] digestTypesS,
            S3Reader s3Reader)
        throws TException
    {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.keyName = keyName;
        this.digestTypesS = digestTypesS;
        this.s3Reader = s3Reader;
        init();
    }
    
  
    private void init()
       throws TException
    {
        checksumHandler = ChecksumHandler.getChecksumHandler(digestTypesS);
        response = new RetrieveResponse(keyName, checksumHandler);
        
    }
    
    
    public RetrieveResponse doUpload()
       throws TException
    {
        try {
            response.startIS = System.currentTimeMillis();
            response.beginIS = System.currentTimeMillis();
            String sha256 = s3Reader.getMetaSha256();
            HashMap<String, String> metadata = new HashMap<>();
            metadata.put("sha256", sha256);
            inObjectSize = inObjectSize = s3Reader.getMetaObjectSize();
            long readBufSize = s3Reader.getReadBufSize();
            long maxBufSize = s3Reader.getMaxBufSize();
            if (inObjectSize > maxBufSize) {
                response = uploadFileParts(metadata);
            } else {
                response = uploadFile(metadata);
            }
            
            validateWrite(response);
            return response;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    } 
    
    protected void validateWrite(RetrieveResponse response)
            throws TException
    {
        fillSha256 = checksumHandler.getChecksum("sha256");
        fillSize = response.length();
        String msg = "ReadToS3 "
                + " - inKey=" + s3Reader.getKey()
                + " - inBucket=" + s3Reader.getBucket()
                + " - outKey=" + keyName
                + " - outBucket=" + bucketName
                + " - inLen=" + s3Reader.getMetaObjectSize()
                + " - fillSize=" + fillSize
                + " - inSha256=" + s3Reader.getMetaSha256()
                + " - fillSha256=" + fillSha256;
                
        if  (s3Reader.getMetaObjectSize() != fillSize) {
            throw new TException.INVALID_DATA_FORMAT(msg + " - in out length mismatch"
                    + " - getMetaObjectSize:" + s3Reader.getMetaObjectSize()
                    + " - fillSize:" + fillSize
            );
        }
        
        if  (!s3Reader.getMetaSha256().equals(fillSha256)) {
            throw new TException.INVALID_DATA_FORMAT(msg + " - digest mismatch");
        }
        log4j.debug("validateWrite worked(" + exec + "):"+ msg);
    }
    
    
        
    public RetrieveResponse uploadFile(Map<String, String> metadata) 
        throws TException
    {
        
        response.startMs = System.currentTimeMillis();
        try {
            long startPartMs = System.currentTimeMillis();
            long startFill = System.currentTimeMillis();
            s3Reader.startRead();
            byte[] bytes = s3Reader.nextRead();

            response.totalFillMs  += System.currentTimeMillis()-startFill;
            response.totalReadBytes += bytes.length;
            
            long startWriteMs = System.currentTimeMillis();
        if (exec) {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .metadata(metadata)
                .build();

            s3.putObject(putObjectRequest, RequestBody.fromBytes(bytes));
        }
            response.totalWriteMs = (System.currentTimeMillis()-startWriteMs);
            long bytesWritten = checksumHandler.fillBuff(bytes);
            long finishDigestMs = System.currentTimeMillis();
            checksumHandler.finishDigests();
            response.totalFillMs  += (System.currentTimeMillis()-finishDigestMs);
            response.setFromChecksumHandler();
            response.endMs = System.currentTimeMillis();
            log4j.debug("Successfully placed " + keyName + " into bucket " + bucketName);
            return response;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception e) {
            e.printStackTrace();
            String msg = "PutObjectRequest exception:"
                    + " - bucketName:" + bucketName
                    + " - objectKey:" + keyName
                    + " - exception:" + e;
            log4j.error(msg,e);
            throw new TException(e);
        }
    }
    
    
    public RetrieveResponse uploadFileParts(Map<String, String> metadata) 
        throws TException
    {
        if (DEBUG) System.out.println("uploadFileParts"
                + "\n - bucketName=" + bucketName
                + "\n - keyName=" + keyName
        );
        response.startMs = System.currentTimeMillis();
        
        // Initiate a multipart upload
        String uploadId = null;
        long startWriteMs = System.currentTimeMillis();
    if (exec) {
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(keyName)
            .metadata(metadata)
            //.checksumAlgorithm(ChecksumAlgorithm.SHA256)
            .build();

        CreateMultipartUploadResponse createResponse = s3.createMultipartUpload(createRequest);
        uploadId = createResponse.uploadId();
    }
        response.totalWriteMs += (System.currentTimeMillis()-startWriteMs);

        // Prepare the parts to be uploaded
        List<CompletedPart> completedParts = new ArrayList<>();
        int partNumber = 1;
        // Read the file and upload each part
        try {
            long position = 0;
            response.beginIS = System.currentTimeMillis();
            s3Reader.startRead();
            while (s3Reader.isMore()) {
                long startPartMs = System.currentTimeMillis();
                long startFill = System.currentTimeMillis();
                byte[] bytes = s3Reader.nextRead();
                response.totalReadMs += System.currentTimeMillis() - startFill;
                long bytesRead = checksumHandler.fillBuff(bytes);
                
                response.totalFillMs  += System.currentTimeMillis()-startFill;
                response.totalReadBytes += bytesRead;
                //System.out.println("Bytes read:" + bytesRead);

                startWriteMs = System.currentTimeMillis();
            if (exec) {
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .contentLength((long) bytesRead)
                    .build();

                UploadPartResponse uploadResponse = s3.uploadPart(uploadPartRequest, RequestBody.fromBytes(bytes));

                completedParts.add(CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(uploadResponse.eTag())
                    .build());
            }
                response.totalWriteMs += (System.currentTimeMillis()-startWriteMs);

                response.totalWriteBytes += bytes.length;
                long partMs = System.currentTimeMillis() - startPartMs;
                log4j.debug("add completedParts(" + partNumber + "):"  
                        + " - position=" + position 
                        + " - bytesRead=" + bytesRead
                        + " - partsMs=" + partMs
                );
                position += bytesRead;
                if (bytesRead < BUFSIZE) break;

                partNumber++;
            }
            checksumHandler.finishDigests();
            response.setFromChecksumHandler();
            response.endMs = System.currentTimeMillis();
            //response.totalWriteBytes = position;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
        }
        response.completeMultiPartAdd = System.currentTimeMillis();
        // Complete the multipart upload
        
        
        startWriteMs = System.currentTimeMillis();
    if (exec) {
        CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build();

        
        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(keyName)
            .uploadId(uploadId)
            .multipartUpload(completedUpload)
            //.checksumSHA256(sha256)
            .build();

        CompleteMultipartUploadResponse completeResponse = s3.completeMultipartUpload(completeRequest);
    }
        
    
        response.setFromChecksumHandler();
        response.endMs = System.currentTimeMillis();
        //addDigestTags(keyName, response);
        
        return response;
    } 

    public boolean isExec() {
        return exec;
    }

    public void setExec(boolean exec) {
        this.exec = exec;
    }

    public RetrieveResponse getResponse() {
        return response;
    }

    public String getFillSha256() {
        return fillSha256;
    }

    public Long getFillSize() {
        return fillSize;
    }

    public ChecksumHandler getChecksumHandler() {
        return checksumHandler;
    }

    
    
}