/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3.staging.action;

/**
 *
 * @author loy
 */
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
import java.nio.ByteBuffer;
import java.io.InputStream;
import java.util.HashMap;
import org.apache.http.HttpEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.s3.staging.action.TagObject;
import org.cdlib.mrt.s3.staging.tools.ChecksumHandler;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.TException;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class StreamToS3 {
    protected static final Logger logger = LogManager.getLogger(); 
    
    final static Long MB = 1024L * 1024 * 1024;
    final static int BUFSIZE = 75 * 1024 * 1024;
    
    protected String urlS = null;
    protected S3Client s3 = null;
    protected String existingBucketName = null;
    //protected String keyName = null;
    //protected String [] digestTypesS = null;
    // see https://www.baeldung.com/aws-s3-multipart-upload
    
    
    public static StreamToS3 getStreamToS3 (
            S3Client s3,
            String existingBucketName)
        throws TException
    {
        return new StreamToS3(s3, existingBucketName);
    }
    
    protected StreamToS3(
            S3Client s3,
            String existingBucketName)
        throws TException
    {
        this.s3 = s3;
        this.existingBucketName = existingBucketName;
    }
    
    /**
    protected Url2S3(
            String urlS,
            S3Client s3,
            String existingBucketName,
            String keyName,
            String [] digestTypesS)
        throws TException
    {
        this.urlS = urlS;
        this.s3 = s3;
        this.existingBucketName = existingBucketName;
        this.keyName = keyName;
        this.digestTypesS = digestTypesS;
        if (digestTypesS == null) {
            throw new TException.INVALID_OR_MISSING_PARM("digestTypes required");
        }
        response = new Url2S3Response();
        checksumHandler = ChecksumHandler.getChecksumHandler(digestTypesS);
    }
    **/
    
    public RetrieveResponse uploadFileParts(
            InputStream inStream,
            String keyName,
            String [] digestTypesS)
        throws TException
    {
        ChecksumHandler checksumHandler = ChecksumHandler.getChecksumHandler(digestTypesS);
        RetrieveResponse response = new RetrieveResponse(keyName, checksumHandler);
        System.out.println("uploadFileParts"
                + "\n - urlS=" + urlS
                + "\n - existingBucketName=" + existingBucketName
                + "\n - keyName=" + keyName
        );
        response.startMs = System.currentTimeMillis();
        // Initiate a multipart upload
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
            .bucket(existingBucketName)
            .key(keyName)
            //.checksumAlgorithm(ChecksumAlgorithm.SHA256)
            .build();

        CreateMultipartUploadResponse createResponse = s3.createMultipartUpload(createRequest);

        String uploadId = createResponse.uploadId();

        // Prepare the parts to be uploaded
        List<CompletedPart> completedParts = new ArrayList<>();
        int partNumber = 1;
        ByteBuffer buffer = ByteBuffer.allocate(BUFSIZE); // Set your preferred part size (75 MB in this example)

        // Read the file and upload each part
        try {
            long position = 0;
            response.startIS = System.currentTimeMillis();
            response.beginIS = System.currentTimeMillis();
            while (true) {
                long startPartMs = System.currentTimeMillis();
                long startFill = System.currentTimeMillis();
                long bytesRead = checksumHandler.fillBuff(inStream, buffer, BUFSIZE);
                response.totalFillMs  += System.currentTimeMillis()-startFill;
                response.totalReadBytes += bytesRead;
                //System.out.println("Bytes read:" + bytesRead);


                buffer.flip();
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(existingBucketName)
                    .key(keyName)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .contentLength((long) bytesRead)
                    .build();

                UploadPartResponse uploadResponse = s3.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(buffer));

                completedParts.add(CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(uploadResponse.eTag())
                    .build());

                long partMs = System.currentTimeMillis() - startPartMs;
                System.out.println("add completedParts(" + partNumber + "):"  
                        + " - position=" + position 
                        + " - bytesRead=" + bytesRead
                        + " - partsMs=" + partMs
                );
                position += bytesRead;
                if (bytesRead < BUFSIZE) break;

                //System.out.println("add completedParts:" + position);
                buffer.clear();
                partNumber++;
            }
            response.totalWriteBytes = position;
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
        }
        response.completeMultiPartAdd = System.currentTimeMillis();
        // Complete the multipart upload
        CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build();

        checksumHandler.finishDigests();

        
        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
            .bucket(existingBucketName)
            .key(keyName)
            .uploadId(uploadId)
            .multipartUpload(completedUpload)
            //.checksumSHA256(sha256)
            .build();

        CompleteMultipartUploadResponse completeResponse = s3.completeMultipartUpload(completeRequest);
        response.setFromChecksumHandler();
        response.endMs = System.currentTimeMillis();
        response.dump("end result");
        //addDigestTags(keyName, response);
        
        return response;
    } 
    
    public void addDigestTags(String keyName, RetrieveResponse response)
        throws TException
    {
        String sha256 = response.getSha256();
        if (sha256 != null) {
            addTag(keyName, "sha256", sha256);
        }
        String md5 = response.getMd5();
        if (md5 != null) {
            addTag(keyName, "md5", md5);
        }
    }
    
    public void addTag(String keyName, String tagKey, String tagValue)
        throws TException
    { 
       TagObject.setTag(s3, existingBucketName, keyName, tagKey, tagValue);
       System.out.println("addTag"
               + " - existingBucketName:" + existingBucketName
               + " - keyName:" + keyName
               + " - tagKey:" + tagKey
               + " - tagValue:" + tagValue
       );
    }
}