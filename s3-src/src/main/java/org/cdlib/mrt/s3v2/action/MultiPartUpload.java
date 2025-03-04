/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3v2.action;

/**
 *
 * @author loy
 */
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.RandomAccessFile;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.s3.model.S3ResponseMetadata;


    
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import java.nio.file.Paths;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import java.nio.ByteBuffer;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class MultiPartUpload {
    protected static final Logger logger = LogManager.getLogger(); 
    
    final static Long MB = 1024L * 1024 * 1024;
    
    // see https://www.baeldung.com/aws-s3-multipart-upload
    public static void uploadFileParts(
            S3Client s3,
            String existingBucketName,
            String keyName, 
            String filePath,
            Map<String, String> metadata) 
    {

    // Initiate a multipart upload
    CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
        .bucket(existingBucketName)
        .key(keyName)
        .metadata(metadata)
        .build();

    CreateMultipartUploadResponse createResponse = s3.createMultipartUpload(createRequest);

    String uploadId = createResponse.uploadId();

    // Prepare the parts to be uploaded
    List<CompletedPart> completedParts = new ArrayList<>();
    int partNumber = 1;
    ByteBuffer buffer = ByteBuffer.allocate(5 * 1024 * 1024); // Set your preferred part size (5 MB in this example)

    // Read the file and upload each part
    try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
        long fileSize = file.length();
        long position = 0;

        while (position < fileSize) {
            file.seek(position);
            int bytesRead = file.getChannel().read(buffer);

            buffer.flip();
            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(existingBucketName)
                .key(keyName)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength((long) bytesRead)
                .build();

            UploadPartResponse response = s3.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(buffer));

            completedParts.add(CompletedPart.builder()
                .partNumber(partNumber)
                .eTag(response.eTag())
                .build());

            logger.debug("add completedParts:" + position);
            //System.out.println("add completedParts:" + position);
                buffer.clear();
                position += bytesRead;
                partNumber++;
            }
        
    } catch (IOException e) {
        e.printStackTrace();
    }

    // Complete the multipart upload
    CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
        .parts(completedParts)
        .build();

    CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
        .bucket(existingBucketName)
        .key(keyName)
        .uploadId(uploadId)
        .multipartUpload(completedUpload)
        .build();

    CompleteMultipartUploadResponse completeResponse = s3.completeMultipartUpload(completeRequest);
    } 

    public String uploadFile(S3TransferManager transferManager, String bucketName,
                             String key, URI filePathURI) {
        UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
            .putObjectRequest(b -> b.bucket(bucketName).key(key))
            .source(Paths.get(filePathURI))
            .build();

        FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);

        CompletedFileUpload uploadResult = fileUpload.completionFuture().join();
        return uploadResult.response().eTag();
    }
}