/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3v2.action;

/**
 *
 * @author loy
 */

import org.cdlib.mrt.utility.TException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.s3.model.S3ResponseMetadata;


import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.s3v2.tools.GetSSM;
import org.cdlib.mrt.utility.Checksums;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
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

public class GetObject {
    protected static final Logger logger = LogManager.getLogger(); 
    public static GetSSM getSSM = new GetSSM();
    

    /**
     * Asynchronously retrieves the bytes of an object from an Amazon S3 bucket and writes them to a local file.
     *
     * @param bucketName the name of the S3 bucket containing the object
     * @param keyName    the key (or name) of the S3 object to retrieve
     * @param path       the local file path where the object's bytes will be written
     * @return a {@link CompletableFuture} that completes when the object bytes have been written to the local file
     */
    public static void downloadObjectFuture(S3AsyncClient s3AsyncClient, String bucketName, String keyName, String path) 
    {
        
logger.trace("***downloadObjectFuture:"
        + " - keyName=" + keyName
        + " - bucketName=" + bucketName
);     
        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();
 

        CompletableFuture<GetObjectResponse> futureGet = s3AsyncClient.getObject(objectRequest,
                AsyncResponseTransformer.toFile(Paths.get(path)));

        futureGet.whenComplete((resp, err) -> {
            try {
                if (resp != null) {
                    System.out.println("Object downloaded. Details: "+resp);
                } else {
                    err.printStackTrace();
                }
            } finally {
               // Only close the client when you are completely done with it
                s3AsyncClient.close();
            }
        });
        futureGet.join();
    }

    public static GetObjectResponse downloadObjectTransfer(S3AsyncClient s3AsyncClient, String bucketName,
                             String key, String downloadedFileWithPath) 
    {
logger.trace("***downloadObjectTransfer:"
        + " - keyName=" + key
        + " - bucketName=" + bucketName
);
        S3TransferManager transferManager =
                S3TransferManager.builder()
                             .s3Client(s3AsyncClient)
                             .build();
        DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
                .getObjectRequest(b -> b.bucket(bucketName).key(key))
                .destination(Paths.get(downloadedFileWithPath))
                .build();

        FileDownload downloadFile = transferManager.downloadFile(downloadFileRequest);

        GetObjectResponse downloadResponse = downloadFile.completionFuture().join().response();
        logger.debug("Content length [{}]", downloadResponse.contentLength());
        return downloadResponse;
    }

    public static void getObjectSync (S3Client s3Client, String bucketName, String keyName, String path ) 
        throws TException
    {
logger.trace("***getObjectSync:"
        + " - keyName=" + keyName
        + " - bucketName=" + bucketName
);
        File outFile = new File(path);
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            InputStream is = s3Client.getObject(objectRequest, ResponseTransformer.toInputStream());
            
            FileUtil.stream2File(is, outFile);
        } catch (S3Exception e) {
          System.err.println(e.awsErrorDetails().errorMessage());
           throw new TException(e);
        }
    }  
    
    

    public static InputStream getObjectSyncInputStream (S3Client s3Client, String bucketName, String keyName) 
        throws TException
    {
logger.trace("***getObjectSync:"
        + " - keyName=" + keyName
        + " - bucketName=" + bucketName
);
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            InputStream is = s3Client.getObject(objectRequest, ResponseTransformer.toInputStream());
            return is;
            
        } catch (S3Exception e) {
           //System.err.println(e.awsErrorDetails().errorMessage());
           if ((e.statusCode() == 404) || e.toString().contains("404")) {
               throw new TException.REQUESTED_ITEM_NOT_FOUND("Not found:"
                       + " - bucket:" + bucketName
                       + " - key:" + keyName);
           }
           throw new TException(e);
        }
    }  
}

