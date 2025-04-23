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
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
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
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3v2.test.TestGetObject;
import static org.cdlib.mrt.s3v2.test.TestGetObject.getClientAWS;
import static org.cdlib.mrt.s3v2.test.TestGetObject.getClientSDSC;
import static org.cdlib.mrt.s3v2.test.TestGetObject.test_wasabi_download;
import org.cdlib.mrt.s3v2.tools.GetSSM;
import org.cdlib.mrt.utility.Checksums;
import org.cdlib.mrt.utility.DateUtil;
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

public class GetObjectList {
    protected static final Logger logger = LogManager.getLogger(); 
    public static void main(String[] args) 
            throws TException
    {
        test_aws_prefix_single();
        test_aws_prefix();
        test_aws_after();
    }
    
    
    public static void test_aws_prefix_single()
            throws TException
    { 
        System.out.println("***test_aws***");
        String bucketName = "uc3-s3mrt5001-stg";
        V2Client v2client = getClientAWS();
        S3Client s3Client = v2client.s3Client();
        String prefix = "ark:/99999/fk4gj0zc73|1|system/mrt-mom.txt";
        CloudResponse response = new CloudResponse();
        awsListPrefix(s3Client, bucketName, prefix, 200, response);
        CloudList cloudList = response.getCloudList();
        String dump = cloudList.dump("test_aws");
        System.out.println(dump);
        
    }
    
    public static void test_aws_prefix()
            throws TException
    { 
        System.out.println("***test_aws***");
        String bucketName = "uc3-s3mrt5001-stg";
        V2Client v2client = getClientAWS();
        S3Client s3Client = v2client.s3Client();
        String prefix = "ark:/99999/fk4gj0zc73|";
        CloudResponse response = new CloudResponse();
        awsListPrefix(s3Client, bucketName, prefix, 200, response);
        CloudList cloudList = response.getCloudList();
        String dump = cloudList.dump("test_aws");
        System.out.println(dump);
        
    }
    
    public static void test_aws_after()
            throws TException
    { 
        System.out.println("***test_aws***");
        String bucketName = "uc3-s3mrt5001-stg";
        V2Client v2client = getClientAWS();
        S3Client s3Client = v2client.s3Client();
        String startAfter = "ark:/99999/fk4gj0zc73|";
        CloudResponse response = new CloudResponse();
        awsListAfter(s3Client, bucketName, startAfter, 200, response);
        CloudList cloudList = response.getCloudList();
        String dump = cloudList.dump("test_aws");
        System.out.println(dump);
        
    }
    
    // seee https://www.baeldung.com/java-aws-s3-list-bucket-objects
    public static void awsListPrefix(
            S3Client s3Client,
            String bucketName,
            String prefix, 
            int maxEntries,
            CloudResponse response) 
        throws TException 
    {

        System.out.println("***>awsListPrefix:"
                + " - bucketName=" + bucketName
                + " - prefix=" + prefix
                + " - maxEntries=" + maxEntries
        );
        //GetObjectList.awsListAfter(s3Client, bucketName, prefix, maxEntries, response);
        String nextContinuationToken = null;
        long totalObjects = 0;

        do {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .build();
            ListObjectsV2Response listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
            nextContinuationToken = listObjectsV2Response.nextContinuationToken();
            List<S3Object> contents = listObjectsV2Response.contents();
            System.out.println("contents size:" + contents.size());
            for (S3Object s3Object : contents) {
                totalObjects++;
                if (maxEntries <= 0) {}
                else if (totalObjects >= maxEntries) return;
                CloudList.CloudEntry entry = s3ObjectToCloudEntry(bucketName, s3Object);
                response.addObject(entry);
            }
        } while (nextContinuationToken != null);
        System.out.println("Number of objects in the bucket: " + totalObjects);
    }
   
    
    // see https://www.baeldung.com/java-aws-s3-list-bucket-objects
    public static void awsListAfter(
            S3Client s3Client,
            String bucketName,
            String startAfter, 
            int maxEntries,
            CloudResponse response) 
        throws TException 
    {
        System.out.println("***>awsListAfter:"
                + " - bucketName=" + bucketName
                + " - startAfter=" + startAfter
                + " - maxEntries=" + maxEntries
        );
        long totalObjects = 0;
        
        doBreak:
        do {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .startAfter(startAfter)
                .build();
            ListObjectsV2Response listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
            List<S3Object> contents = listObjectsV2Response.contents();
            for (S3Object s3Object : contents) {
                CloudList.CloudEntry entry = s3ObjectToCloudEntry(bucketName, s3Object);
                response.addObject(entry);
                totalObjects++;
                if (totalObjects >= maxEntries) break doBreak;
            }
            startAfter = listObjectsV2Response.nextContinuationToken();
            logger.trace("Next Continuation Token: " + startAfter + " - cnt=" + totalObjects);
            
        } while (startAfter != null);
        //System.out.println("Number of objects in the bucket: " + totalObjects);
    }
   
    public static CloudList.CloudEntry s3ObjectToCloudEntry(String bucketName, S3Object s3Object)
        throws TException
    {
            try {
                
                String container = bucketName;
                String key = s3Object.key();
                Long size = s3Object.size();
                String etag = s3Object.eTag();
                String storageClass = s3Object.storageClassAsString();
                //String sha256 = (String)s3Object.getValueForField("sha256",Class.forName("String"));
                
                Instant instantDate = s3Object.lastModified();
                Date date = Date.from(instantDate);
                String lastModified = DateUtil.getIsoDate(date);
                CloudList.CloudEntry entry = new CloudList.CloudEntry(container, key, size, etag, null, lastModified, null, storageClass);
                return entry;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
         
}

