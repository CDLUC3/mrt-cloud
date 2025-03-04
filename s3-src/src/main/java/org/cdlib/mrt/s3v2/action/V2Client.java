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
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.cdlib.mrt.utility.TException;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.s3.model.S3ResponseMetadata;
import software.amazon.awssdk.services.s3.S3Configuration;


import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
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
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class V2Client {
    protected static final Logger logger = LogManager.getLogger(); 
    
    final static Long MB = 1024L * 1024 * 1024;
    
    public enum S3Type {aws, minio, sdsc, wasabi};
    protected S3Type s3Type = null;
    protected S3AsyncClient s3AsyncClient = null;
    protected S3Client s3Client = null;
    protected S3Presigner s3Presigner = null;
    protected Region region = null;
    
    public static V2Client getMinio(String accessKey, String secretKey, String endpoint)
        throws TException
    {
        V2Client minioClient = new V2Client(V2Client.S3Type.minio, Region.US_WEST_2, accessKey, secretKey, endpoint);
        return minioClient;
    }
    
    public static V2Client getSDSC(String accessKey, String secretKey, String endpoint)
        throws TException
    {
        V2Client minioClient = new V2Client(V2Client.S3Type.sdsc, Region.US_WEST_2, accessKey, secretKey, endpoint);
        return minioClient;
    }
    
    public static V2Client getWasabi(String accessKey, String secretKey, String endpoint)
        throws TException
    {
        V2Client wasabioClient = new V2Client(V2Client.S3Type.wasabi, Region.US_EAST_2, accessKey, secretKey, endpoint);
        return wasabioClient;
    }
    
    public static V2Client getAWS()
        throws TException
    {
        V2Client awsClient = new V2Client(V2Client.S3Type.aws, Region.US_WEST_2);
        return awsClient;
    }
    
    protected V2Client(S3Type s3Type, Region region, String accessKey, String secretKey, String endPoint)
        throws TException
    {
        this.s3Type = s3Type;
        this.s3AsyncClient = asycClient(s3Type, region, accessKey, secretKey, endPoint);
        this.s3Client = syncClient(region, accessKey, secretKey, endPoint);
        this.s3Presigner = presigner(region, accessKey, secretKey, endPoint);
        this.region = region;
    }
    
    protected V2Client(S3Type s3Type,  Region region)
        throws TException
    {
        this.s3Type = s3Type;
        this.s3AsyncClient = asyncClientDefaultNew(region);
        this.s3Client = syncClientDefault(Region.US_WEST_2);
        this.s3Presigner = presignerDefault(Region.US_WEST_2);
        
    }
    
    public static S3AsyncClient asycClient(S3Type s3Type, Region region, String accessKey, String secretKey, String endPoint) 
            throws TException
    {
        try {
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                    accessKey, secretKey);
            AwsCredentialsProvider creds = StaticCredentialsProvider.create(awsCreds);
            /*
            S3AsyncClient s3AsyncClient = S3AsyncClient.crtBuilder()
                                                   .credentialsProvider(creds)
                                                   .endpointOverride(URI.create(endPoint))
                                                   .region(region)
                                                   .targetThroughputInGbps(20.0)
                                                   .minimumPartSizeInBytes(MB)
                                                   .build();
            */
            System.out.println("s3Type:" + s3Type.toString());
            S3AsyncClient s3AsyncClient = null;
            if ((s3Type == S3Type.sdsc) || (s3Type == S3Type.minio)) {
                // sdsc type S3 interface on Qumulo doesn’t currently support virtual host style bucket addressing
                System.out.println("SET forcePathStyle(true)");
                
                s3AsyncClient = S3AsyncClient.crtBuilder()
                                                   .credentialsProvider(creds)
                                                   .forcePathStyle(true)
                                                   .endpointOverride(URI.create(endPoint))
                                                   .region(region)
                                                // .targetThroughputInGbps(20.0)
                                                 //.minimumPartSizeInBytes(MB)
                                                   .build();
           /*
                s3AsyncClient = S3AsyncClient.builder()
                        .credentialsProvider(creds)
                        .endpointOverride(URI.create(endPoint))
                        .region(region)
                        .multipartEnabled(true)
                        .forcePathStyle(true)
                        .multipartConfiguration(conf -> conf.apiCallBufferSizeInBytes(32 * MB))
                        .build();
           */
            } else {
                System.out.println("NO SET forcePathStyle(true)");
                s3AsyncClient = S3AsyncClient.builder()
                        .credentialsProvider(creds)
                        .endpointOverride(URI.create(endPoint))
                        .region(region)
                        .multipartEnabled(true)
                        .multipartConfiguration(conf -> conf.apiCallBufferSizeInBytes(32 * MB))
                        .build();
            }
            return s3AsyncClient;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    }  
    
    public static S3Client syncClient(Region region, String accessKey, String secretKey, String endPoint) 
            throws TException
    {
        try {
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                    accessKey, secretKey);
            AwsCredentialsProvider creds = StaticCredentialsProvider.create(awsCreds);
                   
            S3Client s3Client = S3Client.builder()
                          .credentialsProvider(creds)
                          .region(region)
                          .endpointOverride(URI.create(endPoint))
                          .forcePathStyle(true)
                          .build();
            return s3Client;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    }   
    
    public static S3Presigner presigner(Region region, String accessKey, String secretKey, String endPoint) 
            throws TException
    {
        try {
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                    accessKey, secretKey);
            AwsCredentialsProvider creds = StaticCredentialsProvider.create(awsCreds);
            URI endpointURI = new URI(endPoint);
            S3Presigner presigner = S3Presigner.builder()
                    .credentialsProvider(creds)
                    .endpointOverride(endpointURI)
                    .region(region)
                    //>>> required
                    .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                    //<<<
                    .build();
            return presigner;

        } catch (URISyntaxException e) {
            System.err.println("Invalid URI syntax: " + e.getMessage());
            throw new TException(e);
            
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    }      
    
    public static S3Presigner presignerDefault(Region region) 
            throws TException
    {
        try {
            S3Presigner presigner = S3Presigner.builder()
                    .region(region)
                    .build();
            return presigner;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    }        
    
    public static S3AsyncClient asyncClientDefaultOld(Region region) 
            throws TException
    {
        try {        
            S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
                    
                          .region(region)
                          .endpointOverride(URI.create("https://s3.us-west-2.amazonaws.com"))
                          .multipartEnabled(true)
                          .multipartConfiguration(conf -> conf.apiCallBufferSizeInBytes(20 * MB))
                //.targetThroughputInGbps(20.0)
                //.minimumPartSizeInBytes(8 * MB)
                          .forcePathStyle(true)
                          .build();
           
            return s3AsyncClient;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    }                
    
    public static S3AsyncClient asyncClientDefaultNew(Region region) 
            throws TException
    {
        try {        
            S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
                
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(region)
                .multipartEnabled(true)
                .multipartConfiguration(conf -> conf.apiCallBufferSizeInBytes(20 * MB))
                //.targetThroughputInGbps(20.0)
                //.minimumPartSizeInBytes(8 * MB)
                //          .forcePathStyle(true)
                          .build();
           
            return s3AsyncClient;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    }       
    
    public static S3AsyncClient asyncClientDefaultCRT(Region region) 
            throws TException
    {
        try {        
            S3AsyncClient s3AsyncClient = S3AsyncClient.crtBuilder()
                    
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                          //.endpointOverride(URI.create("https://s3.us-west-2.amazonaws.com"))
                          //.multipartEnabled(true)
                          //.multipartConfiguration(conf -> conf.apiCallBufferSizeInBytes(32 * MB))
                .targetThroughputInGbps(20.0)
                //.minimumPartSizeInBytes(8 * MB)
                .minimumPartSizeInBytes(20 * MB)
                 //         .forcePathStyle(true)
                          .build();
           
            return s3AsyncClient;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    }             
    
    public static S3Client syncClientDefault(Region region) 
            throws TException
    {
        try {        
            S3Client s3Client = S3Client.builder()
                          .region(region)
                          .endpointOverride(URI.create("https://s3.us-west-2.amazonaws.com"))
                          .forcePathStyle(true)
                          .build();
           
            return s3Client;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    } 
        
    public S3Client s3Client() {
            return this.s3Client;
    }
        
    public S3AsyncClient s3AsyncClient() {
            return this.s3AsyncClient;
    }

    public S3Presigner s3Presigner() {
        return s3Presigner;
    }

    public Region getRegion() {
        return region;
    }

    public S3Type getS3Type() {
        return s3Type;
    }
    
}

