/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3v2.test;

/**
 *
 * @author loy
 */

import org.cdlib.mrt.s3v2.action.*;
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
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class TestGetPresigned {
    protected static final Logger logger = LogManager.getLogger(); 
    public static GetSSM getSSM = new GetSSM();
    
    final static Long MB = 1024L * 1024 * 1024;
     protected static final String fileBig = "/apps/replic1/tomcatdata/big/6G.txt";
     protected static final String downFilePrefix = "/home/loy/tasks/awsv2/downloadsection.";
    protected static final String downloadBig = "/home/loy/tasks/awsv2/download.6G.txt";
    protected static final String downloadSmall = "/home/loy/tasks/awsv2/download.small.txt";
    protected static final String downloadBigWasabi = "/home/loy/tasks/awsv2/wasabi.download.6G.txt";
    protected static final String downloadBigMinio = "/home/loy/tasks/awsv2/minio.download.6G.txt";
    protected static final String downloadBigSDSC = "/home/loy/tasks/awsv2/sdsc.download.6G.txt";
    protected static final String sha256Big = "dd2a99e35c6ad550221bc3441d3ece19e0a84e2fccbf88246245f52f1a2e1cf6";
    protected static final String keyBig = "post|Big";
    protected static final String keyPre1= "ark:/99999/fk4gj0zc73|1|producer/X44100.pdf";
    
    protected static final String keyPre2= "ark:/99999/fk4gj0zc73|1|producer/X44100_DATA.xml";
    
    protected static final String keyPre3= "ark:/99999/fk49w1x25r|1|producer/X58701.pdf";
    protected static final String keyPre9502 = "ark:/20775/bb0547060t|1|producer/1-1.pdf";
    protected static final String keyPre2002 = "ark:/20775/bb0547060t|1|producer/1-1.pdf";
    protected static final String fileSmall = "/home/loy/t/ark+=99999=fk4806c36f/16/system/mrt-ingest.txt";
    protected static final String sha256Small = "47db2924db63fbaf78e9bf96fac08e96afbaa711a299c09d5ae13e8ee0b92452";
    protected static final String keySmall= "post|Small";
    
    public enum DownloadType {sync, transfer, future};
    public static void main(String[] args) 
            throws TException
    {
        //test_aws_download(keyBig, sha256Big, downloadBig);
        //test_sdsc_download(keyBig, sha256Big, downloadBig); // <- 503
        //test_minio_download(keyBig, sha256Big, downloadBig);
        //
        //test_sdsc_download(keyBig, sha256Big, downloadBig, DownloadType.transfer);
        //test_sdsc_download(keyBig, sha256Big, downloadBig, DownloadType.future);
        //test_sdsc_download(keyBig, sha256Big, downloadBig, DownloadType.sync);
        
        
        //test_wasabi_download(keyBig, sha256Big, downloadBig, DownloadType.transfer);
        //test_wasabi_download(keyBig, sha256Big, downloadBig, DownloadType.future);
        //test_wasabi_download(keyBig, sha256Big, downloadBig, DownloadType.sync);
        
        //test_wasabi_download(keyBig, 5000000001L, 5000000104L);
        //test_minio_download(keyBig, 5000000001L, 5000000103L);
        //test_sdsc_download(keyBig, 5000000001L, 5000000102L);
        //test_aws_download(keyBig, 5000000001L, 5000000101L);
        //test_aws_download(keyBig, 5000000001L, 4000000101L);
        //test_aws_download(keyPre3, 10L, null, "attachment; filename=test.pdf");
        //test_minio_download("ark:/28722/bk0003f028k|1|system/mrt-object-map.ttl", 10L, "text/turtle", "attachment; filename=mrt-object-map.ttl");
        test_minio_download("ark:/28722/bk0003f028k|1|system/mrt-object-map.ttl", 10L, null, "attachment; filename=mrt-object-map.ttl");
        //test_wasabi_download("ark:/28722/bk0006w8m0c|1|producer/cabeurle_60_1_00037077.xml", 10L, null,"attachment; filename=cabeurle_60_1_00037077.xml");
        //test_wasabi_download(keyPre9502, 10L, null, "attachment; filename=test.pdf");
        //test_minio_download(keyBig, sha256Big, downloadBig, DownloadType.future);
        //test_minio_download(keyBig, sha256Big, downloadBig, DownloadType.sync);
        //test_wasabi_download(keyBig, sha256Big, downloadBig);
    }
    
    
    
    public static void test_sdsc_download(String key, Long startByte, Long endByte)
            throws TException
    { 
        System.out.println("***test_sdsc***");
        String bucketName = "cdl-sdsc-backup-stg";
        V2Client v2client = getClientSDSC();
        //doGetObjectRange(v2client, bucketName, key, downFilePrefix, startByte, endByte);
    }
    
    public static void test_aws_download(
            String key,
            Long minutes,
            String contentType,
            String contentDisposition)
            throws TException
    { 
        System.out.println("***test_aws***");
        String bucketName = "uc3-s3mrt5001-stg";
        V2Client v2client = getClientAWS();
        doGetObjectPresign(v2client,bucketName, key,minutes,contentType, contentDisposition);
    }
       public static void test_wasabi_download(
            String key,
            Long minutes,
            String contentType,
            String contentDisposition)
            throws TException
    { 
        System.out.println("***test_wasabi***");
        
        V2Client v2client = getClientWasabi();
        String bucketName = "uc3-wasabi-useast-2.stage";
        doGetObjectPresign(v2client,bucketName, key,minutes,contentType, contentDisposition);
        //doGetObjectRange(v2client, bucketName, key, downFilePrefix, startByte, endByte);
    }
    
    public static void test_minio_download(
            String key,
            Long minutes,
            String contentType,
            String contentDisposition)
            throws TException
    { 
        System.out.println("***test_minio***");
        String bucketName = "cdl.sdsc.stage";
        V2Client v2client = getClientMinio();
        doGetObjectPresign(v2client,bucketName, key,minutes,contentType, contentDisposition);
        //doGetObjectRange(v2client, bucketName, key, downFilePrefix, startByte, endByte);
    }
    
    public static Properties getMeta(String header, S3Client s3Client, String bucketName, String key)
    {
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        
        if (prop == null) {
            System.out.println(header + ": prop null");
          
            
        } else if (prop.isEmpty()) {
            System.out.println(header + ": prop null");
            
        } else {
            System.out.println(PropertiesUtil.dumpProperties(header, prop));
        }
        return prop;
    }
    
    public static V2Client getClientWasabi() 
            throws TException
    {
        System.out.println("---getClientWasabi");
        
        String endpoint = "https://s3.us-east-2.wasabisys.com:443";
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/wasabi-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/wasabi-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        Region region = Region.US_EAST_2;
        
        V2Client v2client = V2Client.getWasabi(accessKey, secretKey, endpoint);
        return v2client;
    }
    
    
    public static V2Client getClientMinio() 
            throws TException
    {
        System.out.println("---getClientMinio");
        String endpoint = "https://cdl.s3.sdsc.edu:443";
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/sdsc-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/sdsc-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        
        System.out.println("keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String key = "ark:/28722/bk0006w8m0c|1|system/mrt-erc.txt";
        Region region = Region.US_WEST_2;

        V2Client v2client = V2Client.getMinio(accessKey, secretKey, endpoint);
        return v2client;
    }
    
    public static V2Client getClientSDSC() 
            throws TException
    {
        System.out.println("---getClientSDSC");
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        System.out.println("sdsc keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String endpoint = "https://uss-s3.sdsc.edu:443";

        V2Client v2client = V2Client.getSDSC(accessKey, secretKey, endpoint);
        return v2client;
    }
    
    public static V2Client getClientAWS() 
            throws TException
    {
        System.out.println("---getClientAWS");
        V2Client v2client = V2Client.getAWS();
        return v2client;
    }
    
    protected static void doGetObjectPresign(
            V2Client v2client,
            String bucketName,
            String key,
            Long minutes,
            String contentType,
            String contentDisposition)
        throws TException
    {
        
        S3Client s3Client = v2client.s3Client();
        S3AsyncClient s3AsyncClient = v2client.s3AsyncClient();
        S3Presigner s3Presigner = v2client.s3Presigner();
        Long mS = minutes * 1000 * 60;
        String presign = GetObjectPresign.getObjectPresign(s3Presigner, bucketName, key, mS, contentType, contentDisposition);
        
        
        
        System.out.println("doGetObjectAsync"
                + " - bucketName=" + bucketName
                + " - key=" + key
                + " - mS=" + mS
                + " - contentType=" + contentType
                + " - contentDisposition=" + contentDisposition
        ); 
        
        
        System.out.println("**Presign->>>" + presign + "<<<-");
        
    }
}

