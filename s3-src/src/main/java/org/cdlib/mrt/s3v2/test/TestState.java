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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import java.net.URI;
import java.io.File;
import java.net.URL;
import java.util.Date;
import java.time.Instant;

import org.cdlib.mrt.s3v2.tools.GetSSM;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.utility.Checksums;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;

import software.amazon.awssdk.services.s3.S3AsyncClient;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class TestState {
    protected S3AsyncClient s3Client = null;
    public static GetSSM getSSM = new GetSSM();
    
    protected static final String fileBig = "/apps/replic1/tomcatdata/big/6G.txt";
    protected static final String downloadBig = "/home/loy/tasks/awsv2/download.6G.txt";
    protected static final String downloadSmall = "/home/loy/tasks/awsv2/download.small.txt";
    protected static final String downloadBigWasabi = "/home/loy/tasks/awsv2/wasabi.download.6G.txt";
    protected static final String downloadBigMinio = "/home/loy/tasks/awsv2/minio.download.6G.txt";
    protected static final String downloadBigSDSC = "/home/loy/tasks/awsv2/sdsc.download.6G.txt";
    protected static final String sha256Big = "dd2a99e35c6ad550221bc3441d3ece19e0a84e2fccbf88246245f52f1a2e1cf6";
    protected static final String keyBig = "post|Big";
    
    protected static final String fileSmall = "/home/loy/t/ark+=99999=fk4806c36f/16/system/mrt-ingest.txt";
    protected static final String sha256Small = "47db2924db63fbaf78e9bf96fac08e96afbaa711a299c09d5ae13e8ee0b92452";
    protected static final String keySmall= "post|Small";
    
    public static void main(String[] args) 
            throws TException
    {
        //test_minio_sync(keySmall, sha256Small, fileSmall, downloadSmall);
        //test_minio_sync(keyBig, sha256Big, fileBig, downloadBig);
        //if (true) return;
        test_aws(keySmall, sha256Small, fileSmall, downloadSmall);
        test_sdsc(keySmall, sha256Small, fileSmall, downloadSmall);
        test_glacier(keySmall, sha256Small, fileSmall, downloadSmall);
        test_wasabi(keySmall, sha256Small, fileSmall, downloadSmall);
        test_minio_sync(keySmall, sha256Small, fileSmall, downloadSmall);
        
        //test_aws(keyBig, sha256Big, fileBig, downloadBig);
        //test_sdsc(keyBig, sha256Big, fileBig, downloadBig);
        //test_glacier(keyBig, sha256Big, fileBig, downloadBig);
        //test_wasabi(keyBig, sha256Big, fileBig, downloadBig);
        //test_minio_sync(keyBig, sha256Big, fileBig, downloadBig);
        
        //test_minio_sync(keyBig, sha256Big, fileBig, downloadBig);
        //test_minio_sync(keySmall, sha256Small, fileSmall, downloadSmall);
        //test_minio(keySmall, sha256Small, fileSmall, downloadSmall);
        //(keySmall, sha256Small, fileSmall, downloadSmall);
        //test_wasabi(keyBig, sha256Big, fileBig, downloadBig);
        //test_aws_fail(keyBig, sha256Big, fileBig, downloadBig);
        //test_aws(keyBig, sha256Big, fileBig, downloadBig);
        //test_sdsc(keyBig, sha256Big, fileBig, downloadBig);
        //test_glacier(keyBig, sha256Big, fileBig, downloadBig);
        //test_minio_delete( keySmall );
        
        // test_temp_delete( keySmall );
        
        //test_wasabi_delete( keySmall );
        
        
        //test_wasabi_uploadFileAsync(fileBig, keyBig, sha256Big);
        
        //test_wasabi_uploadFileAsync(fileSmall, keySmall, sha256Small);
        
        //test_wasabi_putS3Object(fileSmall, keySmall, sha256Small);
        
        //test_minio_uploadFileAsync(fileSmall, keySmall, sha256Small) ;//<-fails
        
        //test_temp_uploadFileAsync(fileSmall, keySmall, sha256Small) ;//<-works
        
        //test_temp_uploadFileAsync(fileBig, keyBig, sha256Big) ;
        
        //test_temp_multiparUpload(fileBig, keyBig, sha256Big) ;
        
        //test_wasabi_multiPartUpload(fileBig, keyBig, sha256Big) ;
        
        //test_wasabi_download(downloadBigWasabi, keyBig, sha256Big) ;
        
        //test_sdsc_updown(keyBig, sha256Big, fileBig, downloadBigSDSC) ;
                
        //test_sdsc_putS3Object(fileSmall, keySmall, sha256Small) ; 
        
        //test_aws(args);
        
        //test_glacier(args);
        
        //test_minio(args);
        
        //test_wasabi(args);
    }
    
    public static void test_wasabi(String key, String sha256, String upFilePath, String downFilePth)
            throws TException
    { 
        System.out.println("***test_wasabi***");
        
        V2Client v2client = getClientWasabi();
        String bucketName = "uc3-wasabi-useast-2.stage";
        testState( v2client, bucketName, key, sha256, upFilePath, downFilePth);
    }
    
    public static void test_aws(String key, String sha256, String upFilePath, String downFilePth)
            throws TException
    { 
        System.out.println("***test_aws***");
        String bucketName = "uc3-s3mrt1001-stg";
        V2Client v2client = getClientAWS();
        testState(v2client, bucketName, key, sha256, upFilePath, downFilePth);
        //testStateSync( v2client, bucketName, key, sha256, upFilePath, downFilePth);
        //testState( v2client, bucketName, key, sha256, upFilePath, downFilePth);  // works aws
    }
    
    public static void test_aws_fail(String key, String sha256, String upFilePath, String downFilePth)
            throws TException
    { 
        System.out.println("***test_aws***");
        String bucketName = "uc3-s3mrt1001-stg";
        V2Client v2client = getClientAWS();
        testStateAsync(v2client, bucketName, key, sha256, upFilePath, downFilePth);
        //testStateSync( v2client, bucketName, key, sha256, upFilePath, downFilePth);
        //testState( v2client, bucketName, key, sha256, upFilePath, downFilePth);  // works aws
    }
    
    public static void test_glacier(String key, String sha256, String upFilePath, String downFilePth)
            throws TException
    { 
        System.out.println("***test_glacier***");
        String bucketName = "uc3-s3mrt6001-stg";
        V2Client v2client = getClientAWS();
        testState(v2client, bucketName, key, sha256, upFilePath, downFilePth);
        //testStateSync( v2client, bucketName, key, sha256, upFilePath, downFilePth);
        //testState( v2client, bucketName, key, sha256, upFilePath, downFilePth);  // works aws
    }
    
    public static void test_minio(String key, String sha256, String upFilePath, String downFilePth)
            throws TException
    { 
        System.out.println("***test_minio***");
        String bucketName = "cdl.sdsc.stage";
        V2Client v2client = getClientMinio();
        testState( v2client, bucketName, key, sha256, upFilePath, downFilePth);
        //testStateSync( v2client, bucketName, key, sha256, upFilePath, downFilePth);
        //testStateAsync( v2client, bucketName, key, sha256, upFilePath, downFilePth);
    }
    
    public static void test_minio_sync(String key, String sha256, String upFilePath, String downFilePth)
            throws TException
    { 
        System.out.println("***test_minio_sync***");
        String bucketName = "cdl.sdsc.stage";
        V2Client v2client = getClientMinio();
        testStateSync( v2client, bucketName, key, sha256, upFilePath, downFilePth);
        //testStateSync( v2client, bucketName, key, sha256, upFilePath, downFilePth);
        //testStateAsync( v2client, bucketName, key, sha256, upFilePath, downFilePth);
    }
    
    public static void test_sdsc(String key, String sha256, String upFilePath, String downFilePth)
            throws TException
    { 
        System.out.println("***test_sdsc***");
        String bucketName = "cdl-sdsc-backup-stg";
        V2Client v2client = getClientSDSC();
        testState( v2client, bucketName, key, sha256, upFilePath, downFilePth);
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
    
    
    protected static void addProp(Properties prop, String key, String value)
    {
        if (value == null) return;
        prop.setProperty(key, value);
    }
    
    protected static void doUploadAsync(
            V2Client v2client,
            String bucketName,
            String key,
            String filePath,
            String sha256)
    {
        S3Client s3Client = v2client.s3Client();
        S3AsyncClient s3AsyncClient = v2client.s3AsyncClient();
        LinkedHashMap<String, String> metadata = new LinkedHashMap();
        metadata.put("sha256", sha256);
        
        PutObjectData.uploadFileParts(
            s3AsyncClient, 
            bucketName,
            key,
            filePath, 
            metadata) ;
        
        Properties prop = getMeta(
            "after uploadFileAsync",
            s3Client, 
            bucketName,
            key);
    }
    
    protected static void doUploadSync(
            V2Client v2client,
            String bucketName,
            String key,
            String filePath,
            String sha256)
    {
        S3Client s3Client = v2client.s3Client();
        S3AsyncClient s3AsyncClient = v2client.s3AsyncClient();
        LinkedHashMap<String, String> metadata = new LinkedHashMap();
        metadata.put("sha256", sha256);
        
        PutObjectData.putS3Object(
            s3Client, 
            bucketName,
            key,
            filePath, 
            metadata) ;
        
        Properties prop = getMeta(
            "after uploadFileAsync",
            s3Client, 
            bucketName,
            key);
    }
    
    protected static void doUploadMultipart(
            V2Client v2client,
            String bucketName,
            String key,
            String filePath,
            String sha256)
    {
        
        
        S3Client s3Client = v2client.s3Client();
        S3AsyncClient s3AsyncClient = v2client.s3AsyncClient();
        
        System.out.println("doPutObject"
                + " - filePath=" + filePath
                + " - bucketName=" + bucketName
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
        );
        
        LinkedHashMap<String, String> metadata = new LinkedHashMap();
        metadata.put("sha256", sha256);
       
        MultiPartUpload.uploadFileParts(
            s3Client,
            bucketName,
            key, 
            filePath,
            metadata);
        
        Properties prop = getMeta("doUploadMultipart", s3Client, bucketName, key);
        System.out.println(PropertiesUtil.dumpProperties("doUploadMultipart", prop));
    }
    
    protected static void doPutObject(
            V2Client v2client,
            String bucketName,
            String key,
            String filePath,
            String sha256)
    {
        
        S3Client s3Client = v2client.s3Client();
        S3AsyncClient s3AsyncClient = v2client.s3AsyncClient();
        
        System.out.println("doPutObject"
                + " - filePath=" + filePath
                + " - bucketName=" + bucketName
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
        );
        
        LinkedHashMap<String, String> metadata = new LinkedHashMap();
        metadata.put("sha256", sha256);
       
        PutObjectData.putS3Object(
            s3Client, 
            bucketName,
            key,
            filePath, 
            metadata) ;
        
        Properties prop = getMeta("test_wasabi_putS3Object prop", s3Client, bucketName, key);
    }
    
    
    protected static Properties doGetObjectSync(
            V2Client v2client,
            String bucketName,
            String key,
            String filePath,
            String sha256)
        throws TException
    {
        
        S3Client s3Client = v2client.s3Client();
        S3AsyncClient s3AsyncClient = v2client.s3AsyncClient();
        
        System.out.println("doPutObject"
                + " - filePath=" + filePath
                + " - bucketName=" + bucketName
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
        );
        Properties prop = getMeta("test_wasabi_putS3Object prop", s3Client, bucketName, key);
        if (prop == null) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("Missing:"
                + " - bucketName=" + bucketName
                + " - filePath=" + filePath
                + " - key=" + key
            );
          
            
        } else if (prop.isEmpty()) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("Missing:"
                + " - bucketName=" + bucketName
                + " - filePath=" + filePath
                + " - key=" + key
            );
            
        }
        GetObject.getObjectSync(s3Client, bucketName, key, filePath);
        File downFile = new File(filePath); 
        long size = downFile.length();
        String [] types = {
                    "sha256"
                };
        Checksums checksums = Checksums.getChecksums(types, downFile);
        
        for (String type : types) {
            String checksum = checksums.getChecksum(type);
            System.out.println("getChecksum(" + type + "):" + checksum);
        }

        return prop;
    }
    
    
    protected static Properties doGetObjectAsync(
            V2Client v2client,
            String bucketName,
            String key,
            String filePath,
            String sha256)
        throws TException
    {
        
        S3Client s3Client = v2client.s3Client();
        S3AsyncClient s3AsyncClient = v2client.s3AsyncClient();
        
        System.out.println("doGetObjectAsync"
                + " - filePath=" + filePath
                + " - bucketName=" + bucketName
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
        );
        Properties prop = getMeta("doGetObjectAsync", s3Client, bucketName, key);
        if (prop == null) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("Missing:"
                + " - bucketName=" + bucketName
                + " - filePath=" + filePath
                + " - key=" + key
            );
          
            
        } else if (prop.isEmpty()) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("Missing:"
                + " - bucketName=" + bucketName
                + " - filePath=" + filePath
                + " - key=" + key
            );
            
        }
        GetObject.downloadObjectTransfer(s3AsyncClient, bucketName, key, filePath);
        File downFile = new File(filePath); 
        long size = downFile.length();
        String [] types = {
                    "sha256"
                };
        Checksums checksums = Checksums.getChecksums(types, downFile);
        
        for (String type : types) {
            String checksum = checksums.getChecksum(type);
            System.out.println("doGetObjectAsync complete getChecksum(" + type + "):" + checksum);
        }

        return prop;
    }
    
    public static void doDelete(
            V2Client v2client,
            String bucketName,
            String key)
        throws TException
    {
        
        S3Client s3Client = v2client.s3Client();
        
        System.out.println("doDelete"
                + " - bucketName=" + bucketName
                + " - key=" + key
        );
        
        Properties prop = getMeta(
            "beforeDelete",
            s3Client, 
            bucketName,
            key);
        
        
        if ((prop == null) || prop.isEmpty()) {
            System.out.println("Does not exist:"
                    + " - bucketname:" + bucketName
                    + " - key:" + key
            );
            return;
        }
        
        DeleteObjectData.deleteS3Object(
            s3Client, 
            bucketName,
            key);
        System.out.println("after delete");
        
        Properties propAfter = getMeta(
            "afterDelete",
            s3Client, 
            bucketName,
            key);
    }


    public static void doPresign (
            V2Client v2client, 
            String bucketName,
            String key)
        throws TException
    {
        try {
            S3Presigner s3Presigner = v2client.s3Presigner();
            String contentType="text/plain";
            String contentDisp="attachment; filename=\"6g.txt\"";
            // Set the presigned URL to expire after one hour.
            java.util.Date expiration = new java.util.Date();
            long expirationMinutes = 30L;
            long expTimeMillis = 1000 * 60 * expirationMinutes;
            expiration.setTime(expTimeMillis);
            System.out.println("***getPreSigned"
                    + " - bucketName:" + bucketName
                    + " - key:" + key
                    + " - expTimeMillis:" + expTimeMillis
                    + " - contentType:" + contentType
                    + " - contentDisp:" + contentDisp
            );
            String urlS = GetObjectPresign.getObjectPresign(s3Presigner, bucketName, key, expTimeMillis, contentType, contentDisp);
            System.out.println("---presign:" + urlS);
            
        } catch (Exception ex) {
            System.out.println("getPreSigned exception:" +ex);
            ex.printStackTrace();
            String exc = "Exception bucketName:" + bucketName + " - key=" + key;
            System.out.println("Presign exception:" + exc);
        }
    }
    
    public static void testStateAsync(
            V2Client v2client, 
            String bucketName,
            String key, 
            String sha256, 
            String upFilePath, 
            String downFilePath)
        throws TException
    { 
        try {
            System.out.println("***testStateAsync");
            doDelete(v2client,bucketName,key);
            //doUploadMultipart(v2client, bucketName, key, upFilePath, sha256);
            //doUpload(v2client, bucketName, key, upFilePath, sha256);
            doUploadAsync(v2client, bucketName, key, upFilePath, sha256);
            File downFile = new File(downFilePath);
            if (downFile.exists()) {
                downFile.delete();
            }
            doGetObjectAsync(v2client, bucketName, key, downFilePath, sha256);
            doDelete(v2client,bucketName,key);
        } catch (Exception e) {
            System.out.println("Exception:" + e);
            e.printStackTrace();
            throw new TException(e);
        }
    }    
    public static void testState(
            V2Client v2client, 
            String bucketName,
            String key, 
            String sha256, 
            String upFilePath, 
            String downFilePath)
        throws TException
    { 
        try {
            System.out.println("***test_state");
            doDelete(v2client,bucketName,key);
            //doUploadMultipart(v2client, bucketName, key, upFilePath, sha256);
            //doUpload(v2client, bucketName, key, upFilePath, sha256);
            doUploadMultipart(v2client, bucketName, key, upFilePath, sha256);
            File downFile = new File(downFilePath);
            if (downFile.exists()) {
                downFile.delete();
            }
            doGetObjectAsync(v2client, bucketName, key, downFilePath, sha256);
            doPresign(v2client, bucketName, key);
            doDelete(v2client,bucketName,key);
        } catch (Exception e) {
            System.out.println("Exception:" + e);
            e.printStackTrace();
            throw new TException(e);
        }
    }   
    public static void testStateSync(
            V2Client v2client, 
            String bucketName,
            String key, 
            String sha256, 
            String upFilePath, 
            String downFilePath)
        throws TException
    { 
        try {
            System.out.println("***testStateSync");
            
            doDelete(v2client,bucketName,key);
            //doUploadMultipart(v2client, bucketName, key, upFilePath, sha256);
            //doUpload(v2client, bucketName, key, upFilePath, sha256);
            doUploadSync(v2client, bucketName, key, upFilePath, sha256);
            File downFile = new File(downFilePath);
            if (downFile.exists()) {
                downFile.delete();
            }
            doGetObjectSync(v2client, bucketName, key, downFilePath, sha256);
            doDelete(v2client,bucketName,key);
        } catch (Exception e) {
            System.out.println("Exception:" + e);
            e.printStackTrace();
            throw new TException(e);
        }
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
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String endpoint = "https://uss-s3.sdsc.edu:443";

        V2Client v2client = V2Client.getMinio(accessKey, secretKey, endpoint);
        return v2client;
    }
    
    public static V2Client getClientAWS() 
            throws TException
    {
        System.out.println("---getClientAWS");
        V2Client v2client = V2Client.getAWS();
        return v2client;
    }

}