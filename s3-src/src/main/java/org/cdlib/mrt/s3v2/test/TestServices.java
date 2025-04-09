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
import java.net.URI;
import java.io.File;
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

public class TestServices {
    protected S3AsyncClient s3Client = null;
    public static GetSSM getSSM = new GetSSM();
    
    protected static final String uploadFileBig = "/apps/replic1/tomcatdata/big/6G.txt";
    protected static final String downloadBig = "/home/loy/tasks/awsv2/download.6G.txt";
    protected static final String downloadBigWasabi = "/home/loy/tasks/awsv2/wasabi.download.6G.txt";
    protected static final String sha256Big = "dd2a99e35c6ad550221bc3441d3ece19e0a84e2fccbf88246245f52f1a2e1cf6";
    protected static final String keyBig = "post|Big";
    
    protected static final String fileSmall = "/home/loy/t/ark+=99999=fk4806c36f/16/system/mrt-ingest.txt";
    protected static final String sha256Small = "47db2924db63fbaf78e9bf96fac08e96afbaa711a299c09d5ae13e8ee0b92452";
    protected static final String keySmall= "post|Small";
    
    public static void main(String[] args) 
            throws TException
    {
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
        
        //test_AWS_multiPartUpload(uploadFileBig, keyBig, sha256Big) ;
        
        test_sdsc_multiPartUpload(uploadFileBig, keyBig +"B", sha256Big); 
        
        //test_sdsc_putS3Object(fileSmall, keySmall, sha256Small) ; 
        
        //test_aws(args);
        
        //test_glacier(args);
        
        //test_minio(args);
        
        //test_wasabi(args);
    }
    
    public static void test_minio(String[] args) 
            throws TException
    {
        System.out.println("***test_minio");
        String endpoint = "https://cdl.s3.sdsc.edu:443";
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/sdsc-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/sdsc-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        
        System.out.println("keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String bucketName = "cdl.sdsc.stage";
        String key = "ark:/28722/bk0006w8m0c|1|system/mrt-erc.txt";
        Region region = Region.US_WEST_2;

        V2Client v2client = V2Client.getMinio(accessKey, secretKey, endpoint);
        S3Client s3Client = v2client.s3Client();
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        System.out.println(PropertiesUtil.dumpProperties("test", prop));
        s3Client.close();
    }
    
    public static void test_AWS_multiPartUpload(String filePath, String key, String sha256) 
            throws TException
    {
        System.out.println("***test_AWS_multiPartUpload");
        
        
        String bucketName = "uc3-s3mrt1001-stg";

        V2Client v2client = V2Client.getAWS();
        doUploadMultipart(v2client, bucketName, key, filePath, sha256);
       
    }
    
    public static void test_wasabi_uploadFileAsync(String filePath, String key, String sha256) 
            throws TException
    {
        System.out.println("***test_wasabi_uploadFileAsync");
        
        String endpoint = "https://s3.us-east-2.wasabisys.com:443";
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/wasabi-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/wasabi-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String bucketName = "uc3-wasabi-useast-2.stage";
        Region region = Region.US_EAST_2;

        V2Client v2client = V2Client.getWasabi(accessKey, secretKey, endpoint);
        doUpload(v2client, bucketName, key, filePath, sha256);
       
    }
    
    public static void test_wasabi_multiPartUpload(String filePath, String key, String sha256) 
            throws TException
    {
        System.out.println("***test_wasabi_multipartUpload");
        
        String endpoint = "https://s3.us-east-2.wasabisys.com:443";
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/wasabi-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/wasabi-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String bucketName = "uc3-wasabi-useast-2.stage";
        Region region = Region.US_EAST_2;

        V2Client v2client = V2Client.getWasabi(accessKey, secretKey, endpoint);
        doUploadMultipart(v2client, bucketName, key, filePath, sha256);
       
    }
    
    public static void test_sdsc_uploadFileAsync(String filePath, String key, String sha256) 
            throws TException
    {
        System.out.println("test_sdsc_uploadFileAsync"
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
        );
        System.out.println("***test_sdsc");
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String endpoint = "https://uss-s3.sdsc.edu:443";
        String bucketName = "cdl-sdsc-backup-stg";
        Region region = Region.US_WEST_2;

        V2Client v2client = V2Client.getMinio(accessKey, secretKey, endpoint);
        
        doUpload(v2client, bucketName, key, filePath, sha256);
    }
    
    public static void test_sdsc_multiPartUpload(String filePath, String key, String sha256) 
            throws TException
    {
        System.out.println("test_sdsc_multiPartUpload"
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
        );
        System.out.println("***test_sdsc");
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String endpoint = "https://uss-s3.sdsc.edu:443";
        String bucketName = "cdl-sdsc-backup-stg";
        Region region = Region.US_WEST_2;

        V2Client v2client = V2Client.getSDSC(accessKey, secretKey, endpoint);
        
        doUploadMultipart(v2client, bucketName, key, filePath, sha256);
    }
    
    public static void test_sdsc_putS3Object(String filePath, String key, String sha256) 
            throws TException
    {
        System.out.println("test_sdsc_putS3Object"
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
        );
        System.out.println("***test_sdsc");
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String endpoint = "https://uss-s3.sdsc.edu:443";
        String bucketName = "cdl-sdsc-backup-stg";
        Region region = Region.US_WEST_2;

        V2Client v2client = V2Client.getMinio(accessKey, secretKey, endpoint);
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
        
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        System.out.println(PropertiesUtil.dumpProperties("test", prop));
        s3Client.close();
        s3AsyncClient.close();
    }
    
    public static void test_minio_delete(String key) 
            throws TException
    {
        
        System.out.println("***test_minio_delete");
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/sdsc-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/sdsc-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        
        String endpoint = "https://cdl.s3.sdsc.edu:443";
        String bucketName = "cdl.sdsc.stage";
        Region region = Region.US_WEST_2;

        System.out.println("test_temp_putS3Object"
                + " - key=" + key
                + " - bucketName=" + bucketName
                + " - region=" + region
        );

        V2Client v2client = V2Client.getMinio(accessKey, secretKey, endpoint);
        S3Client s3Client = v2client.s3Client();
        
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        
        System.out.println(PropertiesUtil.dumpProperties("before delete", prop));
        
        DeleteObjectData.deleteS3Object(
            s3Client, 
            bucketName,
            key);
        System.out.println("after delete");
        
        Properties propAfter = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        
        if (propAfter == null) {
            System.out.println("prop null");
            
        } else if (propAfter.isEmpty()) {
            System.out.println("prop empty");
            
        } else {
            System.out.println(PropertiesUtil.dumpProperties("after delete", prop));
        }
    }
    
    public static void test_temp_delete(String key) 
            throws TException
    {
        
        String bucketName = "uc3-s3mrt1001-stg";
        Region region = Region.US_WEST_2;

        System.out.println("test_temp_delete"
                + " - key=" + key
                + " - bucketName=" + bucketName
                + " - region=" + region
        );
        

        V2Client v2client = V2Client.getAWS();
        doDelete(v2client,bucketName,key);
    }
    
    public static void test_wasabi_delete(String key) 
            throws TException
    {
        
        
        String endpoint = "https://s3.us-east-2.wasabisys.com:443";
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/wasabi-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/wasabi-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String bucketName = "uc3-wasabi-useast-2.stage";
        Region region = Region.US_EAST_2;

        System.out.println("test_wasabi_delete"
                + " - key=" + key
                + " - bucketName=" + bucketName
                + " - region=" + region
        );

        V2Client v2client = V2Client.getWasabi(accessKey, secretKey, endpoint);
        doDelete(v2client, bucketName, key);
    }
    
    public static void test_minio_putS3Object(String filePath, String key, String sha256) 
            throws TException
    {
        System.out.println("***test_minio_putS3Object");
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/sdsc-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/sdsc-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        
        String endpoint = "https://cdl.s3.sdsc.edu:443";
        String bucketName = "cdl.sdsc.stage";
        Region region = Region.US_WEST_2;

        System.out.println("test_temp_putS3Object"
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
                + " - bucketName=" + bucketName
                + " - region=" + region
        );


        V2Client v2client = V2Client.getMinio(accessKey, secretKey, endpoint);
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
        
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        System.out.println(PropertiesUtil.dumpProperties("test", prop));
        s3Client.close();
        s3AsyncClient.close();
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

    
    public static void test_temp_uploadFileAsync(String filePath, String key, String sha256) 
            throws TException
    {
        
        String bucketName = "uc3-s3mrt1001-stg";
        Region region = Region.US_WEST_2;

        System.out.println("test_temp_uploadFileAsync"
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
                + " - bucketName=" + bucketName
                + " - region=" + region
        );
        V2Client v2client = V2Client.getAWS();
        doUpload(v2client, bucketName, key, filePath, sha256);
    }

    
    public static void test_temp_multiparUpload(String filePath, String key, String sha256) 
            throws TException
    {
        
        String bucketName = "uc3-s3mrt1001-stg";
        Region region = Region.US_WEST_2;

        System.out.println("test_temp_uploadFileAsync"
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
                + " - bucketName=" + bucketName
                + " - region=" + region
        );
        V2Client v2client = V2Client.getAWS();
        doUploadMultipart(v2client, bucketName, key, filePath, sha256);
    }
    
    public static void test_temp_download(String filePath, String key, String sha256) 
            throws TException
    {
        
        String bucketName = "uc3-s3mrt1001-stg";
        Region region = Region.US_WEST_2;

        System.out.println("test_temp_download"
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
                + " - bucketName=" + bucketName
                + " - region=" + region
        );
        V2Client v2client = V2Client.getAWS();
        doGetObjectAsync(v2client, bucketName, key, filePath, sha256);
    }
    
    public static void test_wasabi_download(String filePath, String key, String sha256) 
            throws TException
    {
                System.out.println("***test_wasabi_download");
        
        String endpoint = "https://s3.us-east-2.wasabisys.com:443";
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/wasabi-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/wasabi-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String bucketName = "uc3-wasabi-useast-2.stage";
        Region region = Region.US_EAST_2;
        
        V2Client v2client = V2Client.getWasabi(accessKey, secretKey, endpoint);
        doGetObjectAsync(v2client, bucketName, key, filePath, sha256);
    }
    
    public static void test_temp_downloadS3Object(String filePath, String key, String sha256) 
            throws TException
    {
        String bucketName = "uc3-s3mrt1001-stg";
        Region region = Region.US_WEST_2;

        System.out.println("test_temp_putS3Object"
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
                + " - bucketName=" + bucketName
                + " - region=" + region
        );
        

        V2Client v2client = V2Client.getAWS();
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
        
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        System.out.println(PropertiesUtil.dumpProperties("test", prop));
        s3Client.close();
        s3AsyncClient.close();
    }

    /**
     * Lists the tags associated with an Amazon S3 object.
     *
     * @param s3 the S3Client object used to interact with the Amazon S3 service
     * @param bucketName the name of the S3 bucket that contains the object
     * @param keyName the key (name) of the S3 object
     */
    public static HeadObjectResponse getHeadResponse(S3Client s3, String bucketName, String keyName) 
            throws TException
    {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest
                .builder()
                .key(keyName)
                .bucket(bucketName)
                .build();

            HeadObjectResponse headResponse  = s3.headObject(headObjectRequest);
            String responsesha256 = headResponse.checksumSHA256();
            Map<String, String> meta  = headResponse.metadata();
            Set<String> keys = meta.keySet();
            for (String key : keys) {
                String value = meta.get(key);
                System.out.println(key+ "=" + value);
            }
            System.out.println("responsesha256=" + responsesha256);
            
            Long size = headResponse.contentLength();
            System.out.println("size=" + size);
            
            String mimeType = headResponse.contentType();
            System.out.println("mimeType=" + mimeType);
            return headResponse;
            

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
            throw new TException(e);
        }
    }
    
    public static Properties getObjectMeta (
            S3Client s3, 
            String bucketName,
            String key)
        throws TException
    {
        Properties prop = new Properties();
        try {
            HeadObjectResponse headResponse = getHeadResponse(s3, bucketName, key);
            
            Map<String, String> metaMap  = headResponse.metadata();
            /*
            GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucketName, key);
            ObjectMetadata metadata = s3Client.getObjectMetadata(request);
            */
            String sha256 = metaMap.get(key);
            addProp(prop, "sha256", sha256);
            Long contentLength = headResponse.contentLength();
            addProp(prop, "size", "" + contentLength);
            addProp(prop, "bucket", bucketName);
            addProp(prop, "key", key);
            String etag = headResponse.eTag();
            addProp(prop, "etag", etag);
            Instant instantDate = headResponse.lastModified();
            Date date = Date.from(instantDate);
            String isoDate = DateUtil.getIsoDate(date);
            addProp(prop, "modified", isoDate);
            
            addProp(prop, "md5", headResponse.eTag());
            String storageClass = headResponse.storageClassAsString();
            addProp(prop, "storageClass", storageClass);
            
            //addProp(prop, "maxErrRetry", "" + getMaxErrRetry());
            String expiration = headResponse.expiresString();
            addProp(prop, "expires", expiration);
            
            if (storageClass.contains("GLACIER") && (expiration != null)) {
                if (expiration.contains("ongoing-request=\"false\"")) {
                    addProp(prop, "glacierRestore", "complete");
                } else if (expiration.contains("ongoing-request=\"true\"")) {
                    addProp(prop, "glacierRestore", "ongoing");
                    addProp(prop, "ongoingRestore", "true");
                }
            }
            return prop;
            
        } catch (Exception ex) {
            if (ex.toString().contains("404")) {
                return new Properties();
            }
            //CloudResponse response = new CloudResponse(bucketName, key);
            //awsHandleException(response, ex);
            return null;
        }
    }
    
    protected static void addProp(Properties prop, String key, String value)
    {
        if (value == null) return;
        prop.setProperty(key, value);
    }
    
    protected static void doUpload(
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
        
        PutObjectData.uploadFileAsync(
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
        GetObject.downloadObjectTransfer(s3AsyncClient, bucketName, key, downloadBig);
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
}