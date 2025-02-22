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
import software.amazon.awssdk.services.s3.model.StorageClass;
import java.net.URI;
import java.util.Date;
import java.time.Instant;

import org.cdlib.mrt.s3v2.tools.GetSSM;
import org.cdlib.mrt.s3.service.CloudResponse;
import java.util.Map;
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

public class TestObjectMeta {
    protected S3AsyncClient s3Client = null;
    public static GetSSM getSSM = new GetSSM();
    
    public static void main(String[] args) 
            throws TException
    {
        test_minio(args);
        if (true) return;
        test_aws(args);
        if (true) return;
        test_glacier(args);
        
        test_minio(args);
        
        test_wasabi(args);
    }
    
    public static void test_aws(String[] args) 
            throws TException
    {
        System.out.println("***test_aws");
        String bucketName = "uc3-s3mrt5001-stg";
        String key = "ark:/99999/fk47387525|1|system/mrt-ingest.txt";
        Region region = Region.US_WEST_2;

        V2Client v2client = V2Client.getAWS();
        S3Client s3Client = v2client.s3Client();
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        System.out.println(PropertiesUtil.dumpProperties("test", prop));
        doStorageClass(s3Client, bucketName, key) ;
        CloudResponse response = CloudResponse.get(prop);
        response.dumpVar("AWS dumpVar");
        s3Client.close();
    }
    
    public static void test_glacier(String[] args) 
            throws TException
    {
        System.out.println("***test_glacier");
        String bucketName = "uc3-s3mrt6001-stg";
        String key = "ark:/28722/bk0006w8m0c|1|system/mrt-erc.txt";
        Region region = Region.US_WEST_2;

        V2Client v2client = V2Client.getAWS();
        S3Client s3Client = v2client.s3Client();
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        System.out.println(PropertiesUtil.dumpProperties("test", prop));
        doStorageClass(s3Client, bucketName, key) ;
        s3Client.close();
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
         String key = "ark:/20775/bb0547060t|1|producer/1-1.pdf";
        //String key = "ark:/28722/bk0006w8m0c|1|system/mrt-erc.txt";
        Region region = Region.US_WEST_2;

        V2Client v2client = V2Client.getMinio(accessKey, secretKey, endpoint);
        S3Client s3Client = v2client.s3Client();
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        System.out.println(PropertiesUtil.dumpProperties("test", prop));
        doStorageClass(s3Client, bucketName, key) ;
        s3Client.close();
    }
    
    public static void test_wasabi(String[] args) 
            throws TException
    {
        System.out.println("***test_wasabi");
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
        String key = "ark:/28722/bk0006w8m0c|1|system/mrt-erc.txt";
        Region region = Region.US_EAST_2;

        V2Client v2client = V2Client.getWasabi(accessKey, secretKey, endpoint);
        S3Client s3Client = v2client.s3Client();
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        System.out.println(PropertiesUtil.dumpProperties("test", prop));
        doStorageClass(s3Client, bucketName, key) ;
        s3Client.close();
    }
    
    public static void test_sdsc(String[] args) 
            throws TException
    {
        System.out.println("***test_sdsc");
        String aws_access_key_id = "000000050012f3304cf6";
        String aws_secret_access_key = "xdWhD/VoOzLUq4Nn6z3RQ99cEok27PiVMDYPJnog";
        String endpoint = " https://uss-s3.sdsc.edu:443";
        String bucketName = "cdl-sdsc-backup-stg";
        String key = "ark:/28722/bk0006w8m0c|1|system/mrt-erc.txt";
        Region region = Region.US_WEST_2;

        V2Client v2client = V2Client.getMinio(aws_access_key_id, aws_secret_access_key, endpoint);
        S3Client s3Client = v2client.s3Client();
        Properties prop = GetObjectMeta.getObjectMeta (
            s3Client, 
            bucketName,
            key);
        System.out.println(PropertiesUtil.dumpProperties("test", prop));
        s3Client.close();
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
    
    public Properties getObjectMeta (
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
    
    public static void doStorageClass(S3Client s3Client, String bucketName, String keyName) 
            throws TException
    {
        
        StorageClass storageClass = GetObjectMeta.getStorageClass(s3Client, bucketName, keyName);
        System.out.println("storageClass=" + storageClass.toString());
    }
}