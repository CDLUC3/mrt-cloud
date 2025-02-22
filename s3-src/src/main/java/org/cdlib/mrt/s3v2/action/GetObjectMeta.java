/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3v2.action;

/**
 *
 * @author loy
 */
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

import org.cdlib.mrt.s3.service.CloudResponse;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class GetObjectMeta {
    protected S3Client s3Client = null;
    private static final Logger log4j = LogManager.getLogger();
    
    public static void main_5001(String[] args) {
        String bucketName = "uc3-s3mrt5001-stg";
        String keyName = "ark:/99999/fk47387525|1|system/mrt-ingest.txt";
        Region region = Region.US_WEST_2;

        S3Client s3AWS = S3Client.builder()
                          .region(region)
                          .endpointOverride(URI.create("https://s3.us-west-2.amazonaws.com"))
                          .forcePathStyle(true)
                          .build();

        
        s3AWS.close();
    }
    
    public static void main_9501(String[] args) {
        String bucketName = "uc3-s3mrt5001-stg";
        String keyName = "ark:/99999/fk47387525|1|system/mrt-ingest.txt";
        Region region = Region.US_WEST_2;

        S3Client s3AWS = S3Client.builder()
                          .region(region)
                          .endpointOverride(URI.create("https://s3.us-west-2.amazonaws.com"))
                          .forcePathStyle(true)
                          .build();

        
        s3AWS.close();
    }

    public GetObjectMeta(S3Client s3Client) 
    {
        this.s3Client = s3Client;
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
            log4j.trace("responsesha256=" + responsesha256);
            
            Long size = headResponse.contentLength();
            log4j.trace("size=" + size);
            
            String mimeType = headResponse.contentType();
            log4j.trace("mimeType=" + mimeType);
            return headResponse;
            

        } catch (S3Exception e) {
            //System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    }
    
    public static Properties getObjectMeta (
            S3Client s3, 
            String bucketName,
            String key)
    {
        Properties prop = new Properties();
        try {
            HeadObjectResponse headResponse = getHeadResponse(s3, bucketName, key);
            
            Map<String, String> metaMap  = headResponse.metadata();
            /*
            GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucketName, key);
            ObjectMetadata metadata = s3Client.getObjectMetadata(request);
            */
            String sha256 = metaMap.get("sha256");
            log4j.trace("SHA256Meta:" + sha256);
            addProp(prop, "sha256", sha256);
            //System.out.println(PropertiesUtil.dumpProperties("TEST1", prop));
            Long contentLength = headResponse.contentLength();
            addProp(prop, "size", "" + contentLength);
            addProp(prop, "bucket", bucketName);
            addProp(prop, "key", key);
            String etag = headResponse.eTag();
            System.out.println("before etag:" + etag);
            
            //etag = etag.replace("\"","");
            //System.out.println("after   etag:" + etag);
            addProp(prop, "etag", etag);
            Instant instantDate = headResponse.lastModified();
            Date date = Date.from(instantDate);
            String isoDate = DateUtil.getIsoDate(date);
            addProp(prop, "modified", isoDate);
            
            addProp(prop, "md5", headResponse.eTag());
            String storageClassS = headResponse.storageClassAsString();
            if (storageClassS == null) storageClassS = StorageClass.STANDARD.toString();
            log4j.trace("***assigned storageClass:" + storageClassS);
            //Note storageClass only set when NOT standard S3 - glacier = "GLACIER"
            addProp(prop, "storageClass", storageClassS);
            
            //addProp(prop, "maxErrRetry", "" + getMaxErrRetry());
            String expiration = headResponse.expiresString();
            addProp(prop, "expires", expiration);
            String restore = headResponse.restore();
            addProp(prop, "restore", restore); //restore="ongoing-request="true""
            
            if (storageClassS.contains("GLACIER") && (restore != null)) {
                if (restore.contains("ongoing-request=\"false\"")) {
                    addProp(prop, "glacierRestore", "complete");
                } else if (restore.contains("ongoing-request=\"true\"")) {
                    addProp(prop, "glacierRestore", "ongoing");
                    addProp(prop, "ongoingRestore", "true");
                }
            }
            return prop;
            
        } catch (Exception ex) {
            //System.out.println("Exception:" + ex);
            //ex.printStackTrace();
            if (ex.toString().contains("404")) {
                return new Properties();
            }
            //CloudResponse response = new CloudResponse(bucketName, key);
            //awsHandleException(response, ex);
            ex.printStackTrace();
            return null;
        }
    }
    
    protected static void addProp(Properties prop, String key, String value)
    {
        if (value == null) return;
        prop.setProperty(key, value);
    }
    
    public static StorageClass getStorageClass (
            S3Client s3, 
            String bucketName,
            String key)
    {
        Properties prop = new Properties();
        try {
            HeadObjectResponse headResponse = getHeadResponse(s3, bucketName, key);
            
            Map<String, String> metaMap  = headResponse.metadata();
            /*
            GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucketName, key);
            ObjectMetadata metadata = s3Client.getObjectMetadata(request);
            */
 
            String storageClassS = headResponse.storageClassAsString();
            
            StorageClass storageClass = null;
            if (storageClassS == null) {
                storageClass = StorageClass.STANDARD;
                
            } else {
                storageClass = StorageClass.valueOf(storageClassS);
            }
            
            return storageClass;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            
            return null;
        }
    }
}