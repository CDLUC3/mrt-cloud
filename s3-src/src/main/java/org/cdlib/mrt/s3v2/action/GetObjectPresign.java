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


import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.time.Instant;

import org.cdlib.mrt.s3.service.CloudResponse;
import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import software.amazon.awssdk.core.RequestOverrideConfiguration;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class GetObjectPresign {
    protected S3Client s3Client = null;
    
    protected static final Logger logger = LogManager.getLogger(); 
    

    public static String getObjectPresign(S3Presigner presigner, String bucketName, String keyName, long expirationMs,
            String contentType,
            String contentDisposition) 
    {
       
     // Create an S3Presigner using the default region and credentials.
      // This is usually done at application startup, because creating a presigner can be expensive.
     
            
        
        //HashMap<String, ArrayList<String>> map = getReplaceMap(contentType, contentDisposition);
        GetObjectRequest getObjectRequest = null;
        if ((contentType != null) && (contentDisposition != null)){
            // Create a GetObjectRequest to be pre-signed
            getObjectRequest =
              GetObjectRequest.builder()
                              .bucket(bucketName)
                              .key(keyName)
                      .responseContentType(contentType)
                      .responseContentDisposition(contentDisposition)
                              .build();
            
        } else if (contentType != null) { 
            getObjectRequest =
              GetObjectRequest.builder()
                              .bucket(bucketName)
                              .key(keyName)
                      .responseContentType(contentType)
                              .build();
            
        } else if (contentDisposition != null) { 
            getObjectRequest =
                GetObjectRequest.builder()
                              .bucket(bucketName)
                              .key(keyName)
                      .responseContentDisposition(contentDisposition)
                              .build();
        } else {
            getObjectRequest =
              GetObjectRequest.builder()
                              .bucket(bucketName)
                              .key(keyName)
                              .build();
            
        }
        

         // Create a GetObjectPresignRequest to specify the signature duration
        GetObjectPresignRequest getObjectPresignRequest =
            GetObjectPresignRequest.builder()
                                 .signatureDuration(Duration.ofMillis(expirationMs))
                                 .getObjectRequest(getObjectRequest)
                                 .build();
      
        // Generate the presigned request
        PresignedGetObjectRequest presignedGetObjectRequest =
          presigner.presignGetObject(getObjectPresignRequest);

      // Log the presigned URL, for example.
        System.out.println("Presigned URL: " + presignedGetObjectRequest.url());

      // It is recommended to close the S3Presigner when it is done being used, because some credential
      // providers (e.g. if your AWS profile is configured to assume an STS role) require system resources
      // that need to be freed. If you are using one S3Presigner per application (as recommended), this
      // usually is not needed.
        //presigner.close();
        return presignedGetObjectRequest.url().toExternalForm();
    }
    
    public String createPresignedGetUrl(String bucketName, String keyName, long expirationMs) 
    {
        //https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/RequestOverrideConfiguration.Builder.html#headers(java.util.Map)
        
        try (S3Presigner presigner = S3Presigner.create()) {
            //RequestOverrideConfiguration overrideConfigurat = RequestOverrideConfiguration.
            
          
            GetObjectRequest objectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMillis(expirationMs))  // The URL will expire in 10 minutes.
                    .getObjectRequest(objectRequest)
                    .build();

            PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);
            logger.info("Presigned URL: [{}]", presignedRequest.url().toString());
            logger.info("HTTP method: [{}]", presignedRequest.httpRequest().method());

            return presignedRequest.url().toExternalForm();
        }
    }
    
    public String createPresignedGetUrl2(String bucketName, String keyName, long expirationMs) 
    {
            // Create an S3Presigner using the default region and credentials.
             // This is usually done at application startup, because creating a presigner can be expensive.
             S3Presigner presigner = S3Presigner.create();

             // Create a GetObjectRequest to be pre-signed
             GetObjectRequest getObjectRequest =
                     GetObjectRequest.builder()
                                     .bucket("my-bucket")
                                     .key("my-key")
                                     .build();

             // Create a GetObjectPresignRequest to specify the signature duration
             GetObjectPresignRequest getObjectPresignRequest =
                 GetObjectPresignRequest.builder()
                                        .signatureDuration(Duration.ofMinutes(10))
                                        .getObjectRequest(getObjectRequest)
                                        .build();

             // Generate the presigned request
             PresignedGetObjectRequest presignedGetObjectRequest =
                 presigner.presignGetObject(getObjectPresignRequest);

             // Log the presigned URL, for example.
             System.out.println("Presigned URL: " + presignedGetObjectRequest.url());

             // It is recommended to close the S3Presigner when it is done being used, because some credential
             // providers (e.g. if your AWS profile is configured to assume an STS role) require system resources
             // that need to be freed. If you are using one S3Presigner per application (as recommended), this
             // usually is not needed.
             presigner.close();
             return presignedGetObjectRequest.url().toExternalForm();
        }
    
    public static Map<String, List<String>> getOverrideMap(String contentType, String contentDisposition)
    {
        if (!StringUtil.isAllBlank(contentType) || !StringUtil.isAllBlank(contentDisposition)) {
            Map<String, List<String>> map = new HashMap<>();
            if (!StringUtil.isAllBlank(contentType)) {
                List<String> headerContent = new ArrayList<>();
                headerContent.add(contentType);
                map.put("Content-Type", headerContent);
            }
            if (!StringUtil.isAllBlank(contentDisposition)) {
                List<String> headerContent = new ArrayList<>();
                headerContent.add(contentDisposition);
                map.put("Content-Disposition", headerContent);
            }
            return map;
        }
        else return null;
    }
}
