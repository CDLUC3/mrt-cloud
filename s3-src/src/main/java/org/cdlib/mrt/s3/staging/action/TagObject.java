/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3.staging.action;

/**
 *
 * @author loy
 */

import org.cdlib.mrt.s3v2.action.*;
import org.cdlib.mrt.utility.TException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import software.amazon.awssdk.services.s3.model.S3Exception;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.s3v2.tools.GetSSM;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging; 
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;

/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class TagObject {
    protected static final Logger logger = LogManager.getLogger(); 
    public static GetSSM getSSM = new GetSSM();
    
    public static void setTag(S3Client s3Client, String bucketName, String keyName, String tagKey, String tagValue)
        throws TException
    {
        HashMap<String,String> tagMap = getObjectTagsHash (s3Client, bucketName, keyName );
        addTag(tagKey, tagValue, tagMap);
        ArrayList<Tag> tagArray = buldListTags(tagMap);
        writeTags(s3Client, bucketName, keyName, tagArray);
    }
    
    public static void setTags(S3Client s3Client, String bucketName, String keyName, HashMap<String,String> tagMap)
        throws TException
    {
        ArrayList<Tag> tagArray = buldListTags(tagMap);
        writeTags(s3Client, bucketName, keyName, tagArray);
    }
 

    public static List<Tag> getObjectTags (S3Client s3Client, String bucketName, String keyName ) 
        throws TException
    {
        logger.trace("***getObjectTags:"
                + " - keyName=" + keyName
                + " - bucketName=" + bucketName
        );
        try {
            GetObjectTaggingRequest getTaggingRequest = GetObjectTaggingRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();

            GetObjectTaggingResponse getTaggingResponse = s3Client.getObjectTagging(getTaggingRequest);
            List<Tag> existingTags = new ArrayList<>(getTaggingResponse.tagSet());
            return existingTags;
            
        } catch (S3Exception e) {
          System.err.println(e.awsErrorDetails().errorMessage());
           throw new TException(e);
        }
    }

    public static HashMap<String,String> getObjectTagsHash (S3Client s3Client, String bucketName, String keyName ) 
        throws TException
    {
        logger.trace("***getObjectTags:"
                + " - keyName=" + keyName
                + " - bucketName=" + bucketName
        );
        try {
            HashMap tagMap = new HashMap<>();
            GetObjectTaggingRequest getTaggingRequest = GetObjectTaggingRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();

            GetObjectTaggingResponse getTaggingResponse = s3Client.getObjectTagging(getTaggingRequest);
            String jsonS = getTaggingResponse.toString();
            List<Tag> existingTags = new ArrayList<>(getTaggingResponse.tagSet());
            for (Tag tag : existingTags) {
                String tagKey = tag.key();
                String tagValue = tag.value();
                tagMap.put(tagKey, tagValue);
                System.out.println("getObjectTagsHash"
                        + " - tagKey=" + tagKey
                        + " - tagValue=" + tagValue
                );
            }
            return tagMap;
            
        } catch (S3Exception e) {
          System.err.println(e.awsErrorDetails().errorMessage());
           throw new TException(e);
        }
    }
    
    protected static void addTag(String tagKey, String tagValue, HashMap<String,String> tagMap)
        throws TException
    {
        tagMap.put(tagKey, tagValue);
    }
    
    protected static Tag getS3Tag(String key, String value)
        throws TException
    {
        return Tag.builder().key(key).value(value).build();
    }
    
    protected static ArrayList<Tag> buldListTags(HashMap<String, String> hash)
        throws TException
    {
        ArrayList<Tag>tags = new ArrayList<>();
        Set<String> keys = hash.keySet();
        for (String tagKey : keys) {
            String tagValue = hash.get(tagKey);
            System.out.println("getObjectTagsHash"
                    + " - tagKey=" + tagKey
                    + " - tagValue=" + tagValue
            );
            Tag tag = getS3Tag(tagKey, tagValue);
            tags.add(tag);
        }
        return tags;
    }
    
    protected static void writeTags(S3Client s3Client, String bucketName, String keyName, List<Tag> existingTags)
        throws TException
    {
        try {
            Tagging tagging = Tagging.builder().tagSet(existingTags /* or newTags */).build();

            PutObjectTaggingRequest putTaggingRequest = PutObjectTaggingRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .tagging(tagging)
                    .build();

            s3Client.putObjectTagging(putTaggingRequest);
            System.out.println("Object tags updated successfully.");
            
        } catch (S3Exception e) {
          System.err.println(e.awsErrorDetails().errorMessage());
           throw new TException(e);
        }
    }
}

