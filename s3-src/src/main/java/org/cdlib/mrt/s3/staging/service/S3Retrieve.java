/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3.staging.service;

/**
 *
 * @author loy
 */
import org.cdlib.mrt.s3.service.*;
import org.cdlib.mrt.s3.staging.action.Url2S3;
import org.cdlib.mrt.s3.staging.action.TagObject;
import org.cdlib.mrt.s3.staging.service.RetrieveResponse;
import org.cdlib.mrt.s3.staging.tools.ChecksumHandler;

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
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.time.Instant;
import java.util.ArrayList;

import org.cdlib.mrt.s3v2.tools.GetSSM;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.utility.Checksums;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.CloudList.CloudEntry;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.staging.tools.DeletePrefixList;
import org.cdlib.mrt.s3.staging.tools.UploadS3Zip;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
import org.cdlib.mrt.s3v2.tools.GetSSM;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONObject;

import software.amazon.awssdk.services.s3.S3AsyncClient;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class S3Retrieve {
    
    private NodeIO nodeIO = null;
    private static int failCnt = 0;
    protected AWSS3V2Cloud service = null;
    protected String bucket = null;
    protected Url2S3 url2S3 = null;
    protected LoggerInf logger = null;
    
    final static int BUFSIZE = 75 * 1024 * 1024;
    protected long stagingNode = 8501;
    
    public static S3Retrieve getS3Retrieve(LoggerInf logger)
        throws TException
    {
        S3Retrieve s3Retrieve = new S3Retrieve(8501, logger);
        return s3Retrieve;
    }
    
    protected S3Retrieve(long stagingNode, LoggerInf logger)
        throws TException
    {
        this.logger = logger;
        this.stagingNode = stagingNode;
        //nodeIO = NodeIO.getNodeIOConfig("yaml:2", logger) ;
        nodeIO = NodeIO.getNodeIOConfig("yaml:2", logger) ;
        NodeIO.AccessNode retrievingAccessNode = nodeIO.getAccessNode(stagingNode);
        if (retrievingAccessNode == null) {
            throw new TException.INVALID_OR_MISSING_PARM("stageNode accessNode not found");
        }
        service = (AWSS3V2Cloud)retrievingAccessNode.service;
        bucket = retrievingAccessNode.container;
        url2S3 = service.getUrl2S3(bucket);

    }
    
    
    
    public RetrieveResponse putUrl(String url, String key, String[] digestTypes) 
            throws TException
    {
        
        RetrieveResponse upResponse = url2S3.uploadFileParts(url, key, digestTypes);
        return upResponse;
    }
    
    public RetrieveResponse getFile(String key, File  downloadFile, String[] digestTypes) 
            throws TException
    {
        RetrieveResponse downResponse = new RetrieveResponse();
        try {
            downResponse.startMs = System.currentTimeMillis();
            CloudResponse cloudResponse = service.getTransfer(bucket, key, downloadFile);
            if (cloudResponse.getException() != null) {
                throw new TException(cloudResponse.getException());
            }
            downResponse.completeMultiPartAdd = System.currentTimeMillis();
            downResponse.totalReadBytes = cloudResponse.getStorageSize();
            downResponse.startIS = downResponse.completeMultiPartAdd;
            if ((digestTypes != null) && (digestTypes.length > 0)) {
                ChecksumHandler checksumHandler = getFileDigests(downloadFile, digestTypes);
                downResponse.setChecksumHandler(checksumHandler);
                downResponse.setFromChecksumHandler();
            }
            downResponse.beginIS = System.currentTimeMillis();
            downResponse.setStatusOK();
            downResponse.endMs = System.currentTimeMillis();
            return downResponse;
       
        } catch (Exception ex) {
            downResponse.setException(ex);
            downResponse.setStatusFail();
            
        } finally {
            return downResponse;
        }
    }
    
    public static ChecksumHandler getFileDigests(File downloadFile, String[] digestTypes)
        throws TException
    {
        ChecksumHandler checksumHandler = ChecksumHandler.getChecksumHandler(digestTypes);
        ByteBuffer buffer = ByteBuffer.allocate(BUFSIZE);
        try {
            InputStream inFiletream = new FileInputStream(downloadFile);
        
            while (true) {
                long bytesRead = checksumHandler.fillBuff(inFiletream, buffer, BUFSIZE);
                if (bytesRead < BUFSIZE) break;
                buffer.clear();
            }
            checksumHandler.finishDigests();
            return checksumHandler;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public JSONObject deletePrefix(String prefix)
        throws TException
    {
        JSONObject responseJson = null;
        try {
            responseJson = DeletePrefixList.deleteList(prefix);
            System.out.println("JSON:\n"  + responseJson.toString(2));
            return responseJson;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }    
    }   
    
    public HashMap<String, String> getTagHash(String key)
        throws TException
    { 
        return TagObject.getObjectTagsHash(service.getS3Client(), bucket, key);
    }
    
    public void addTagsHash(HashMap<String, String> tags, String key)
        throws TException
    { 
        TagObject.setTags(service.getS3Client(), bucket, key, tags);
    }
    
    
    public String getTag(String key, String tagKey)
        throws TException
    { 
        HashMap<String, String> tagMap = getTagHash(key);
        return tagMap.get(tagKey);
    }
    
    public void addTag(String key, String tagKey, String tagValue)
        throws TException
    {
        TagObject.setTag(service.getS3Client(), bucket, key, tagKey, tagValue);
        System.out.println("addTag"
                + " - key=" + key
                + " - tagKey=" + tagKey
                + " - tagValue=" + tagValue
        );
    }
    
    public void setPrefixTag(String keyPrefix, String tagKey, String tagValue)
        throws TException
    {
        CloudResponse response = service.getObjectList (bucket, keyPrefix);
        CloudList cloudList = response.getCloudList();
        List<CloudEntry> list = cloudList.getList();
        for (CloudEntry entry : list) {
            String key = entry.getKey();
            addTag(key, tagKey, tagValue);
        }
    }
    
    public ArrayList<RetrieveResponse> uploadZip(String zipKey, String zipPrefix, String[] digestTypes)
        throws TException
    {        
        try {
            S3Client s3Client = service.getS3Client();
        
            UploadS3Zip uploadS3Zip = new UploadS3Zip(s3Client, bucket, digestTypes);
       
            //InputStream httpInStream = HttpGet.getStream(zipUrl, 60000,  logger);
    
            CloudResponse response = new CloudResponse();
            InputStream zipStream = service.getObjectStreaming(bucket, zipKey, response);
            if (response.getException() != null) {
                throw response.getException();
            }
            uploadS3Zip.uploadZip(zipPrefix, zipStream);
            ArrayList<RetrieveResponse> responseArr = uploadS3Zip.getRetrieveArr();
            int rCnt = 0;
            for (RetrieveResponse retrieveResponse : responseArr) {
                rCnt++;
                String zKey = retrieveResponse.getKey();
                long zLength = retrieveResponse.length();
                String zSha256 = retrieveResponse.getSha256();
                String zMd5 = retrieveResponse.getMd5();
                long zUpMs = retrieveResponse.totalTimeMs();
                System.out.println("zipUp(" + rCnt + "):]\n"
                        + " - key:" + zKey + "\n"
                        + " - length:" + zLength + "\n"
                        + " - sha256:" + zSha256 + "\n"
                        + " - md5:" + zMd5 + "\n"
                        + " - mS:" + zUpMs + "\n"
                );
            }
            return responseArr;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
                  
}