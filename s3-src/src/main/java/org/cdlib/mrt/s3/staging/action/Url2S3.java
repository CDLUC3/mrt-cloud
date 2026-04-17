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
import org.cdlib.mrt.s3.staging.service.RetrieveResponse;
import software.amazon.awssdk.core.sync.RequestBody;
import java.util.List;
import java.util.ArrayList;

import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import java.nio.ByteBuffer;
import java.io.InputStream;
import org.apache.http.HttpEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.s3.tools.ChecksumHandler;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.TException;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class Url2S3 {
    protected static final Logger logger = LogManager.getLogger(); 
    
    final static Long MB = 1024L * 1024 * 1024;
    final static int BUFSIZE = 75 * 1024 * 1024;
    
    protected String urlS = null;
    protected S3Client s3Client = null;
    protected String bucketName = null;
    //protected String keyName = null;
    //protected String [] digestTypesS = null;
    // see https://www.baeldung.com/aws-s3-multipart-upload
    
    
    public static Url2S3 getUrl2S3 (
            S3Client s3Client,
            String bucketName)
        throws TException
    {
        return new Url2S3(s3Client, bucketName);
    }
    
    protected Url2S3(
            S3Client s3Client,
            String bucketName)
        throws TException
    {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }
    
    
    public RetrieveResponse uploadFileParts(
            String urlS,
            String keyName,
            String [] digestTypesS)
        throws TException
    { 
        try {
            InputStream urlStream = url2Stream( urlS);
            StreamToS3 streamToS3 = StreamToS3.getStreamToS3 (s3Client, bucketName);
            RetrieveResponse retrieveResponse = streamToS3.uploadFileParts(urlStream, keyName, digestTypesS);
            streamToS3.addDigestTags(retrieveResponse.getKey(), retrieveResponse);
            return retrieveResponse;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
        }
    }
    
    public static InputStream url2Stream( String urlS, long startByte, long endByte)
        throws TException
    {
        int timeout = 60000;
        try {
            HttpEntity entity = HTTPUtil.getObjectEntity(urlS, timeout, startByte, endByte);
            return entity.getContent();

        } catch(Exception ex) {
            String err = "url2Stream - Exception:" + ex + " - name:" + urlS;
            throw new TException.GENERAL_EXCEPTION( err);


        }

    }
    
    public static InputStream url2Stream( String urlS)
        throws TException
    {
        int timeout = 1800000;
        try {
            HttpEntity entity = HTTPUtil.getObjectEntity(urlS, timeout);
            return entity.getContent();

        } catch(Exception ex) {
            String err = "url2Stream - Exception:" + ex + " - name:" + urlS;
            throw new TException.GENERAL_EXCEPTION( err);


        }

    }
}