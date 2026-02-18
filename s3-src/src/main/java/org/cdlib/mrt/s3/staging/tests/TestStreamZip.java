/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3.staging.tests;

/**
 *
 * @author loy
 */
import org.cdlib.mrt.s3.staging.tools.*;
import java.io.File;
import java.net.URI;
import java.net.URL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.utility.TException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.io.IOException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.ArrayList;
import org.cdlib.mrt.utility.HttpGet;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.staging.service.RetrieveResponse;
import org.cdlib.mrt.s3.staging.service.S3Retrieve;
import org.cdlib.mrt.s3.staging.action.StreamToS3;
import org.cdlib.mrt.s3v2.action.V2Client;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONObject;
//import static org.cdlib.mrt.s3v2.test.TestGetUrl2S3.get_aws_bigNS;
import software.amazon.awssdk.services.s3.S3Client;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class TestStreamZip {
    protected static final Logger log4j = LogManager.getLogger(); 
    protected static final long defaultNode = 8501;
    //protected CloudStoreInf service = null;
    //protected String bucket = null;
    //protected File containerFile = null;
    //protected String prefix = null;
    //protected int count = 0;
    //protected int deleteCount = 0;
    protected ArrayList<RetrieveResponse> retrieveArr = new ArrayList<>();
    protected S3Client s3Client = null;
    protected String bucketName = null;
    protected String [] digestTypesS = new String [1];
    

    public TestStreamZip(
            S3Client s3Client,
            String bucketName,
            String [] digestTypesS)
        throws TException
    {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.digestTypesS = digestTypesS;
    }
    
    public static void main(String[] args) 
            throws TException
    {
        main_staging(args);
        //delete("zip");

    }
    
    public static void main_orig(String[] args) 
            throws TException
    {
        InputStream zipStream = null;
        try {
            LoggerInf logger = new TFileLogger("TestAWSGet", 50, 50);
            String urlZipS = "http://localhost:35121/content/test.zip";
            String zipKey = "zip/test.zip";
            V2Client v2client = V2Client.getAWS();
            S3Client s3Client = v2client.s3Client();
            String bucketName = "dloy-tagging-bucket";
            String [] digestTypes = {"sha256","md5"};
            AWSS3V2Cloud service = AWSS3V2Cloud.getAWS(logger);
            S3Retrieve s3Staging = S3Retrieve.getS3Retrieve(logger);

            RetrieveResponse response = s3Staging.putUrl(urlZipS, zipKey, digestTypes);
            JSONObject jsonResponse = response.getJson();
            System.out.println("JSON:\n" + jsonResponse.toString(2));
            UploadS3Zip uploadS3Zip = new UploadS3Zip(s3Client, bucketName, digestTypes);
            //InputStream httpInStream = HttpGet.getStream(zipUrl, 60000,  logger);
            //File zipFile = new File("/home/loy/tasks/inventory/250604-sla/tmp/test.zip");
            CloudResponse streamResponse = new CloudResponse(bucketName, zipKey);
            zipStream = service.getObjectStreaming(bucketName, zipKey, streamResponse);
            uploadS3Zip.uploadZip("ziptest", zipStream);

            s3Staging.deletePrefix("zip");
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                if (zipStream != null) {
                    zipStream.close();
                }
            } catch (Exception ex) { }
        }
    }
    
    
    public static void main_staging(String[] args) 
            throws TException
    {
        InputStream zipStream = null;
        try {
            LoggerInf logger = new TFileLogger("TestAWSGet", 50, 50);
            String urlZipS = "http://localhost:35121/content/test.zip";
            String zipKey = "zip/test.zip";
            String zipPrefix = "ziptest";
            String bucketName = "dloy-tagging-bucket";
            String [] digestTypes = {"sha256","md5"};
            //AWSS3V2Cloud service = AWSS3V2Cloud.getAWS(logger);
            S3Retrieve s3Retrieve = S3Retrieve.getS3Retrieve(logger);
            RetrieveResponse response = s3Retrieve.putUrl(urlZipS, zipKey, digestTypes);
            response.dump(zipKey);
            ArrayList<RetrieveResponse> stageArr = s3Retrieve.uploadZip(zipKey, zipPrefix, digestTypes);
                    
            //s3Staging.deletePrefix("zip");
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                if (zipStream != null) {
                    zipStream.close();
                }
            } catch (Exception ex) { }
        }
    }
    
    public static void delete(String prefix) 
            throws TException
    { 
       
        JSONObject responseJson = DeletePrefixList.deleteList(prefix);
        System.out.println("JSON:\n"  + responseJson.toString(2));
 
    }
    
    public static void upload(String prefix) 
            throws TException
    { 
       
        JSONObject responseJson = DeletePrefixList.deleteList(prefix);
        System.out.println("JSON:\n"  + responseJson.toString(2));
 
    }
}