/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3.staging.tests;

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
import java.net.URL;
import java.util.Date;
import java.time.Instant;

import org.cdlib.mrt.s3v2.tools.GetSSM;
import org.cdlib.mrt.s3.service.CloudResponse;
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;


import org.cdlib.mrt.s3.staging.service.S3Retrieve;
import org.cdlib.mrt.s3.staging.service.RetrieveResponse;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.json.JSONObject;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class TestS3Retrieve {
    protected static final String NAME = "TestS3Retrieve";
    protected static final String myFilePath = "/home/loy/tasks/cloud/250910-url2s3/data/";
    
    protected static final String [] allDigestTypes = {
           // "adler32",
            //"crc32",
            //"md2",sh3224("SHA3-224"),
           "sh3256",
           "sh3384",
           "sh3512",
           "md5",
           "sha1",
           "sha256",
           "sha384",
           "sha512"
     };
    String digest = "0cea910427b89b1db501c81c2684aa197c6459584ad8fd8d4d0a4d75c0b9795a";
    protected static String [] urltest 
            = {"http://uc3-mrtstore-prd01:35121/content/9501/ark%3A%2F13030%2Fm500009d/1/producer%2FLCD08470_01.aif"};
    protected static String [] urls =
        {
            "https://submit.escholarship.org/data/2025-09-30T00-36-12/qt5p27n7qt/mrt-erc.txt",
           "https://submit.escholarship.org/data/2025-09-30T00-36-12/qt5p27n7qt/content/Lankford_1580.pdf",
           "https://submit.escholarship.org/data/2025-09-30T00-36-12/qt5p27n7qt/content/qt5p27n7qt.pdf",
           "https://submit.escholarship.org/data/2025-09-30T00-36-12/qt5p27n7qt/meta/qt5p27n7qt.meta.xml"
        };
    
    protected static String [] digests = {"md5", "sha256"};
    
    protected static final String [] usualDigestTypes = {
           "md5",
           "sha256",
     };
    
    protected static final String [] emptyDigestTypes = {
     };
    
    protected S3Retrieve retrieve = null;
    
    public TestS3Retrieve(LoggerInf logger) 
        throws TException
    {
        retrieve = S3Retrieve.getS3Retrieve(logger);
    }
    
    public static void main(String[] args) 
            throws TException
    {
       
        
        LoggerInf logger = new TFileLogger("TestAWSGet", 50, 50);
        TestS3Retrieve testRetrieve = new TestS3Retrieve(logger);
        
        S3Retrieve retrieve = S3Retrieve.getS3Retrieve(logger);
        testRetrieve.test(urltest);
        /*
        for (String url, urls) {
            processUrl(url);
        }
*/
    }
    
    public void test(String [] urls)
       throws TException
    {
        processUrl(urls[0]);
    }
    
    protected void processUrls(String [] urls)
        throws TException
    {
        for (String url: urls) {
            processUrl(url);
        }
    }
    
    protected void processUrl(String urlS)
        throws TException
    {
        try {
            String key = getKey(urlS);
            System.out.println("processUrl entered:"
                    + " - url=" + urlS
                    + " - key=" + key
            );
            RetrieveResponse retrieveResponse = retrieve.putUrl(urlS, key, usualDigestTypes);
            dumpResponse("retrieveResponse", retrieveResponse);
            File downloadFile = getDownloadFile(key);
            RetrieveResponse getResponse = retrieve.getFile(key, downloadFile, usualDigestTypes);
            dumpResponse("getResponse", getResponse);
            //cleanup(downloadFile, key);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw  tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    
    protected File getDownloadFile(String key)
        throws TException
    {
        try {
            File downFile = new File(myFilePath + key);
            System.out.println("DOWNFILE:" + downFile.getCanonicalPath());
            return downFile;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    protected void dumpResponse(String header, RetrieveResponse response)
        throws TException
    {
        
        JSONObject json = response.getJson();
        System.out.println("***" + header + "***\n" + "JSON\n"+ json.toString(2));
    }
    
    protected String getKey(String urlS) 
        throws TException
    {
        try {
            URI uri = new URI(urlS);
            String key = uri.getPath();
            int pos = key.lastIndexOf("/");
            long timeMs = System.currentTimeMillis();
            key = NAME + "|" + timeMs + "|" + key.substring(pos+1);
            System.out.println("KEY:" + key);
            return key;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
}