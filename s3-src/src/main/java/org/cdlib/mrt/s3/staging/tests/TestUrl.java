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


import org.cdlib.mrt.s3.staging.service.S3Retrieve;
import org.cdlib.mrt.s3.staging.service.RetrieveResponse;

import org.cdlib.mrt.s3v2.tools.GetSSM;
import org.cdlib.mrt.s3.service.CloudResponse;
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
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

public class TestUrl {
    protected S3AsyncClient s3Client = null;
    public static GetSSM getSSM = new GetSSM();
    
    protected static final String fileBig = "/apps/replic1/tomcatdata/big/6G.txt";
    protected static final String sha256Big = "dd2a99e35c6ad550221bc3441d3ece19e0a84e2fccbf88246245f52f1a2e1cf6";
    protected static final String keyBig = "post|Big";
    
    protected static final String fileSmall = "/home/loy/t/ark+=99999=fk4806c36f/16/system/mrt-ingest.txt";
    protected static final String sha256Small = "47db2924db63fbaf78e9bf96fac08e96afbaa711a299c09d5ae13e8ee0b92452";
    protected static final String keySmall= "post|Small";
    
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
    
    protected static String [] urls =
        {
            "https://submit.escholarship.org/data/2025-09-30T00-36-12/qt5p27n7qt/mrt-erc.txt",
           "https://submit.escholarship.org/data/2025-09-30T00-36-12/qt5p27n7qt/content/Lankford_1580.pdf",
           "https://submit.escholarship.org/data/2025-09-30T00-36-12/qt5p27n7qt/content/qt5p27n7qt.pdf",
           "https://submit.escholarship.org/data/2025-09-30T00-36-12/qt5p27n7qt/meta/qt5p27n7qt.meta.xml",
           "https://uc3-mrtstore-prd01:35121/state/9501?t=xml"
        };
    
    protected static String [] digests = {"md5", "sha256"};
    
    protected static final String [] usualDigestTypes = {
           "md5",
           "sha256",
     };
    
    protected static final String [] emptyDigestTypes = {
     };
    
    
    
    public static void main(String[] args) 
            throws TException
    {
       
        
        
        for (String url: urls) {
            testUrl(url);
        }
    }

    public static void testUrl(String urlS)
    {
        try {
            URI uri = new URI(urlS);
            System.out.println("url:" + urlS + "\n"
                    + " - authority:" + uri.getAuthority() + "\n"
                    + " - Fragment:" + uri.getFragment() + "\n"
                    + " - getHost:" + uri.getHost() + "\n"
                    + " - getPath:" + uri.getPath() + "\n"
                    + " - getPort:" + uri.getPort() + "\n"
                    + " - getQuery:" + uri.getQuery() + "\n"
                    + " - getRawAuthority:" + uri.getRawAuthority() + "\n"
                    + " - getRawPath:" + uri.getRawPath() + "\n"
                    + " - getRawQuery:" + uri.getRawQuery() + "\n"
                    + " - getRawSchemeSpecificPart:" + uri.getRawSchemeSpecificPart() + "\n"
                    + " - getRawUserInfo:" + uri.getRawUserInfo() + "\n"
                    + " - getScheme:" + uri.getScheme() + "\n"
                    + " - getSchemeSpecificPart:" + uri.getSchemeSpecificPart() + "\n"
                    + " - getUserInfo:" + uri.getUserInfo() + "\n"
            );
            
            String key = uri.getPath();
            int pos = key.lastIndexOf("/");
            long timeMs = System.currentTimeMillis();
            key = "TestS3Retrieve|" + timeMs + "|" + key.substring(pos+1);
            System.out.println("KEY:" + key);
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
        }
    }
}