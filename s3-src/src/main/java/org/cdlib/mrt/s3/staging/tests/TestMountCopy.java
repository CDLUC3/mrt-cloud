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
import java.util.Properties;
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
import org.cdlib.mrt.utility.PropertiesUtil;
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

public class TestMountCopy {
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
    
    
    public static void main(String[] args) 
            throws TException
    {
        main_orig(args);
        //delete("zip");

    }
    
    public static void main_orig(String[] args) 
            throws TException
    {
        try {
            LoggerInf logger = new TFileLogger("TestAWSGet", 50, 50);
            AWSS3V2Cloud service = AWSS3V2Cloud.getAWS(logger);
            String upFileS = "/home/loy/mount/dloy-tagging-bucket/ziptest/ark+=13030=c8057d0v/3/producer/cstr_138.tif";
            File upFile = new File(upFileS);
            CloudResponse response = service.putObject("uc3-s3mrt1001-stg", "testup/file", upFile);
            System.out.println(response.dump("up dump"));
            Properties prop = service.getObjectMeta("uc3-s3mrt1001-stg", "testup/file");
            System.out.println(PropertiesUtil.dumpProperties("test/file", prop));
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
        }
    }
    
    
}