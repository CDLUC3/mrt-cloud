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
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import java.net.URI;
import java.io.File;
import java.net.URL;
import java.util.Date;
import java.time.Instant;

import org.cdlib.mrt.s3v2.tools.GetSSM;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.utility.Checksums;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

import software.amazon.awssdk.services.s3.S3AsyncClient;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class TestAWSNodeIO {
    protected S3AsyncClient s3Client = null;
    public static GetSSM getSSM = new GetSSM();
    
    protected static final String fileBig = "/apps/replic1/tomcatdata/big/6G.txt";
    protected static final String downloadDir = "/home/loy/tasks/awsv2/";
    protected static final String downloadBig = "/home/loy/tasks/awsv2/download.6G.txt";
    protected static final String downloadSmall = "/home/loy/tasks/awsv2/download.small.txt";
    protected static final String downloadBigWasabi = "/home/loy/tasks/awsv2/wasabi.download.6G.txt";
    protected static final String downloadBigMinio = "/home/loy/tasks/awsv2/minio.download.6G.txt";
    protected static final String downloadBigSDSC = "/home/loy/tasks/awsv2/sdsc.download.6G.txt";
    protected static final String sha256Big = "dd2a99e35c6ad550221bc3441d3ece19e0a84e2fccbf88246245f52f1a2e1cf6";
    protected static final String keyBig = "ark:/9999/test|3|post-Big";
    
    protected static final String fileSmall = "/home/loy/t/ark+=99999=fk4806c36f/16/system/mrt-ingest.txt";
    protected static final String sha256Small = "47db2924db63fbaf78e9bf96fac08e96afbaa711a299c09d5ae13e8ee0b92452";
    protected static final String keySmall= "ark:/9999/test|4|post-Small";
    
            //String yamlName = "yaml:2";
            //String yamlName = "yaml:2";
            //String yamlName = "jar:nodes-remote";
            //String yamlName = "jar:nodes-stagedef";
            //String yamlName = "jar:nodes-proddef";
            //String yamlName = "jar:nodes-stagenodry";
            //String yamlName = "jar:nodes-sdsc-temp";
            //String yamlName = "jar:nodes-sdsc-backup";
            
            //String yamlName = "jar:nodes-stagedef";
    private static NodeIO nodeIO = null;
    private static int failCnt = 0;
    
    public static void main(String[] args) 
            throws TException
    {
         
        LoggerInf logger = new TFileLogger("TestAWSGet", 50, 50);
        //nodeIO = NodeIO.getNodeIOConfig("yaml:2", logger) ;
        nodeIO = NodeIO.getNodeIOConfig("yaml:2", logger) ;
        //test_minio_sync(keySmall, sha256Small, fileSmall, downloadSmall);
        //test_minio_sync(keyBig, sha256Big, fileBig, downloadBig);
        //if (true) return;
        System.out.println("\n\n++++VERSION:" + nodeIO.getAwsVersion());
        test_node(6001, keySmall, sha256Small, fileSmall, downloadDir, logger);
        test_node(7001, keySmall, sha256Small, fileSmall, downloadDir, logger);
        test_node(9502, keySmall, sha256Small, fileSmall, downloadDir, logger);
        test_node(7502, keySmall, sha256Small, fileSmall, downloadDir, logger);
        test_node(2002, keySmall, sha256Small, fileSmall, downloadDir, logger);
        
        System.out.println("\n\n++++FAILCNT:" + failCnt);
        
        if (false) return;
        test_node(7001, keyBig, sha256Big, fileBig, downloadDir, logger);
        test_node(9502, keyBig, sha256Big, fileBig, downloadDir, logger);
        test_node(7502, keyBig, sha256Big, fileBig, downloadDir, logger);
        test_node(6001, keyBig, sha256Big, fileBig, downloadDir, logger);
        test_node(2002, keyBig, sha256Big, fileBig, downloadDir, logger);
        System.out.println("FAILCNT:" + failCnt);
        //test_minio_sync(keyBig, sha256Big, fileBig, downloadBig);
        //test_minio_sync(keySmall, sha256Small, fileSmall, downloadSmall);
        //test_minio(keySmall, sha256Small, fileSmall, downloadSmall);
        //(keySmall, sha256Small, fileSmall, downloadSmall);
        //test_wasabi(keyBig, sha256Big, fileBig, downloadBig);
        //test_aws_fail(keyBig, sha256Big, fileBig, downloadBig);
        //test_aws(keyBig, sha256Big, fileBig, downloadBig);
        //test_sdsc(keyBig, sha256Big, fileBig, downloadBig);
        //test_glacier(keyBig, sha256Big, fileBig, downloadBig);
        //test_minio_delete( keySmall );
        
        // test_temp_delete( keySmall );
        
        //test_wasabi_delete( keySmall );
        
        
        //test_wasabi_uploadFileAsync(fileBig, keyBig, sha256Big);
        
        //test_wasabi_uploadFileAsync(fileSmall, keySmall, sha256Small);
        
        //test_wasabi_putS3Object(fileSmall, keySmall, sha256Small);
        
        //test_minio_uploadFileAsync(fileSmall, keySmall, sha256Small) ;//<-fails
        
        //test_temp_uploadFileAsync(fileSmall, keySmall, sha256Small) ;//<-works
        
        //test_temp_uploadFileAsync(fileBig, keyBig, sha256Big) ;
        
        //test_temp_multiparUpload(fileBig, keyBig, sha256Big) ;
        
        //test_wasabi_multiPartUpload(fileBig, keyBig, sha256Big) ;
        
        //test_wasabi_download(downloadBigWasabi, keyBig, sha256Big) ;
        
        //test_sdsc_updown(keyBig, sha256Big, fileBig, downloadBigSDSC) ;
                
        //test_sdsc_putS3Object(fileSmall, keySmall, sha256Small) ; 
        
        //test_aws(args);
        
        //test_glacier(args);
        
        //test_minio(args);
        
        //test_wasabi(args);
    }
    
    public static void test_node(long nodeID, String key, String sha256, String upFilePath, String downFilePth, LoggerInf log)
            throws TException
    { 
        
        NodeIO.AccessNode accessNode = getClientNodeIO(nodeID);
        String bucketName = accessNode.container;
        String description = accessNode.nodeDescription;
        CloudStoreInf client = accessNode.service;
        System.out.println("\n*****test: " + accessNode.nodeNumber + ":" + description + " *****");
        testState( client, bucketName, key, sha256, upFilePath, downFilePth, log);
    }

    
    public static Properties getMeta(String header, CloudStoreInf client, String bucketName, String key)
        throws TException
    {
        Properties prop =  client.getObjectMeta(bucketName, key);
        
        if (prop == null) {
            System.out.println(header + ": prop null");
          
            
        } else if (prop.isEmpty()) {
            System.out.println(header + ": prop null");
            
        } else {
            System.out.println(PropertiesUtil.dumpProperties(header, prop));
        }
        return prop;
    }
    
    protected static CloudResponse doPutObject(
            CloudStoreInf client,
            String bucketName,
            String key,
            String filePath,
            String sha256)
        throws TException
    {
        
        
        System.out.println("doPutObject"
                + " - filePath=" + filePath
                + " - bucketName=" + bucketName
                + " - filePath=" + filePath
                + " - key=" + key
                + " - sha256=" + sha256
        );
        File inFile = new File(filePath);
        CloudResponse response = client.putObject(bucketName, key, inFile);
        return response;
    }
    
    
    
    protected static void testPutObject(
            CloudStoreInf client,
            String bucketName,
            String key,
            String putFilePath,
            String sha256)
        throws TException
    {
        
        
        try {
            System.out.println("testPutObject"
                    + " - filePath=" + putFilePath
                    + " - bucketName=" + bucketName
                    + " - key=" + key
                    + " - sha256=" + sha256
            );
            File putFile = new File(putFilePath);
            CloudResponse response = client.putObject(bucketName, key, putFile);
            Exception runEx = response.getException();
            if (runEx != null) {
                System.out.println("getObject exception:" + runEx);
                return;
            }
            String putSha256 = response.getSha256();
            if (putSha256.equals(sha256)) {
                System.out.println(">>>Upload match:" + key + "\n");
            } else {
                System.out.println(">>>Upload FAILS match:" + key
                        + " - inSha256:" + sha256
                        + " - putSha256:" + putSha256
                        + "\n"
                );
                failCnt++;
            }

        } catch (Exception ex) {
            System.out.println("getObject run exception:" + ex);
            ex.printStackTrace();
            
        }
    }
    
    
    protected static void testGetObject(
            CloudStoreInf client,
            String bucketName,
            String key,
            File outDir,
            String sha256,
            LoggerInf logger)
        throws TException
    {
        File downFile = null;
        CloudResponse response = new CloudResponse(bucketName, key);
        try {
            downFile = new File(outDir, "outFile.txt");
            if (downFile.exists()) {
                downFile.delete();
            }
            
            System.out.println("doPutObject"
                    + " - filePath=" + downFile.getCanonicalPath()
                    + " - bucketName=" + bucketName
                    + " - key=" + key
                    + " - sha256=" + sha256
            );
            client.getObject(bucketName, key, downFile, response);
            Exception runEx = response.getException();
            if (runEx != null) {
                System.out.println("getObject exception:" + runEx);
                return;
            }
            String fileSha256 = CloudUtil.getDigestValue("sha256", downFile, logger);
            
            if (fileSha256.equals(sha256)) {
                System.out.println(">>>testGetObject match:" + key + "\n");
            } else {
                System.out.println(">>>testGetObject FAILS match:" + key
                        + " - inSha256:" + sha256
                        + " - fileSha256:" + fileSha256
                        + "\n"
                );
                failCnt++;
            }

        } catch (Exception ex) {
            System.out.println("getObject run exception:" + ex);
            ex.printStackTrace();
            
        } finally {
            try {
                downFile.delete();
            } catch (Exception e) { }
        }
    }
    
    public static CloudResponse doDelete(
            CloudStoreInf client,
            String bucketName,
            String key)
        throws TException
    {
        
        System.out.println("doDelete"
                + " - bucketName=" + bucketName
                + " - key=" + key
        );
        
        Properties prop = client.getCloudProp();
        if (prop == null) {
            System.out.println("prop null");
        } else if (prop.size() == 0) {
            System.out.println("Does not exist:"
                    + " - bucketname:" + bucketName
                    + " - key:" + key
            );
            return null;
        }
        
        CloudResponse response = client.deleteObject(bucketName, key);
        Exception ex = response.getException();
        if (ex != null) {
            System.out.println("Delete Exception" + ex);
            return response;
        }
        System.out.println("after delete");
        
        prop = client.getCloudProp();
        if ((prop == null) || prop.isEmpty()) {
            System.out.println("After delete:"
                    + " - bucketname:" + bucketName
                    + " - key:" + key
            );
        } else {
            System.out.println(PropertiesUtil.dumpProperties("***Bad remaining properties", prop));
        }
        return response;
    }


    public static URL doPresign (
            CloudStoreInf client,
            String bucketName,
            String key)
        throws TException
    {
        try {
            String contentType="text/plain";
            String contentDisp="attachment; filename=\"6g.txt\"";
            // Set the presigned URL to expire after one hour.
            java.util.Date expiration = new java.util.Date();
            long expirationMinutes = 30L;
            System.out.println("***getPreSigned"
                    + " - bucketName:" + bucketName
                    + " - key:" + key
                    + " - expirationMinutes:" + expirationMinutes
                    + " - contentType:" + contentType
                    + " - contentDisp:" + contentDisp
            );
            CloudResponse response = client.getPreSigned(expirationMinutes, bucketName, key, contentType, contentDisp);
            Exception ex = response.getException();
                if (ex != null) {
                System.out.println("doPresign Exception" + ex);
                throw new TException(ex);
            }
            URL url = response.getReturnURL();
            String urlS = url.toString();
            //System.out.println("---presign:" + urlS);
            return url;
            
        } catch (Exception ex) {
            System.out.println("getPreSigned exception:" +ex);
            ex.printStackTrace();
            String exc = "Exception bucketName:" + bucketName + " - key=" + key;
            System.out.println("Presign exception:" + exc);
            throw new TException(ex);
        }
    }


    public static void testPresign (
            CloudStoreInf client,
            String bucketName,
            String key,
            File outDir,
            String sha256,
            LoggerInf logger)
        throws TException
    {
        File preFile = null;
        try {
            URL presign = doPresign(client, bucketName, key);
            System.out.println("***testPreSigned"
                    + " - bucketName:" + bucketName
                    + " - key:" + key
                    + " - presign>>>" + presign + "<<<"
            );
            preFile = new File(outDir, "presign.txt");
            if (preFile.exists()) {
                preFile.delete();
            }
            String presignS = presign.toString();
            presignS = presignS.replace(":443/", "/");
            presign = new URL(presignS);
            FileUtil.url2File(logger, presign, preFile);
            //url2file(logger, presign, preFile, 10);
            String fileSha256 = CloudUtil.getDigestValue("sha256", preFile, logger);
            
            if (fileSha256.equals(sha256)) {
                System.out.println(">>>presignLoad match:" + key + "\n");
            } else {
                System.out.println(">>>presignLoad FAILS match:" + key
                        + " - inSha256:" + sha256
                        + " - putSha256:" + fileSha256
                        + "\n"
                );
                failCnt++;
            }
            
        } catch (Exception ex) {
            System.out.println("getPreSigned exception:" +ex);
            ex.printStackTrace();
            String exc = "Exception:" + key + " - ex=" + ex;
            System.out.println("Presign exception:" + exc);
            throw new TException(ex);
            
        } finally {
            try {
                preFile.delete();
            } catch (Exception e) { }
        }
    }
    
    public static int url2file(LoggerInf logger, URL presign, File preFile, int retry)
            throws TException
    {
        Exception testEx = null;
        for (int iter=1; iter<=retry; iter++) {
            try {
                FileUtil.url2File(logger, presign, preFile);
                return iter;
            } catch (Exception ex) {
                testEx = ex;
            }
            if (iter < retry) {
                long sleepMl = iter * 1000;
                System.out.println("iter:" + iter
                        + " - sleepMl:" + sleepMl
                        + " - ex:" + testEx
                );
                try {
                    Thread.sleep(sleepMl);
                } catch (Exception e) { }
            }
        }
        throw new TException(testEx);
    }
    
    public static void testState(
            CloudStoreInf client,
            String bucketName,
            String key, 
            String sha256, 
            String upFilePath, 
            String downDirPath,
            LoggerInf logger)
        throws TException
    { 
        File outDir = new File(downDirPath);
        try {
            System.out.println("***\ntestState\n");
            doDelete(client,bucketName,key);
            //doUploadMultipart(v2client, bucketName, key, upFilePath, sha256);
            //doUpload(v2client, bucketName, key, upFilePath, sha256);
            
            testPutObject(client, bucketName, key, upFilePath, sha256);
            
            testGetObject(client, bucketName, key, outDir, sha256, logger);
            
            testPresign(client, bucketName, key, outDir, sha256, logger);
            
            doDelete(client,bucketName,key);
            
        } catch (Exception e) {
            System.out.println("Exception:" + e);
            e.printStackTrace();
            throw new TException(e);
        }
    }   
    
    public static NodeIO.AccessNode getClientNodeIO(long nodeID) 
            throws TException
    {
        System.out.println("NodeIO:" + nodeID);
        return  nodeIO.getAccessNode(nodeID);
    }

}