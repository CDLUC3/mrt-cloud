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
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
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

import org.cdlib.mrt.s3v2.tools.GetSSM;
import org.cdlib.mrt.s3.service.CloudResponse;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import static org.cdlib.mrt.s3v2.test.TestState.getSSM;
import org.cdlib.mrt.utility.DateUtil;
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

public class TestAWSPut {
    protected S3AsyncClient s3Client = null;
    public static GetSSM getSSM = new GetSSM();
    
    public static void main(String[] args) 
            throws TException
    {
        
        LoggerInf logger = new TFileLogger("TestAWSGet", 50, 50);
        String yamlName = "yaml:";
        NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
        test_aws(nodeIO, logger);
        test_minio(nodeIO, logger);
        if (true) return;
    }
    
    public static void test_aws(NodeIO nodeIO, LoggerInf logger) 
            throws TException
    {
        System.out.println("***test_aws");
        String bucketName = "uc3-s3mrt5001-stg";
        String key = "ark:/99999/fk47387525|1|system/mrt-ingest.txt";
        String contentType = "text/plain";
        String contentDisposition = "attachment; filename=mrt-ingest.txt";
        NodeIO.AccessNode accessNode = nodeIO.getAccessNode(5001);
        CloudStoreInf client = accessNode.service;
        testClient("AWS", client, bucketName, key, contentType, contentDisposition);
                
    }
    
    public static void test_minio(NodeIO nodeIO, LoggerInf logger) 
            throws TException
    {
        System.out.println("***test_mineo"
                + "");
        String bucketName = "cdl.sdsc.stage";
        String key = "ark:/20775/bb0547060t|1|producer/1-1.pdf";
        String contentType = "application/pdf";
        String contentDisposition = "attachment; filename=1-1.pdf";
        NodeIO.AccessNode accessNode = nodeIO.getAccessNode(9502);
        CloudStoreInf client = accessNode.service;
        testClient("Minio", client, bucketName, key, contentType, contentDisposition);
                
    }
    
    public static void testClient(String header, CloudStoreInf client, 
            String bucketName, String key,
            String contentType, String contentDisposition)
        throws TException
    {
        try {
            System.out.println("Test - "
                    + header + "\n"
                    + " - bucketName:" + bucketName
                    + " - key:" + key + "\n"
            );
            Properties prop = client.getObjectMeta(bucketName, key);
            System.out.println(PropertiesUtil.dumpProperties(header, prop));
            
           CloudResponse response = client.getPreSigned(10, bucketName, key, contentType, contentDisposition);
           URL preUrl = response.getReturnURL();
           String preUrlS = null;
           if (preUrl != null) preUrlS = preUrl.toString();
           CloudResponse.ResponseStatus statusRS = response.getStatus();
           String statusS = statusRS.toString();
           System.out.println("Presign(" + statusS + ")\n>>>" + preUrlS + "<<<");
           
        } catch (Exception ex) {
             System.out.println("Exception:" + ex);
        }
    }
    
    
    public static V2Client getClientWasabi() 
            throws TException
    {
        System.out.println("---getClientWasabi");
        
        String endpoint = "https://s3.us-east-2.wasabisys.com:443";
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/wasabi-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/wasabi-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        Region region = Region.US_EAST_2;
        
        V2Client v2client = V2Client.getWasabi(accessKey, secretKey, endpoint);
        return v2client;
    }
    
    
    public static V2Client getClientMinio() 
            throws TException
    {
        System.out.println("---getClientMinio");
        String endpoint = "https://cdl.s3.sdsc.edu:443";
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/sdsc-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/sdsc-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        
        System.out.println("keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String key = "ark:/28722/bk0006w8m0c|1|system/mrt-erc.txt";
        Region region = Region.US_WEST_2;

        V2Client v2client = V2Client.getMinio(accessKey, secretKey, endpoint);
        return v2client;
    }
    
    public static V2Client getClientSDSC() 
            throws TException
    {
        System.out.println("---getClientSDSC");
        String ssmAccess = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-accessKey";
        String ssmSecret = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-secretKey";
        
        String accessKey = getSSM.getssmv2(ssmAccess);
        String secretKey = getSSM.getssmv2(ssmSecret);
        System.out.println("wasabi keys"
                + " - accessKey=" + accessKey
                + " - secretKey=" + secretKey
        );
        String endpoint = "https://uss-s3.sdsc.edu:443";

        V2Client v2client = V2Client.getMinio(accessKey, secretKey, endpoint);
        return v2client;
    }
    
    public static V2Client getClientAWS() 
            throws TException
    {
        System.out.println("---getClientAWS");
        V2Client v2client = V2Client.getAWS();
        return v2client;
    }
}