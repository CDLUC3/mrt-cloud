/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.test.tasks.d260410_fail7502;
import org.cdlib.mrt.s3.test.replicFixity.*;
import org.cdlib.mrt.s3.test.provider.*;
import java.io.File;
import java.util.HashMap;
import org.cdlib.mrt.s3.stat.*;
import org.cdlib.mrt.utility.*;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.tools.CloudChecksum;
import org.cdlib.mrt.s3.tools.CloudManifestCopyS3ToS3;
import org.cdlib.mrt.s3v2.action.MultiPartUpload;
import org.cdlib.mrt.s3v2.action.PutObjectData;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
import software.amazon.awssdk.services.s3.S3Client;
/**
 *
 * @author replic
 */
public class Test7502Get {
    
    protected static final String NAME = "TestCopyFixity";
    protected static final String MESSAGE = NAME + ": ";
    
    protected LoggerInf logger = null;
    protected static String [] types = {
                "sha256"
            };
    //protected static String yamlName = "jar:nodes-remote";
    protected static String yamlName = "yaml:2";
    protected RunStat runStat = null;
    protected NodeIO nodeIO = null;
    
    
    
    
    public static void main(String[] args) {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        long inNode = 7502;
        
        
        
        
        //TestVal bk00010877t = TestVal.get("ark:/28722/bk00010877t", 11, 5675771L);
        
        
        File outFile = new File("/home/loy/tasks/replic/260409-s2s/test/dump.txt");
        String inKey = "ark:/28722/bk00010877t|1|producer/cabeurle_60_1_00037069.xml";
        try {
            if (outFile.exists()) {
                outFile.delete();
                System.out.println("Delete:" + outFile.getAbsolutePath());
            }
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            NodeIO.AccessNode inAccessNode = nodeIO.getAccessNode(inNode);
            CloudStoreInf inService = inAccessNode.service;
            String inBucket = inAccessNode.container;
            System.out.println("Test7502Get"
                    + " - inBucket=" + inBucket
                    + " - inKey=" + inKey
            );
            CloudResponse response = new CloudResponse(inBucket, inKey);
            inService.getObject(inBucket, inKey, outFile, response);
            System.out.println(response.dump("outDump"));
            
            if (outFile.exists()) {
                
                System.out.println("Outfile exists:" + outFile.getAbsolutePath()
                        + " - filelen:" + outFile.length()
                );
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
}
