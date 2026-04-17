/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.test.replicFixity;
import org.cdlib.mrt.s3.test.provider.*;
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
public class TestCopyS3ToS3 {
    
    protected static final String NAME = "TestCopyFixity";
    protected static final String MESSAGE = NAME + ": ";
    
    protected LoggerInf logger = null;
    protected static String [] types = {
                "sha256"
            };
    protected RunStat runStat = null;
    protected NodeIO nodeIO = null;
    
    public TestCopyS3ToS3(NodeIO nodeIO, LoggerInf logger) 
            throws TException
    {
        this.nodeIO = nodeIO;
        this.logger = logger;
    }
    
    
    
    public static void main(String[] args) {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        
        
        
        String yamlName = "jar:nodes-remote";
        //String yamlName = "yaml:2";
        
        TestVal bk00010877t = TestVal.get("ark:/28722/bk00010877t", 11, 5675771L);
        TestVal bb4472128f = TestVal.get("ark:/20775/bb4472128f", 5, 2408754846L);
        TestVal m5dz0c7f = TestVal.get("ark:/13030/m5dz0c7f", 9, 200971500895L);

        //"ark:/13030/m5dz0c7f"
        
        
        try {
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            System.out.println(MESSAGE + " - yamlName=" + yamlName);
            TestCopyS3ToS3 tc = new TestCopyS3ToS3(nodeIO, logger);
            tc.test(bk00010877t, true, 9502, 7502 );
            //tc.test(bb4472128f, true, 9502, 7502 );
            //tc.test(m5dz0c7f, true, 9501, 7502 );
            
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
    
    public void test(
            TestVal t,
            boolean fixity,
            long inNode,
            long outNode)
        throws TException
    {
        CloudManifestCopyS3ToS3 cmcs2s = null;
        CloudStoreInf outService = null;
        try {
            cmcs2s =  CloudManifestCopyS3ToS3.getCloudManifestCopyS3ToS3(fixity, nodeIO, inNode, outNode, logger);
            CloudManifestCopyS3ToS3.Stat stat = new CloudManifestCopyS3ToS3.Stat(t.ark);
            cmcs2s.copyObject(t.ark, stat);
            stat.dump(t.ark);
            
        } catch (TException tex) {
                throw tex;
        
        } catch (Exception ex) {
                throw new TException(ex);
        }
    }
 
    
    
    public static class TestVal {
        public String ark = null;
        public int versions = 0;
        public Long size = null;
        
        public static TestVal get(
                String ark,
                int versions,
                Long size
            ) 
        {
            return new TestVal(ark, versions, size);
        }
        
        public TestVal(
                String ark,
                int versions,
                Long size
            ) 
        {
            this.ark = ark;
            this.versions = versions;
            this.size = size;
        }
    }
}
