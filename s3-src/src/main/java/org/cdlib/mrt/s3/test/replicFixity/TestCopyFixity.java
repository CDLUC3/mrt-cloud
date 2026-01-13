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
import org.cdlib.mrt.s3.tools.CloudManifestCopyFixity;
import org.cdlib.mrt.s3v2.action.MultiPartUpload;
import org.cdlib.mrt.s3v2.action.PutObjectData;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
import software.amazon.awssdk.services.s3.S3Client;
/**
 *
 * @author replic
 */
public class TestCopyFixity {
    
    protected static final String NAME = "TestCopyFixity";
    protected static final String MESSAGE = NAME + ": ";
    
    protected LoggerInf logger = null;
    protected static String [] types = {
                "sha256"
            };
    protected static String [] jarVersionsSave = {
                "yaml:1",
                "yaml:2"
            };
   //protected static String yamlName = "nodes-remote_v2";
    protected static String yamlName = "yaml:2";
    protected RunStat runStat = null;
    protected NodeIO nodeIO = null;
    
    public TestCopyFixity(NodeIO nodeIO, LoggerInf logger) 
            throws TException
    {
        this.nodeIO = nodeIO;
        this.logger = logger;
    }
    
    
    
    public static void main(String[] args) {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        
        TestVal bb0546922j = TestVal.get("ark:/20775/bb0546922j", 6, 13936361L);
        TestVal bb05469232 = TestVal.get("ark:/20775/bb05469232", 6,  3473199L);
        TestVal bb0547060t = TestVal.get("ark:/20775/bb0547060t", 6,  7767082L);
        
        
        
        try {
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            System.out.println(MESSAGE + " - yamlName=" + yamlName);
            TestCopyFixity tc = new TestCopyFixity(nodeIO, logger);
            long inNode = 9502;
            long outNode = 7502;
            //tc.test(bb0546922j, inNode, outNode );
            tc.test(bb05469232, inNode, outNode );
            tc.test(bb0547060t, inNode, outNode );
            
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
    
    public void test(
            TestVal t,
            long inNode,
            long outNode)
        throws TException
    {
        CloudManifestCopyFixity cmcf = null;
        CloudStoreInf outService = null;
        try {
            cmcf =  CloudManifestCopyFixity.getCloudManifestCopyFixity(true, nodeIO, inNode, outNode, logger);
            CloudManifestCopyFixity.Stat stat = new CloudManifestCopyFixity.Stat(t.ark);
            cmcf.copyObject(t.ark, stat);
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
