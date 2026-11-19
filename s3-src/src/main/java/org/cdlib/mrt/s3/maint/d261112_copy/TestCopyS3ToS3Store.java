/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.maint.d261112_copy;
import org.cdlib.mrt.log.utility.Log4j2Util;
import org.cdlib.mrt.s3.test.replicFixity.*;
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
public class TestCopyS3ToS3Store {
    
    protected static final String NAME = "TestCopyFixity";
    protected static final String MESSAGE = NAME + ": ";
    
    protected LoggerInf logger = null;
    protected static String [] types = {
                "sha256"
            };
    protected RunStat runStat = null;
    protected NodeIO nodeIO = null;
    protected static String log4jxml 
            = "/mnt/efs/dloy/MRTMaven/main/replic/mrt-replic/replication-war/src/main/resources/log4j2.xml";
    
    public TestCopyS3ToS3Store(NodeIO nodeIO, LoggerInf logger) 
            throws TException
    {
        this.nodeIO = nodeIO;
        this.logger = logger;
    }
    
    
    
    public static void main(String[] args) {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        try {
            Log4j2Util.setLoggerLevel(log4jxml, "debug");
        } catch (Exception ex) {
            System.out.println("SetLogger exception:" + ex);
            return;
        }
        if (false) return;
        
        //String yamlName = "jar:nodes-remote";
        String yamlName = "jar:nodes-ceph2";
        //String yamlName = "yaml:2";
                /*
        TestVal bk00010877t = TestVal.get("ark:/28722/bk00010877t", 11, 5675771L);
        TestVal bb4472128f = TestVal.get("ark:/20775/bb4472128f", 5, 2408754846L);
        TestVal m5dz0c7f = TestVal.get("ark:/13030/m5dz0c7f", 9, 200971500895L);
        TestVal m5741zj2 = TestVal.get("ark:/13030/m5741zj2", 19, 12603529073L);
        TestVal bk0003d925t = TestVal.get("ark:/28722/bk0003d925t", 20, 240284321L);
        TestVal qt6jr5f1pp = TestVal.get("ark:/13030/qt6jr5f1pp", 134,8160718202L);
 

ark:/13030/qt3bp9t2b4,8363,314701257
ark:/13030/qt4w285458,7699,288728723
ark:/13030/qt8fz0q7bb,8112,301475781

        */
        TestVal qt3bp9t2b4 = TestVal.get("ark:/13030/qt3bp9t2b4",8363,314701257L);
        TestVal qt4w285458 = TestVal.get("ark:/13030/qt4w285458",7699,288728723L);
        TestVal qt8fz0q7bb = TestVal.get("ark:/13030/qt8fz0q7bb", 8112,301475781L);

        //"ark:/13030/m5dz0c7f"
        
        
        try {
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            System.out.println(MESSAGE + " - yamlName=" + yamlName);
            TestCopyS3ToS3Store tc = new TestCopyS3ToS3Store(nodeIO, logger);
            tc.test(qt3bp9t2b4, true, 5001, 8003 );
            //tc.test(bk00010877t, true, 9502, 7502 );
            //tc.test(m5741zj2, true, 9501, 7502 );
            //tc.test(bb4472128f, true, 9502, 7502 );
            //tc.test(m5dz0c7f, true, 9501, 7502 );
            //tc.test(bk0003d925t, true, 9502, 7502 );
            //tc.test(qt6jr5f1pp, true, 9501, 7502 );
            //tc.test(qt6jr5f1pp, true, 7502, 2002 );
            
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
