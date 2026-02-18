/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.test.provider;
import java.util.HashMap;
import org.cdlib.mrt.s3.stat.*;
import org.cdlib.mrt.utility.*;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.tools.CloudChecksum;
import org.cdlib.mrt.s3v2.action.MultiPartUpload;
import org.cdlib.mrt.s3v2.action.PutObjectData;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
import software.amazon.awssdk.services.s3.S3Client;
/**
 *
 * @author replic
 */
public class TestStatUpload {
    
    protected LoggerInf logger = null;
    protected static String [] types = {
                "sha256"
            };
    protected static String [] jarVersionsSave = {
                "yaml:1",
                "yaml:2"
            };
    protected static String [] jarVersions = {
                "nodes-remote_v1",
                "nodes-remote_v2",
                "yaml:2"
            };
    protected RunStat runStat = null;
    
    public TestStatUpload(LoggerInf logger) 
            throws TException
    {
        this.logger = logger;
        runStat = new RunStat(logger);
    }
    
    
    
    public static void main(String[] args) {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        
        TestVal t600K = TestVal.get("t600K", "/dpr2/replic1/provider/600K.pdf", 
            608746L, "2d43c5c30e877903c88bccaed9f7559db95a0fc3cd25210ae037c4d9f1998255");
        
        TestVal t600M = TestVal.get("t600M", "/dpr2/replic1/provider/600M.aif", 
            602722980L, "85ad096033e03cfeef7ab22f3333601c55fe91a31ab9c242924542d96c21e58b");
        
        TestVal t60M = TestVal.get("t60M", "/dpr2/replic1/provider/60M.txt",
            60144943L, "86d53813ea440bf2aabe6a39b389c43de79c71b2cc7a0f7f35118dd85de309f2");
        
        TestVal t6G = TestVal.get("t6G", "/dpr2/replic1/provider/6G.mp4",
            6371909109L, "6aa54ac23ef13781a564b2b7612756197f28054f94a9e29ac277ba93889150dd"); 
        
        TestVal t6M = TestVal.get("t6M", "/dpr2/replic1/provider/6M.pdf",
            6043096L, "1775471a380d5b17c1dd2d5fe32af95eb8c2aeb711ffe6ae19f0739dcddc3c79");
        
        TestVal t60K = TestVal.get("t60K", "/dpr2/replic1/provider/60K.jpg",
            60037L, "ece925059442c7cd0952b3c6caa0b5c5300db1533d4bf00d6bcd8f1209fbc083");
        
        
        try {
            
            TestStatUpload tc = new TestStatUpload(logger);
            
            if (false) tc.dumpMulti(2, 9502, t60K);
            if (false) tc.dumpMulti(2, 9502, t60M);
            if (false) tc.dumpMulti(2, 9502, t6G);
            if (false) tc.dumpMulti(2, 2002, t60M);
            
            if (false) tc.dumpMulti(2, 2002, t600M);
            if (false) tc.dumpMulti(2, 9502, t600M);
            if (false) tc.dumpMulti(2, 7502, t600M);
            if (true) tc.dumpMulti(2, 5001, t600M);
            
            if (false) tc.testUploadSeq(t6G, 2002);
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
    public void dumpMulti(
            int cnt,
            long node,
            TestVal t)
        throws TException
    {
        for (int i=0; i<cnt; i++) {
            System.out.println("####################################");
            testUploadMultiPart(t, node);
            System.out.println("--------------------------------------------------------------------------------");
            testUploadSeq(t, node);
        }
        runStat.dumpEntries("tmult");
        runStat.dumpEntries("tseq");
        runStat.addTallyEntries();
        runStat.dumpTallyEntry("tmult");
        runStat.dumpTallyEntry("tseq");
        
    }
    
    public void testUploadMultiPart(
            TestVal t,
            long node)
        throws TException
    {
        try {
            String category = "tmult";
            NodeIO.AccessNode accessNode = getAccessNode(2, node);
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            if ( !(service instanceof AWSS3V2Cloud) ) {
                throw new TException.INVALID_OR_MISSING_PARM("AWSS3V2Cloud required for testing");
            }
            HashMap<String, String> metadata = new HashMap<>();
            metadata.put("sha256", t.sha256);
            
            AWSS3V2Cloud awss3 = (AWSS3V2Cloud)service;
            S3Client s3 = awss3.getS3Client();
            long startTime = System.currentTimeMillis();
            MultiPartUpload.uploadFileParts(s3, bucket, t.key, t.filePath, metadata);
            long processTime = System.currentTimeMillis() - startTime;
            
            System.out.println("process time:" + processTime);
            
            CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, t.key);
            System.out.println("begin:"
                    + " - metaObjectSize=" + cloudChecksum.getMetaObjectSize()
                    + " - metaSha256=" + cloudChecksum.getMetaSha256()
            );
            cloudChecksum.process();
            cloudChecksum.dump("the test");


            for (String type : types) {
                String checksum = cloudChecksum.getChecksum(type);
                System.out.println("getChecksum(" + type + "):" + checksum);
            }

            String testType = "sha256";
            CloudChecksum.Digest test = cloudChecksum.getDigest(testType);
            System.out.println(test.dump("test digest"));
            
            CloudChecksum.CloudChecksumResult result = cloudChecksum.validateSizeChecksum(t.sha256, testType, t.size, logger);
            System.out.println(result.dump("TEST"));
            boolean match = result.checksumMatch & result.fileSizeMatch;
            
            runStat.addEntry(category, 2, processTime, t.size, match, "node:" + node + " - key:" + t.key);
            
            CloudResponse response = awss3.deleteObject(bucket, t.key);
            
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
    public void testUploadSeq(
            TestVal t,
            long node)
        throws TException
    {
        try {
            String category = "tseq";
            NodeIO.AccessNode accessNode = getAccessNode(2, node);
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            if ( !(service instanceof AWSS3V2Cloud) ) {
                throw new TException.INVALID_OR_MISSING_PARM("AWSS3V2Cloud required for testing");
            }
            HashMap<String, String> metadata = new HashMap<>();
            metadata.put("sha256", t.sha256);
            
            AWSS3V2Cloud awss3 = (AWSS3V2Cloud)service;
            S3Client s3 = awss3.getS3Client();
            long startTime = System.currentTimeMillis();
            PutObjectData.putS3Object(s3, bucket, t.key, t.filePath, metadata);
            long processTime = System.currentTimeMillis() - startTime;
            
            System.out.println("process time:" + processTime);
            
            CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, t.key);
            System.out.println("begin:"
                    + " - metaObjectSize=" + cloudChecksum.getMetaObjectSize()
                    + " - metaSha256=" + cloudChecksum.getMetaSha256()
            );
            cloudChecksum.process();
            cloudChecksum.dump("the test");


            for (String type : types) {
                String checksum = cloudChecksum.getChecksum(type);
                System.out.println("getChecksum(" + type + "):" + checksum);
            }

            String testType = "sha256";
            CloudChecksum.Digest test = cloudChecksum.getDigest(testType);
            System.out.println(test.dump("test digest"));
            
            CloudChecksum.CloudChecksumResult result = cloudChecksum.validateSizeChecksum(t.sha256, testType, t.size, logger);
            System.out.println(result.dump("TEST"));
            boolean match = result.checksumMatch & result.fileSizeMatch;
            
            runStat.addEntry(category, 2, processTime, t.size, match, "node:" + node + " - key:" + t.key);
            
            CloudResponse response = awss3.deleteObject(bucket, t.key);
            
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
            
    
    public NodeIO.AccessNode getAccessNode(int version, long nodeNum)
        throws TException
    {
        if (nodeNum == 5001) version = 3;
        String jarBase = jarVersions[(version - 1)];
        try {
            NodeIO nodeIO = NodeIO.getNodeIOConfig(jarBase, logger) ;
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(nodeNum);
            return accessNode;
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    
    public static class TestVal {
        public String key = null;
        public String filePath = null;
        public Long size = null;
        public String sha256 = null;
        public static TestVal get(
                String key,
                String filePath,
                Long size,
                String sha256
            ) 
        {
            return new TestVal(key, filePath, size, sha256);
        }
        
        public TestVal(
                String key,
                String filePath,
                Long size,
                String sha256
            ) 
        {
            this.key = key;
            this.filePath = filePath;
            this.size = size;
            this.sha256 = sha256;
        }
    }
}
