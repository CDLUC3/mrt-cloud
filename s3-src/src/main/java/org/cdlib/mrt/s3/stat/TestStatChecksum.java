/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.stat;
import org.cdlib.mrt.utility.*;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.tools.CloudChecksum;
/**
 *
 * @author replic
 */
public class TestStatChecksum {
    
    protected LoggerInf logger = null;
    protected static String [] types = {
                "md5",
                "sha256"
            };
    protected static String [] jarVersions = {
                "yaml:1",
                "yaml:2"
            };
    protected static String [] jarVersionsSave = {
                "nodes-remote_v1",
                "nodes-remote_v2"
            };
    protected RunStat runStat = null;
    
    public TestStatChecksum(LoggerInf logger) 
            throws TException
    {
        this.logger = logger;
        runStat = new RunStat(logger);
    }
    
    
    
    public static void main(String[] args) {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        try {
            String keySmall = "ark:/28722/k23j3911v|1|system/mrt-object-map.ttl";
            String digestSmall = "636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3a";
            long lenSmall = 4412;
            
            String keyBig = "ark:/28722/k23x83k93|1|producer/artiraq.org/static/opencontext/kenantepe/full/Fieldphotos/2005/AreaF/F7L06135T19.JPG";
            String digestBig = "e2b4badaa573012330751d846d3326f957d7cd9c89a1998000118223edf60767";
            long lenBig = 2004797;
            
            String keyDbl = "ark:/28722/bk0003d6571|1|producer/ark:/28722/bk0003d660p";
            String digestDbl = "86d53813ea440bf2aabe6a39b389c43de79c71b2cc7a0f7f35118dd85de309f2";
            long lenDbl = 60144943;
            
            String keyZero = "ark:/13030/m5rv22w2|1|producer/025/Icon";
            String digestZero = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
            String badDigestZero = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8ff";
            long lenZero = 0;
            
            String keyReallyBig = "ark:/99999/fk48k8jp86|1|producer/Asha_G.tar.gz";
            //testTime(node, types, service, bucket, keySmall, logger);
            
            TestStatChecksum tc = new TestStatChecksum(logger);
            if (false) tc.dumpMulti(10,  7502, keySmall, 
                    "636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3a",  // right
                    //"636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3b", 
                    4412);
            
            if (false) tc.dumpMulti(10,  7502, keyBig, digestBig, lenBig);
            
            if (false) tc.dumpMulti(10,  2002, keyDbl, digestDbl, lenDbl);
            
            if (false) tc.dumpMulti(2,  9502, keyZero, badDigestZero, lenZero);
            
            if (false) tc.dumpMulti(2,  9502, keyZero, digestZero, lenZero);
            
            if (false) tc.dumpMulti(10,  9502, keyDbl, digestDbl, lenDbl);
            
            if (false) tc.dumpMulti(10,  5001, keyDbl, digestDbl, lenDbl);
            
            if (false) tc.dumpMulti(10,  7502, keyDbl, digestDbl, lenDbl);
            
            if (true) tc.dumpMulti(10,  9502, keySmall, digestSmall, lenSmall);
            
            if (false) tc.dumpMulti(10,  2002, keySmall, digestSmall, lenSmall);
            
            if (false) tc.testValid(1, 7502, keySmall, 
                    "636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3a",  // right
                    //"636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3b", 
                    4412);
            if (false) tc.testValid(2, 7502, keySmall, 
                    "636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3a",  // right
                    //"636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3b", 
                    4412);
            /*
            testValid(node, types, service, bucket, keySmall, 
                    "636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3a",  // right
                    //"636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3b", 
                    4413, //right
                    //4413, 
                    logger);
            
            testValid(node, types, service, bucket, keySmall, 
                    "636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3b",  // wrong
                    //"636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3b", 
                    4412, //right
                    //4413, 
                    logger);
            

            
            testValid(9502, types, service, bucket, keySmall, 
                    "636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3a",  // right
                    //"636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3b", 
                    4412, //right
                    //4413, 
                    logger);
            */
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
    public void dumpMulti(
            int cnt,
            long node,
            String key,
            String testChecksum,
            long testLen)
        throws TException
    {
        for (int i=0; i<cnt; i++) {
            System.out.println("####################################");
            testValid(1, node,  key, testChecksum, testLen);
            System.out.println("--------------------------------------------------------------------------------");
            testValid(2, node,  key, testChecksum, testLen);
        }
        runStat.dumpEntries("v1");
        runStat.dumpEntries("v2");
        runStat.addTallyEntries();
        runStat.dumpTallyEntry("v1");
        runStat.dumpTallyEntry("v2");
        
    }
            
    
    public NodeIO.AccessNode getAccessNode(int version, long nodeNum)
        throws TException
    {
        
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
    
    
    public void testTime(
            int version,
            long node,
            String key)
        throws TException
    {
        try {
            NodeIO.AccessNode accessNode = getAccessNode(version, node);
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            // CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, key);
            CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, key, 2000000);
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

        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
    public void testValid(
            int version,
            long node,
            String key,
            String testChecksum,
            long testLen)
        throws TException
    {
        try {
            
            NodeIO.AccessNode accessNode = getAccessNode(version, node);
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, key);
            System.out.println("begin:"
                    + " - metaObjectSize=" + cloudChecksum.getMetaObjectSize()
                    + " - metaSha256=" + cloudChecksum.getMetaSha256()
            );
            long startTime = System.currentTimeMillis();
            cloudChecksum.process();
            long processTime = System.currentTimeMillis() - startTime;
            System.out.println("process time:" + processTime);
            cloudChecksum.dump("the test");


            for (String type : types) {
                String checksum = cloudChecksum.getChecksum(type);
                System.out.println("getChecksum(" + type + "):" + checksum);
            }

            String testType = "sha256";
            CloudChecksum.Digest test = cloudChecksum.getDigest(testType);
            System.out.println(test.dump("test digest"));
           
            
            CloudChecksum.CloudChecksumResult result = cloudChecksum.validateSizeChecksum(testChecksum, testType, testLen, logger);
            boolean match = false;
            if (result.checksumMatch && result.fileSizeMatch) {
                match = true;
            }
            System.out.println(result.dump("TEST"));
            
            runStat.addEntry("v" + version, 1, processTime, testLen, match, "node:" + node + " - key:" + key);
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
}
