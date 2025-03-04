/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.test;
import org.cdlib.mrt.s3.tools.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.security.MessageDigest;
import java.util.Properties;
import org.cdlib.mrt.utility.*;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import static org.cdlib.mrt.utility.MessageDigestValue.getAlgorithm;
import org.cdlib.mrt.s3.tools.CloudChecksum;
/**
 *
 * @author replic
 */
public class TestCloudChecksumRange {
    
            String keySmall = "ark:/28722/k23j3911v|1|system/mrt-object-map.ttl";
            String keyBig = "ark:/28722/k23x83k93|1|producer/artiraq.org/static/opencontext/kenantepe/full/Fieldphotos/2005/AreaF/F7L06135T19.JPG";
            String keyReallyBig = "ark:/99999/fk48k8jp86|1|producer/Asha_G.tar.gz";
            
    public static void main(String[] args) {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        try {
            String [] types = {
                "md5",
                "sha256"
            };
            
            String jarBase = "jar:nodes-stagedef";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(jarBase, logger) ;
            
            test(nodeIO, 9502, types, 
                   "ark:/28722/k23j3911v|1|system/mrt-object-map.ttl", 
                   "636c68fdf4ad96eac9e87adc478aeecbf6d867eaa3eeec3f6e98e4faf32d8e3a",
                   4412,
                   logger
                   );
            
            test(nodeIO, 9502, types, 
                   "ark:/28722/k23x83k93|1|producer/artiraq.org/static/opencontext/kenantepe/full/Fieldphotos/2005/AreaF/F7L06135T19.JPG", 
                   "e2b4badaa573012330751d846d3326f957d7cd9c89a1998000118223edf60767",
                   2004797,
                   logger
                   );
            
            test(nodeIO, 9502, types, 
                   "ark:/99999/fk41g1x66w|1|producer/Abewe_2_5GB.blob", 
                   "5d3e4207a04770fd1269a41bd309e8efcbcdddc671f234920007c47721298ac4",
                   2566914048L,
                   logger
                   );
            
 
            //testTime(node, types, service, bucket, keySmall, logger);
            
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
    
    
    
    public static void test(
            NodeIO nodeIO,
            long node,
            String [] types, 
            String key,
            String testChecksum,
            long testLen, 
            LoggerInf logger)
        throws TException
    {
        try {
            System.out.println("\n***************************************");
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(node);
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, key);
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
            
            CloudChecksum.CloudChecksumResult result = cloudChecksum.validateSizeChecksum(testChecksum, testType, testLen, logger);
            System.out.println(result.dump("TEST"));
            
        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
}
