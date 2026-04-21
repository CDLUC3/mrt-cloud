/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3v2.test;
import org.cdlib.mrt.s3.test.s3tos3.*;
import org.cdlib.mrt.s3.staging.action.*;
import org.cdlib.mrt.s3.tools.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.security.MessageDigest;
import java.util.Properties;
import org.cdlib.mrt.utility.*;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.tools.S3Reader;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.tools.ChecksumHandler;
import static org.cdlib.mrt.utility.MessageDigestValue.getAlgorithm;
/**
 *
 * @author replic
 */
public class TestV2Upload {
    
    protected static String [] digestTypesS = {"md5", "sha256"};
    
    public static void main(String[] args) {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        try {
            if (false) mainTest(
                9501,
                "ark:/13030/c8028pn1|1|producer/c8028pn1.mets.xml",
                6030,
                "6985f62f1aa5724609d1c88ac457ce77257d717efae698f4c70bfd1c4797b91e"
            );
            
            
            if (false) mainTest(
                9501,
                "ark:/13030/m50k26vp|1|producer/781292960.pdf",
                6043096,
                "1775471a380d5b17c1dd2d5fe32af95eb8c2aeb711ffe6ae19f0739dcddc3c79"
            );
            
            // ark:/13030/m50g3hdx|1|producer/LCD11008_02.aif	9501	85ad096033e03cfeef7ab22f3333601c55fe91a31ab9c242924542d96c21e58b	602722980
            if (false) mainTest(
                9501,
                "ark:/13030/m50g3hdx|1|producer/LCD11008_02.aif",
                602722980,
                "85ad096033e03cfeef7ab22f3333601c55fe91a31ab9c242924542d96c21e58b"
            );
            
            
            // ark:/13030/m5dz0c7f|5|producer/AS136-014-M.mp4	9501	8728c4b7f0b0452591a2a4e8fd6af0fb80172f1ed2905bc7b14311d61f60260a	6053716977
            
            if (false) mainTest(
                9501,
                "ark:/13030/m5dz0c7f|5|producer/AS136-014-M.mp4",
                6053716977L,
                "8728c4b7f0b0452591a2a4e8fd6af0fb80172f1ed2905bc7b14311d61f60260a"
            );
            
                
            if (true) mainTest(
                2001,
                "ark:/13030/m5dz0c7f|5|producer/AS136-014-M.mp4",
                6053716977L,
                "8728c4b7f0b0452591a2a4e8fd6af0fb80172f1ed2905bc7b14311d61f60260a"
            );
            
            // ark:/13030/m56h4t8f|1|producer/MS%2027.txt	0	9501            
            if (false) mainTest(
                9501,
                "ark:/13030/m56h4t8f|1|producer/MS 27.txt",
                0L,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            );

         } catch (TException tex) {
            tex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
   
            
    
    public static void mainTest(long node, String key, long size, String digest) 
        throws TException
    {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        System.out.println("\n***mainTest***\n"
                + " - node=" + node + "\n"
                + " - key=" + key + "\n"
                + " - size=" + size + "\n"
                + " - digest=" + digest + "\n"
        );
        try {
            //String jarBase = "yaml:2";
            String jarBase = "jar:nodes-remote";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(jarBase, logger) ;
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(node);
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            System.out.println("\n*** CloudChecksum ***");
            CloudChecksum cloudChecksum = CloudChecksum.getChecksums(digestTypesS, service, bucket, key);
            System.out.println("begin:"
                    + " - metaObjectSize=" + cloudChecksum.getMetaObjectSize()
                    + " - metaSha256=" + cloudChecksum.getMetaSha256()
            );
            cloudChecksum.process();
            cloudChecksum.dump("the test");
            for (String type : digestTypesS) {
                String checksum = cloudChecksum.getChecksum(type);
                System.out.println("getChecksum(" + type + "):" + checksum);
            }
            
            System.out.println("\n*** S3Reader ***");
            long startTimeMs = System.currentTimeMillis();
            int maxbufsize = 200*1000*1000;
            S3Reader s3Reader = S3Reader.getS3Reader( service, bucket, key, maxbufsize);
            System.out.println("begin:"
                    + " - metaObjectSize=" + s3Reader.getMetaObjectSize()
                    + " - metaSha256=" + s3Reader.getMetaSha256()
            );
            long readSize = s3Reader.startRead();
            System.out.println("readSize:" + readSize);
            long totlen = 0;
            ChecksumHandler checksumHandler = ChecksumHandler.getChecksumHandler(digestTypesS);
            while (s3Reader.isMore()) {
                //System.out.println("isMore:" + s3Reader.isMore());
                byte[] bytes = s3Reader.nextRead();
                totlen += bytes.length;
                if (true) System.out.println("s3Reader:"
                        + " - bytes:" + bytes.length
                        + " - isMore:" + s3Reader.isMore()
                        + " - totlen:" + totlen
                );
                checksumHandler.fillBuff(bytes);
            }
            checksumHandler.finishDigests();
            long processTimeMs = System.currentTimeMillis() - startTimeMs;
            System.out.println("Node:" + node + " - processTimeMs:" + processTimeMs + " - bufsize=" + maxbufsize);

         } catch (TException tex) {
            tex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
}
