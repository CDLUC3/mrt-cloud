/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.test.s3tos3;
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
import org.cdlib.mrt.s3v2.tools.S3ToS3;
import static org.cdlib.mrt.utility.MessageDigestValue.getAlgorithm;
/**
 *
 * @author replic
 */
public class TestS3ToS3 {
    
    protected static String [] digestTypesS = {"md5", "sha256"};
    public static final String toPrefix = "test.out-";
    public static final long toNode = 7502;
    //public static final long toNode = 2002;
    //public static final long toNode = 6002;
    // must be divisible by 4096 >>>
    public static final int maxBufSize = 204800000;
    // <<<
    public static void main(String[] args) {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        try {
            
            
            // ark:/13030/m50g3hdx|1|producer/LCD11008_02.aif	9501	85ad096033e03cfeef7ab22f3333601c55fe91a31ab9c242924542d96c21e58b	602722980
            if (false) s3s3Test(
                9501,
                "ark:/13030/m50g3hdx|1|producer/LCD11008_02.aif",
                602722980,
                "85ad096033e03cfeef7ab22f3333601c55fe91a31ab9c242924542d96c21e58b"
            );
            
            
            // ark:/13030/m5dz0c7f|5|producer/AS136-014-M.mp4	9501	8728c4b7f0b0452591a2a4e8fd6af0fb80172f1ed2905bc7b14311d61f60260a	6053716977
            
            if (false) s3s3Test(
                9501,
                "ark:/13030/m5dz0c7f|5|producer/AS136-014-M.mp4",
                6053716977L,
                "8728c4b7f0b0452591a2a4e8fd6af0fb80172f1ed2905bc7b14311d61f60260a"
            );
            
                
            if (false) s3s3Test(
                2001,
                "ark:/13030/m5dz0c7f|5|producer/AS136-014-M.mp4",
                6053716977L,
                "8728c4b7f0b0452591a2a4e8fd6af0fb80172f1ed2905bc7b14311d61f60260a"
            );
            
            // ark:/13030/m56h4t8f|1|producer/MS%2027.txt	0	9501            
            if (false) s3s3Test(
                9501,
                "ark:/13030/m56h4t8f|1|producer/MS 27.txt",
                0L,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            );

//####################################################################
//####################################################################
//####################################################################

            if (false) s3FileS3Test( // 6k
                9501,
                "ark:/13030/c8028pn1|1|producer/c8028pn1.mets.xml",
                6030,
                "6985f62f1aa5724609d1c88ac457ce77257d717efae698f4c70bfd1c4797b91e"
            );
            
            if (true) s3s3Test( // 6k
                9501,
                "ark:/13030/c8028pn1|1|producer/c8028pn1.mets.xml",
                6030,
                "6985f62f1aa5724609d1c88ac457ce77257d717efae698f4c70bfd1c4797b91e"
            );
            
            //=====
            if (false) s3FileS3Test(
                9501,
                "ark:/13030/c8930r9g|1|producer/FILEID-1.189.43.jpg",
                60037,
                "ece925059442c7cd0952b3c6caa0b5c5300db1533d4bf00d6bcd8f1209fbc083"
            );
            
            if (false) s3s3Test( // 60k
                9501,
                "ark:/13030/c8930r9g|1|producer/FILEID-1.189.43.jpg",
                60037,
                "ece925059442c7cd0952b3c6caa0b5c5300db1533d4bf00d6bcd8f1209fbc083"
            );
            
            //=====
            
            
            if (false) s3FileS3Test(
                9501,
                "ark:/13030/m53b60zk|1|producer/Twyford_ucsb_0035D_11046.pdf",
                608746,
                "2d43c5c30e877903c88bccaed9f7559db95a0fc3cd25210ae037c4d9f1998255"
            );
            
               
            if (false) s3s3Test( // 608k
                9501,
                "ark:/13030/m53b60zk|1|producer/Twyford_ucsb_0035D_11046.pdf",
                608746,
                "2d43c5c30e877903c88bccaed9f7559db95a0fc3cd25210ae037c4d9f1998255"
            );
            
            //=====
            
            
            if (false) s3FileS3Test(
                9501,
                "ark:/13030/m50k26vp|1|producer/781292960.pdf",
                6043096,
                "1775471a380d5b17c1dd2d5fe32af95eb8c2aeb711ffe6ae19f0739dcddc3c79"
            );
            
            if (false) s3s3Test(
                9501,
                "ark:/13030/m50k26vp|1|producer/781292960.pdf",
                6043096,
                "1775471a380d5b17c1dd2d5fe32af95eb8c2aeb711ffe6ae19f0739dcddc3c79"
            );
            
            //=========
                 

            if (false) s3FileS3Test(
                9501,
                "ark:/13030/c8416v4h|1|producer/cstr_140_side001.tif",
                60562250,
                "cffdcef022f46cba09534341e842353305fe0109760f0c68d3fb4583555b2cc8"
            );            
            
            if (false) s3s3Test(
                9501,
                "ark:/13030/c8416v4h|1|producer/cstr_140_side001.tif",
                60562250,
                "cffdcef022f46cba09534341e842353305fe0109760f0c68d3fb4583555b2cc8"
            );
            
            //==========
            
            if (false) s3FileS3Test(
                9501,
                "ark:/13030/m5sr4jc5|1|producer/b118058782_C113909826_028.tif",
                200047122,
                "ebd3226fbf35a0b087850e8efb2a8899d7b0cccd135596f9876a79c67884f6bc"
            );
            
            if (false) s3s3Test(
                9501,
                "ark:/13030/m5sr4jc5|1|producer/b118058782_C113909826_028.tif",
                200047122,
                "ebd3226fbf35a0b087850e8efb2a8899d7b0cccd135596f9876a79c67884f6bc"
            );
            
            //=========
            
            if (false) s3FileS3Test(
                9501,
                "ark:/13030/m50g3hdx|1|producer/LCD11008_02.aif",
                602722980,
                "85ad096033e03cfeef7ab22f3333601c55fe91a31ab9c242924542d96c21e58b"
            );
            
            if (true) s3s3Test( // 602M
                9501,
                "ark:/13030/m50g3hdx|1|producer/LCD11008_02.aif",
                602722980,
                "85ad096033e03cfeef7ab22f3333601c55fe91a31ab9c242924542d96c21e58b"
            );
            
            //==========
            
            if (false) s3FileS3Test(
                9501,
                "ark:/13030/m5dz0c7f|5|producer/AS136-014-M.mp4",
                6053716977L,
                "8728c4b7f0b0452591a2a4e8fd6af0fb80172f1ed2905bc7b14311d61f60260a"
            );
            
            if (false) s3s3Test(
                9501,
                "ark:/13030/m5dz0c7f|5|producer/AS136-014-M.mp4",
                6053716977L,
                "8728c4b7f0b0452591a2a4e8fd6af0fb80172f1ed2905bc7b14311d61f60260a"
            );
            
         } catch (TException tex) {
            tex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
           
    
    public static void s3s3Test(long fromNode, String fromKey, long size, String digest) 
        throws TException
    {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        boolean exec = true;
        
        System.out.println("\n***mainTest***\n"
                + " - node=" + fromNode + "\n"
                + " - key=" + fromKey + "\n"
                + " - size=" + size + "\n"
                + " - digest=" + digest + "\n"
                + " - exec=" + exec + "\n"
        );
        try {
            String toKey = toPrefix + fromKey;
            S3ToS3 s3ToS3 = S3ToS3.getS3ToS3("jar:nodes-remote",
                    fromNode, fromKey, toNode, toKey, maxBufSize);
            s3ToS3.setExec(exec);
            S3Reader s3Reader = s3ToS3.getS3Reader();
            System.out.println("S3Reader:" + "\n"
                    + " - readBucket:" + s3Reader.getBucket() + "\n"
                    + " - readKey:" + s3Reader.getKey() + "\n"
                    + " - readSize:" + s3Reader.getMetaObjectSize() + "\n"
                    + " - readSha256:" + s3Reader.getMetaSha256() + "\n"
                    + " - readMaxBufSize:" + s3Reader.getMaxBufSize() + "\n"
            );
            //s3ToS3.deleteTo();
            //s3ToS3.deleteTo();
            S3ToS3.S3ToS3Status runStatus = s3ToS3.copyOver(true);
            s3ToS3.deleteTo();
            
         } catch (TException tex) {
            tex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
           
    
    public static void s3FileS3Test(long fromNode, String fromKey, long size, String digest) 
        throws TException
    {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        boolean exec = true;
        
        System.out.println("\n***mainTest***\n"
                + " - node=" + fromNode + "\n"
                + " - key=" + fromKey + "\n"
                + " - size=" + size + "\n"
                + " - digest=" + digest + "\n"
                + " - exec=" + exec + "\n"
        );
        try {
            String tmpFilePath = "/dpr2/data/s3test/tmpstore.txt";
            String toKey = toPrefix + fromKey;
            S3ToS3 s3ToS3 = S3ToS3.getS3ToS3("jar:nodes-remote",fromNode, fromKey, toNode, toKey, maxBufSize);
            File tmpFile = new File(tmpFilePath);
            s3ToS3.fileLoadCopy(tmpFile);
            s3ToS3.validateFile(true);
            s3ToS3.deleteTo();
            
         } catch (TException tex) {
            tex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
        
    
}
