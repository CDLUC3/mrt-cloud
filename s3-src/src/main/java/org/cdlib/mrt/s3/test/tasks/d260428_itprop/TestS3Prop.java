/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.test.tasks.d260428_itprop;
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
public class TestS3Prop {
    
    protected static String [] digestTypesS = {"md5", "sha256"};
    
    public static void main(String[] args) {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        try {
            propTest(9501, "ark:/13030/c8028pn1|1|producer/c8028pn1.mets.xml");
            propTest(9501, "ark:/13030/m50k26vp|1|producer/781292960.pdf");
            propTest(9501, "ark:/13030/m50g3hdx|1|producer/LCD11008_02.aif");
            propTest(9501, "ark:/13030/m5dz0c7f|5|producer/AS136-014-M.mp4");
            propTest(9501, "ark:/13030/m50k26vp|1|producer/781292960.pdf");
            
            propTest(9501, "ark:/13030/c8028pn1|manifest");
            propTest(9501, "ark:/13030/m50k26vp|manifest");
            propTest(9501, "ark:/13030/m50g3hdx|manifest");
            propTest(9501, "ark:/13030/m5dz0c7f|manifest");
            propTest(9501, "ark:/13030/m50k26vp|manifest");
           

         } catch (TException tex) {
            tex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
   
            
    
    public static void propTest(long node, String key) 
        throws TException
    {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        System.out.println("\n***mainTest***\n"
                + " - node=" + node + "\n"
                + " - key=" + key + "\n"
        );
        
        String checksumSha256 = null;
        try {
            //String jarBase = "yaml:2";
            String jarBase = "jar:nodes-remote";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(jarBase, logger) ;
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(node);
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            Properties metaProp = service.getObjectMeta(bucket, key);
            System.out.println(PropertiesUtil.dumpProperties(key, metaProp));

         } catch (TException tex) {
            tex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
}
