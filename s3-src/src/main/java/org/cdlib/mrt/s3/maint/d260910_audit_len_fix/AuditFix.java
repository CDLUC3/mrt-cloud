/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.maint.d260910_audit_len_fix;


import java.util.ArrayList;
import java.util.Properties;
import java.io.File;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;

import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.test.TestCloudChecksum;
import org.cdlib.mrt.s3.tools.CloudChecksum;
import org.cdlib.mrt.utility.Checksums;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.s3.maint.tools.DataValidate;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;

public class AuditFix {
    public static String manifestKey = "ark:/13030/m533239c|manifest";
    public static String oldManifestFile = "/home/loy/s3/minio/260720-misskeys/manifest/oldman.xml";
    public static String oldSha256 = "2b5d9b86a14a4b461efcdc95fe7f4cf557db26f84ff0f03dc94674e7f33f2c4b";
    public static long oldLen = 100621521;
    
    
    //public static String newManifestFile = "/home/loy/s3/minio/260720-misskeys/manifest/mannew.xml";
    //public static String newSha256 = "542ee541cdbd4f4af7afa4f94202250c4eea80d5c11ac6b10dff50a9f8b6067b";
    //public static long newLen = 100621292;
    
    public static String newManifestFile = "/home/loy/s3/minio/260720-misskeys/manifest/mannew.xml";
    public static String newSha256 = "eda6f10e779ac41fb139010c99fb5fc86fdc1f9dc66f70ccc79c5e8ca5e9494d";
    public static long newLen = 100621304;
    
    
    public static NodeIO.AccessNode nSource = null;
    public static NodeIO.AccessNode nTarget = null;
            
    private static LoggerInf logger = new TFileLogger("test", 50, 50);
       //String key = "ark:/13030/m53z94nj|manifest"; // hang - now works
    public static void main(String[] argv) {
        try {
            String yamlName = "jar:nodes-remote";
            long node = 9501;
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            nSource = nodeIO.getAccessNode(2001);
            nTarget = nodeIO.getAccessNode(7502);
            
            testIn(nSource, "ark:/13030/m5vf7wfv|1|system/mrt-ingest.txt", 
                    1714, 
                    "9a0b507f141f15bc1e81e8c1f5989698cb5e30f93a2cbc8b96a3f43b939e33b6"
            );
            
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
    
    public static void testIn(
            NodeIO.AccessNode accNode,
            String key,
            long size,
            String sha256)
        throws TException
    {
        try {
            AWSS3V2Cloud service = (AWSS3V2Cloud)accNode.service;
            String bucket = accNode.container;
            String [] types = {"sha256"};
            boolean match = validate(true, types, service, bucket, key, size, sha256);
                    
            
        } catch (Exception ex) {
            
        }
    }
    
    protected static boolean validate(
        boolean audit,
        String [] types, 
        CloudStoreInf service, 
        String bucket, 
        String key,
        long size,
        String sha256)
        throws TException
    {
                    
        CloudChecksum checksum = DataValidate.dataCheck (audit, types, service, bucket, key);
        checksum.process();
        long dataSize = checksum.getInputSize();
        String dataSha256 = checksum.getChecksum("sha256");
        System.out.println("validate"
                + " - dataSize=" + dataSize
                + " - dataSha256=" + dataSha256
        );
        if ((size == dataSize) && (sha256.equals(dataSha256)))
        {
            return true;
        }
        return false;
    }
    
}