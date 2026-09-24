/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.maint.tools;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataValidate {
    private static boolean DEBUG = false;
    protected static final Logger log4j = LogManager.getLogger();   
    
    public static CloudChecksum dataCheck (
        boolean audit,
        String [] types, 
        CloudStoreInf service, 
        String bucket, 
        String key)
    throws TException
    {
        try {
            // CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, key);
            CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, key, 2000000);
            if (DEBUG) System.out.println("s3 properties:"
                    + " - metaObjectSize=" + cloudChecksum.getMetaObjectSize()
                    + " - metaSha256=" + cloudChecksum.getMetaSha256()
            );
            if (audit) {
                cloudChecksum.process();
                if (DEBUG) cloudChecksum.dump("the test");


                for (String type : types) {
                    String checksum = cloudChecksum.getChecksum(type);
                    if (DEBUG) System.out.println("getChecksum(" + type + "):" + checksum);
                }
            }
            return cloudChecksum;
            
        } catch (TException tex) {
            log4j.debug(tex.toString(), tex);
            throw tex;
            
        } catch (Exception ex) {
            log4j.debug(ex.toString(), ex);
            throw  new TException(ex);
        }
    }
}