package org.cdlib.mrt.s3.tools;

/*
 * Copyright 2010-2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.text.StringEscapeUtils;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.openstack.utility.OpenStackCmdAbs;
import org.cdlib.mrt.s3.pairtree.PairtreeCloud;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.FileUtil;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using
 * the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on
 * Amazon S3, see http://aws.amazon.com/s3.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in the
 *                   AwsCredentials.properties file before you try to run this
 *                   sample.
 * http://aws.amazon.com/security-credentials
 */
public class CloudCloudCopy {
    
    protected static final String NAME = "CloudCloudCopy";
    protected static final String MESSAGE = NAME + ": ";
    
    
    protected CloudStoreInf inService = null;
    protected String inContainer = null;
    protected CloudStoreInf outService = null;
    protected String outContainer = null;
    protected File tFile = null;
    protected boolean showEntry = true;
                    
    public CloudCloudCopy(
            CloudStoreInf inService,
            String inContainer,
            CloudStoreInf outService,
            String outContainer)
        throws TException
    {
        this.inService = inService;
        this.inContainer = inContainer;
        this.outService = outService;
        this.outContainer = outContainer;
        try {
            tFile = FileUtil.getTempFile("tmp", "txt");
        } catch (TException  tex) {
            throw tex;
            
        } catch (Exception  ex) {
            throw new TException (ex);
        }
        
    }
                    
    public CloudCloudCopy(
            CloudStoreInf inService,
            String inContainer,
            CloudStoreInf outService,
            String outContainer,
            File tFile)
        throws TException
    {
        this.inService = inService;
        this.inContainer = inContainer;
        this.outService = outService;
        this.outContainer = outContainer;
        this.tFile = tFile;
        
    }
    
    public CloudResponse copy(CloudList.CloudEntry entry)
        throws TException
    {
        try {
            String key = entry.getKey();
            key = StringEscapeUtils.unescapeXml(key);
            entry.setKey(key);
            String inMd5 = entry.getEtag();
            long startTime = DateUtil.getEpochUTCDate();
            CloudResponse inResponse = new CloudResponse(inContainer, key);
            inService.getObject(inContainer, key, tFile, inResponse);
            long inTime = DateUtil.getEpochUTCDate();
            //System.out.println("inSize=" + tFile.length());
            CloudResponse outResponse = outService.putObject(outContainer, key, tFile);
            String outMd5 = outResponse.getMd5();
            long endTime = DateUtil.getEpochUTCDate();
            String isoDate = DateUtil.getCurrentIsoDate();
            if (tFile.length() <= OpenStackCmdAbs.SEGSIZE) {
                if (showEntry) System.out.println("***addVersion timing cloud[" + key + "," + isoDate + "]:"
                        + " - inMd5=" + inMd5
                        + " - outMd5=" + outMd5
                        + " - get=" + (inTime - startTime)
                        + " - out=" + (endTime - inTime)
                        + " - trans=" + (endTime - startTime)
                );
                if (!inMd5.equals(outMd5)) {
                    throw new TException.INVALID_DATA_FORMAT("Copied content invalid:"
                            + " - key=" + key
                            + " - insize=" + tFile.length()
                            + " - entry.size=" + entry.size
                            + " - inMd5=" + inMd5
                            + " - outMd5=" + outMd5
                    );
                }
            }
            return outResponse;
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public void copyObject(String ark)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
            
        CloudResponse search = inService.getObjectList(inContainer, ark);
        CloudList cloudList = search.getCloudList();
        List<CloudList.CloudEntry> list = cloudList.getList();
        System.out.println("Entries:" + list.size());
        long cnt = 0;
        for (CloudList.CloudEntry entry : list) {
            if (showEntry || ((cnt%1000) == 0)) 
                System.out.println("***(" + cnt + "):" + entry.key + "***");
            copy(entry);
            cnt++;
        }
        long endTime = DateUtil.getEpochUTCDate();
        String isoDate = DateUtil.getCurrentIsoDate();
        System.out.println("***copyObject timing cloud[" + isoDate + "]:"
                + " - trans=" + (endTime - startTime)
        );
    }
    
    public void close() {
        try {
            tFile.delete();
        } catch (Exception ex) { }
    }

    public void setShowEntry(boolean showEntry) {
        this.showEntry = showEntry;
    }
    
    
    public static class Test {
        public String val = "val";
    }
}
