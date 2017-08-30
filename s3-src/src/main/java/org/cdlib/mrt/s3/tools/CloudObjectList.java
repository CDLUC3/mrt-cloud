package org.cdlib.mrt.s3.tools;

/*
 * Copyright 2010-2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A list of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.BufferedReader;
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
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.openstack.utility.ResponseValues;
import org.cdlib.mrt.s3.pairtree.PairtreeCloud;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudUtil;
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
public class CloudObjectList {
    
    protected static final String NAME = "CloudObjectList";
    protected static final String MESSAGE = NAME + ": ";
    
    
    protected CloudStoreInf service = null;
    protected String container = null;
    protected File tFile = null;
    protected LoggerInf logger = null;
    protected boolean showEntry = false;
                    
    public CloudObjectList(
            CloudStoreInf service,
            String container,
            LoggerInf logger)
        throws TException
    {
        this.service = service;
        this.container = container;
        this.logger = logger;
        try {
            tFile = FileUtil.getTempFile("tmp", "txt");
        } catch (TException  tex) {
            throw tex;
            
        } catch (Exception  ex) {
            throw new TException (ex);
        }
        
    }
    
    public int listObject(String ark)
        throws TException
    {
        long startTime = DateUtil.getEpochUTCDate();
        CloudResponse search = service.getObjectList(container, ark);
        CloudList cloudList = search.getCloudList();
        List<CloudList.CloudEntry> list = cloudList.getList();
        System.out.println("Entries:" + list.size());
        int cnt = 0;
        for (CloudList.CloudEntry entry : list) {
            list(entry);
            cnt++;
        }
        long endTime = DateUtil.getEpochUTCDate();
        String isoDate = DateUtil.getCurrentIsoDate();
        System.out.println("***listObject timing cloud[" + isoDate + "]:"
                + " - cnt=" + cnt
                + " - trans=" + (endTime - startTime)
        );
        return cnt;
    }
    
    
    public void list(CloudList.CloudEntry entry)
        throws TException
    {
        try {
            dummyTException();
            String key = entry.getKey();
            long startTime = DateUtil.getEpochUTCDate();
            System.out.println("Entry:"
                    + " - startTime=" + startTime
                    + " - container=" + container
                    + " - key=" + key
                    + " - size=" + entry.getSize()
                    + " - modified=" + entry.lastModified
                    + " - etag=" + entry.etag
            );
            return;
            
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
    
    private void dummyTException()
            throws TException
    {
        
    }
    
    
    public static class Test {
        public String val = "val";
    }
}
