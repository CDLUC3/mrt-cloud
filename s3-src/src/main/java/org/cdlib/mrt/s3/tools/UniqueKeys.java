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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Properties;

import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.URLEncoder;
import org.cdlib.mrt.utility.FileUtil;

import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * This routine generates a list of manifest URLS that can be used as a feed to inv zookeeper loader
 */
public class UniqueKeys {
    
    protected static final String NAME = "UniqueKeys";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    protected final static String NL = System.getProperty("line.separator");
    
    protected CloudStoreInf cloud = null;
    protected String container = null;
    protected String startKey = null;
    protected String DELIM = "\\|";
    protected Integer delimCnt = 1000;
    
    /**
     * Properties Constructor IdList
     * @param s3ServiceProp S3 service properties
     * @param runProp runtime properties
     * @throws TException 
     */
    public UniqueKeys(
            CloudStoreInf cloud,
            String container)
        throws TException
    {
        this.cloud = cloud;
        this.container = container;
        if (StringUtil.isAllBlank(container)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "container/bucket not supplied");
        }
    }
    
    public List<String> getKeys(String startKey, int outCnt)
        throws TException
    {
        ArrayList<String> keyList = new ArrayList<>(outCnt);
        CloudResponse response = cloud.getObjectList(container, startKey, outCnt);
        CloudList list = response.getCloudList();
        List<CloudList.CloudEntry> entryList = list.getList();
        for (CloudList.CloudEntry entry : entryList) {
            keyList.add(entry.getKey());
        }
        return keyList;
    }
    
    public List<String> getUnique(String startKey, int returnCnt, int delimCnt)
        throws TException
    {
        if (DEBUG) System.out.println("getUnique:" 
                + " - startKey=" + startKey
                + " - returnCnt=" + returnCnt
                + " - delimCnt=" + delimCnt
                );
        ArrayList<String> returnList = new ArrayList<String>(returnCnt);
        String lastStartKey = startKey;
        String lastUnique = "";
        while (true) {
            List<String> keys = getKeys(lastStartKey, 1000);
            if (DEBUG) System.out.println("keys size=" + keys.size());
            if (keys.isEmpty()) return returnList;
            List<String> uniqueList = getUniqueSegments(keys, delimCnt, lastUnique);
            if (DEBUG) System.out.println("uniqueList size=" + uniqueList.size());
            for (String unique : uniqueList) {
                if (returnList.size() < returnCnt) {
                    returnList.add(unique);
                } else {
                    return returnList;
                }
            }
            if (keys.size() < 1000) {
                return returnList;
            }
            lastStartKey = keys.get(keys.size() - 1);
            if (uniqueList.size() > 0) {
                lastUnique = uniqueList.get(uniqueList.size() - 1);
            }
        }
    }
    
    public List<String> getUniqueSegments(
            List<String> keys, 
            int delimCnt, 
            String lastUnique)
        throws TException
    {
        ArrayList<String> uniqueList = new ArrayList<String>();
        for (String key : keys) {
            String unique = getSegmentKey(key, delimCnt);
            if (DEBUG) System.out.println("getUniqueSegments:"
                    + " - key=" + key
                    + " - unique=" + unique
                    );
            if(!lastUnique.equals(unique)) {
                uniqueList.add(unique);
                lastUnique = unique;
            }
        }
        return uniqueList;
    }
    
    public String getSegmentKey(
            String key, 
            int delimCnt)
        throws TException
    {
            String[] parts = key.split(DELIM);
            String unique = "";
            if (delimCnt >= parts.length) {
                unique = key;
            } else {
                for (int i=0; i<delimCnt; i++) {
                    if (i > 0) unique += "|";
                    unique += parts[i];
                }
            }
            return unique;
    }
    
    
    public static void main(String[] args) throws IOException {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         *            AwsCredentials.properties file before you try to run this
         *            sample.
         * http://aws.amazon.com/security-credentials
         */
        main1(args);
    }
    
    
    
    public static void main1(String[] args) throws IOException {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         *            AwsCredentials.properties file before you try to run this
         *            sample.
         * http://aws.amazon.com/security-credentials
         */
        System.out.println("Run main1");
        TFrame tFrame = null;
        File outFile = null;
        try {
            String propertyList[] = {
                "resources/UniqueKeys.properties"};
            tFrame = new TFrame(propertyList, "UniqueKeys");
            Properties runProp = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("lockFile", 10, 10);
            OpenstackCloud cloud = OpenstackCloud.getOpenstackCloud(runProp, logger);
            UniqueKeys idList = new UniqueKeys(cloud, "dpr-9101");
            
            
            List<String> uniqueList = idList.getUnique("", 50, 2);
            System.out.println("***size=" + uniqueList.size());
            for (int i=0; i<uniqueList.size(); i++) {
                String unique = uniqueList.get(i);
                System.out.println("Found(" + i + "):" + unique);
            }
            
            uniqueList.clear();
            uniqueList = idList.getUnique("", 2000, 1);
            System.out.println("***size=" + uniqueList.size());
            for (int i=0; i<uniqueList.size(); i++) {
                String unique = uniqueList.get(i);
                System.out.println("Found(" + i + "):" + unique);
            }
                    
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
    }
}
