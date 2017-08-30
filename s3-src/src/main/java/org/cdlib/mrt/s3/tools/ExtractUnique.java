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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import java.nio.charset.Charset;

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
public class ExtractUnique {
    
    protected static final String NAME = "ExtractUnique";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    public static final int UNIQUE_SIZE = 1000;

    protected final static String NL = System.getProperty("line.separator");
    
    protected CloudStoreInf cloud = null;
    protected String container = null;
    protected String startKey = null;
    protected File containerDir = null;
    protected BufferedReader br = null;
    protected File containerList = null;
    protected long numContainers = 0;
    protected long fileCnt = 0;
    protected LoggerInf logger = null;
    protected ArrayList<ContainerInfo> containers = new  ArrayList();
    protected long maxout = 5000000;
    
    
    /**
     * Properties Constructor IdList
     * @param s3ServiceProp S3 service properties
     * @param runProp runtime properties
     * @throws TException 
     */
    public ExtractUnique(
            CloudStoreInf cloud,
            File containerDir,
            Long maxout,
            LoggerInf logger)
        throws TException
    {
        try {
            this.cloud = cloud;
            this.logger = logger;
            if (logger == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger required");
            }
            this.containerDir = containerDir;
            if (containerDir == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "container/bucket list not supplied");
            }
            containerList = new File(containerDir, "containerList.txt");
            
            FileInputStream fis = new FileInputStream(this.containerList);
            br = new BufferedReader(new InputStreamReader(fis,
                    Charset.forName("UTF-8")));
            if (maxout != null) {
                this.maxout = maxout;
            } 
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }  
    
    public static void main(String[] args) throws IOException {
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
                "resources/ExtractUnique.properties"};
            tFrame = new TFrame(propertyList, "UniqueKeys");
            Properties runProp = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("lockFile", 10, 10);
            OpenstackCloud cloud = OpenstackCloud.getOpenstackCloud(runProp, logger);
            String outDirS = runProp.getProperty("runDir");
            File outDir = new File(outDirS);
            ExtractUnique eu = new ExtractUnique(cloud, outDir, 500L, logger);
            eu.process();
            
                    
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
    }
    
    public void process()
        throws TException
    {
        String line = null;
        try {
            while ((line = br.readLine()) != null) {
                if (fileCnt > maxout) break;
                if (StringUtil.isAllBlank(line)) continue;
                if (line.startsWith("#")) continue;
                if (line.endsWith("__")) {
                    processDistributedContainer(line);
                } else {
                    processContainer(line);
                }
            }
            String dumpOut = dump("FINAL");
            System.out.println(dumpOut);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    public void processContainer(String container)
        throws TException
    {
        if (DEBUG) System.out.println("***Container" + container);
        ContainerInfo info = new ContainerInfo(container);
        containers.add(info);
        numContainers++;
        File outFile = null;
        FileOutputStream fos = null;
        try {
            String containerFileS = container + ".txt";
            outFile = new File(containerDir, containerFileS);
            fos = new FileOutputStream(outFile);
            UniqueKeys idList = new UniqueKeys(cloud, container);
            String key = "";
            while(true) {
                List<String> uniqueList = null;
                uniqueList = idList.getUnique(key, UNIQUE_SIZE, 1);
                System.out.println("***size=" + uniqueList.size());
                if (uniqueList.size() <= 0) break;
                String unique = null; 
                for (int i=0; i<uniqueList.size(); i++) {
                    unique = uniqueList.get(i);
                    logger.logMessage("Found(" + i + "):" + unique, 5);
                    unique += '\n';
                    if (key.equals("") || i > 0) {
                        fos.write(unique.getBytes("utf-8"));
                        fileCnt++;
                        info.cnt++;
                    }
                    if (fileCnt > maxout) return;
                }
                if (uniqueList.size() < UNIQUE_SIZE) break;
                key = unique;
                if (DEBUG) System.out.println("***key:" + key 
                        + " - numContainers=" + numContainers
                        + " - info.cnt=" + info.cnt
                );
                uniqueList.clear();
            }
            info.complete = true;
            if (DEBUG) System.out.println("***container:" + info.name 
                    + " - numContainers=" + numContainers
                    + " - info.cnt=" + info.cnt
            );
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("processContainer Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                    System.out.println("***Close:" + outFile.getAbsolutePath());
                }
            } catch (Exception ex) { }
        }
        
    }
    
    public void processDistributedContainer(String distribContainer)
        throws TException
    {
        if (fileCnt > maxout) return;
        String [] hex = {"0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"};
        for(String s1 : hex) {
            for(String s2 : hex) {
                for(String s3 : hex) {
                    if (fileCnt > maxout) return;
                    String dist = distribContainer + s1 + s2 + s3;
                    try {
                        processContainer(dist);
                    } catch (Exception ex) {
                        logger.logError("Failed container:" + dist, 0);
                    }
                }
            }
        }
    }
    
    public String dump(String header)
    {
        StringBuffer buf = new StringBuffer();
        try {
            buf.append("***" + header + "***\n");
            buf.append(" - containerDir:" + containerDir.getCanonicalPath() + "\n");
            buf.append(" - numContainers:" + numContainers + "\n");
            buf.append(" - fileCnt:" + fileCnt + "\n");
            buf.append("***** CONTAINERS\n");
            for (ContainerInfo info: containers) {
                buf.append(info.name + ": " + info.cnt + " - " + info.complete + "\n");
            }
            return buf.toString();

        } catch (Exception ex) {
            return "Exception: " + ex;
        }
    }
    
    public static class ContainerInfo
    {
        public long cnt = 0;
        public String name = null;
        public boolean complete = false;
        public ContainerInfo(String containerName)
        {
            this.name = containerName;
        }
    }
}
