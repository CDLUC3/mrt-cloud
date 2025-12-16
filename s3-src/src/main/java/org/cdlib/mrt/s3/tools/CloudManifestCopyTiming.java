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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.text.StringEscapeUtils;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.VersionMapUtil;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.openstack.utility.OpenStackCmdAbs;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.pairtree.PairtreeCloud;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.MessageDigestType;
import org.cdlib.mrt.utility.StringUtil;

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
public class CloudManifestCopyTiming {
    
    protected static final String NAME = "CloudManifestCopy";
    protected static final String MESSAGE = NAME + ": ";
    
    
    protected CloudStoreInf inService = null;
    protected String inContainer = null;
    protected CloudStoreInf outService = null;
    protected String outContainer = null;
    protected File tFile = null;
    protected boolean showEntry = true;
    protected LoggerInf logger = null;
    protected String inSha256 = null;
    protected PrintWriter printWriter = null;
                    
    
    public static CloudManifestCopyTiming getCloudManifestCopyTiming(
            String nodeName,
            long inNode,
            long outNode,
            PrintWriter printWriter,
            LoggerInf logger
    ) 
        throws TException
    {
        NodeIO nodeIO = new NodeIO(nodeName, logger);
        nodeIO.printNodes("CloudManifestCopyNode" + nodeName);
        NodeIO.AccessNode inAccessNode = nodeIO.getAccessNode(inNode);
        NodeIO.AccessNode outAccessNode = nodeIO.getAccessNode(outNode);
        System.out.println("setServices:" 
                + " - inAccessNode.container=" + inAccessNode.container
                + " - outAccessNode.container=" + outAccessNode.container
        );
        CloudManifestCopyTiming cmc = new CloudManifestCopyTiming(inAccessNode.service, inAccessNode.container,
            outAccessNode.service, outAccessNode.container, printWriter, logger);
        return cmc;
    }
    
    public static CloudManifestCopyTiming getCloudManifestCopyTimingJSON(
            String nodeName,
            long inNode,
            long outNode,
            PrintWriter printWriter,
            LoggerInf logger
    ) 
        throws TException
    {
        NodeIO nodeIO = NodeIO.getNodeIOConfig(nodeName, logger);
        nodeIO.printNodes("CloudManifestCopyNode" + nodeName);
        NodeIO.AccessNode inAccessNode = nodeIO.getAccessNode(inNode);
        NodeIO.AccessNode outAccessNode = nodeIO.getAccessNode(outNode);
        System.out.println("setServices:" 
                + " - inAccessNode.container=" + inAccessNode.container
                + " - outAccessNode.container=" + outAccessNode.container
        );
        CloudManifestCopyTiming cmc = new CloudManifestCopyTiming(inAccessNode.service, inAccessNode.container,
            outAccessNode.service, outAccessNode.container, printWriter, logger);
        return cmc;
    }
    
    public CloudManifestCopyTiming(
            CloudStoreInf inService,
            String inContainer,
            CloudStoreInf outService,
            String outContainer,
            PrintWriter printWriter,
            LoggerInf logger)
        throws TException
    {
        this.inService = inService;
        this.inContainer = inContainer;
        this.outService = outService;
        this.outContainer = outContainer;
        this.printWriter = printWriter;
        this.logger = logger;
        try {
            tFile = FileUtil.getTempFile("tmp", ".txt");
        } catch (TException  tex) {
            throw tex;
            
        } catch (Exception  ex) {
            throw new TException (ex);
        }
    }
    
    public CloudResponse copy(CloudList.CloudEntry entry, Stat stat)
        throws TException
    {
        String inSHA256 = null;
        try {
            String key = entry.getKey();
            key = StringEscapeUtils.unescapeXml(key);
            entry.setKey(key);
            long startGetTime = DateUtil.getEpochUTCDate();
            CloudResponse inResponse = new CloudResponse(inContainer, key);
            inService.getObject(inContainer, key, tFile, inResponse);
            if (inResponse.getException() != null) {
                throw inResponse.getException();
            }
            long endGetTime = DateUtil.getEpochUTCDate() - startGetTime;
            inSHA256 = inResponse.getFileMetaProperty("sha-256");
            if (StringUtil.isAllBlank(inSHA256)) {
                inSHA256 = CloudUtil.getDigestValue("sha-256", tFile, logger);
                System.out.println("Calculate inSHA256");
            }
            String outSHA256 = entry.getEtag();
            //System.out.println("inSHA256:" + inSHA256);
            //System.out.println("outSHA256:" + outSHA256);
            long startPutTime = DateUtil.getEpochUTCDate();
            //System.out.println("inSize=" + tFile.length());
            CloudResponse outResponse = outService.putObject(outContainer, key, tFile);
            if (inResponse.getException() != null) {
                throw inResponse.getException();
            }
            long endPutTime = DateUtil.getEpochUTCDate() - startPutTime;
            String isoDate = DateUtil.getCurrentIsoDate();
                if (!inSHA256.equals(outSHA256)) {
                    throw new TException.INVALID_DATA_FORMAT("Copied content invalid:"
                            + " - key=" + key
                            + " - insize=" + tFile.length()
                            + " - entry.size=" + entry.size
                            + " - inSHA256=" + inSHA256
                            + " - outSHA256=" + outSHA256
                    );
                }
            printWriter.println("Entry>> - " + key + " - " + entry.size + " - " + endGetTime+ " - " + endPutTime);
            printWriter.flush();
            stat.getTime += endGetTime;
            stat.putTime += endPutTime;
            stat.objCnt++;
            stat.objSize += entry.size;
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
    
    public Stat copyObject(String ark)
        throws TException
    {
        try {
            long startTime = DateUtil.getEpochUTCDate();

            CloudList cloudList = getCloudList(ark);
            List<CloudList.CloudEntry> list = cloudList.getList();
            System.out.println(">>>Entries:" + list.size());
            long cnt = 0;
            Stat stat = new Stat(ark);
            for (CloudList.CloudEntry entry : list) {
                long timeVal = DateUtil.getEpochUTCDate();
                if (showEntry || ((cnt%1000) == 0)) 
                    System.out.println("***(" + cnt + "):" + entry.key + "*** - time=" + timeVal);
                copy(entry, stat);
                cnt++;
            }
            long endTime = DateUtil.getEpochUTCDate();
            String isoDate = DateUtil.getCurrentIsoDate();
            System.out.println("***copyObject timing cloud[" + isoDate + "]:"
                    + " - trans=" + (endTime - startTime)
            );
 
            return stat;
            
        } catch (TException tex) {
            throw tex;
            
        } finally {
            close();
        }
    }
    
    protected CloudList getCloudList(String arkS)
        throws TException
    {
        
        CloudList cloudList = new CloudList();
        try {
            Identifier ark = new Identifier(arkS);
            VersionMap versionMap = getVersionMap(ark);
            int current = versionMap.getCurrent();
            addManifest(arkS, cloudList);
            for (int v=1; v <= current; v++) {
                List<FileComponent> versionList = VersionMapUtil.getVersion(versionMap, v);
                for (FileComponent component : versionList) {
                    String key = component.getLocalID();
                    MessageDigest digest = component.getMessageDigest();
                    CloudUtil.KeyElements keyElements = CloudUtil.getKeyElements(key);
                    //CloudResponse response = inService.getObjectList(inContainer, key);
                    if (keyElements.versionID != v) continue;
                    //Need to allow zero length files - if (component.getSize() == 0) continue;
                    cloudList.add(inContainer, key, 
                            component.getSize(), 
                            digest.getValue(),
                            component.getMimeType(),
                            null);
                    if ((cloudList.size() % 100) == 0) {
                        String msg =  "dump[" + cloudList.size() + "]" + key;
                        System.out.println(msg);
                    }
                }
            }
            return cloudList;

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
            
    
    protected void addManifest(String arkS, CloudList cloudList)
        throws TException
    {
        try {
            String key = arkS + "|manifest";
            CloudResponse inResponse = new CloudResponse(inContainer, key);
            inService.getObject(inContainer, key, tFile, inResponse);
            long size = tFile.length();
            String manifestSHA256 = CloudUtil.getDigestValue("sha-256", tFile, logger);
            String manifestMimeType = "application/xml";
            CloudList.CloudEntry entry = new CloudList.CloudEntry(inContainer, key, size, manifestSHA256, manifestMimeType, null);
            cloudList.add(entry);
            System.out.println(entry.dump("addManifest"));

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    /**
     * Get version content information from a specific manifext.txt
     * @param versionFile manifest file
     * @return Version file content
     * @throws TException
     */
    protected VersionMap getVersionMap(Identifier objectID)
            throws TException
    {
        try {
            InputStream manifestXMLIn = inService.getManifest(inContainer, objectID);
            if (manifestXMLIn == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "cloud object not found:" + objectID.getValue());
            }
            return ManifestSAX.buildMap(manifestXMLIn, logger);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public void close() 
    {
        FileUtil.removeFile(tFile);
    }

    public void setShowEntry(boolean showEntry) {
        this.showEntry = showEntry;
    }
    
    
    public static class Test {
        public String val = "val";
    }
    
    public static class Stat
    {
        public String id = null;
        public long getTime = 0;
        public long putTime = 0;
        public long objSize = 0;
        public long objCnt = 0;
        public Stat(
                String id) 
        {
            this.id = id;
        }
        
        public String dump(String header)
        {
            StringBuffer buf = new StringBuffer();
            buf.append( header + ">>");
            buf.append(" - " + id);
            buf.append(" - " + objCnt);
            buf.append(" - " + objSize);
            buf.append(" - " + getTime);
            buf.append(" - " + putTime);
            return buf.toString();
        }
    }
}
