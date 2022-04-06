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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.lang3.StringEscapeUtils;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.VersionMapUtil;
import org.cdlib.mrt.core.DateState;
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
import org.cdlib.mrt.utility.PropertiesUtil;
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
public class CloudManifestCopyVersion {
    
    protected static final String NAME = "CloudManifestCopyVersion";
    protected static final String MESSAGE = NAME + ": ";
    protected static boolean DEBUG = false;
    protected static int NEEDCOPYLOG = 7;
    
    
    protected CloudStoreInf inService = null;
    protected String inContainer = null;
    protected CloudStoreInf outService = null;
    protected String outContainer = null;
    protected File tFile = null;
    protected boolean showEntry = true;
    protected LoggerInf logger = null;
    protected String inSha256 = null;
    protected Properties outManProp = null;
    
                    
    
    public static CloudManifestCopyVersion getCloudManifestCopyVersion(
            NodeIO nodeIO,
            long inNode,
            long outNode,
            LoggerInf logger
        ) 
        throws TException
    {
        NodeIO.AccessNode inAccessNode = nodeIO.getAccessNode(inNode);
        if (inAccessNode == null) {
            throw new TException.INVALID_OR_MISSING_PARM("getCloudManifestCopyVersion inNode not found:" + inNode);
        }
        NodeIO.AccessNode outAccessNode = nodeIO.getAccessNode(outNode);
        if (outAccessNode == null) {
            throw new TException.INVALID_OR_MISSING_PARM("getCloudManifestCopyVersion outNode not found:" + outNode);
        }
        System.out.println("setServices:" 
                + " - inAccessNode.container=" + inAccessNode.container
                + " - outAccessNode.container=" + outAccessNode.container
        );
        CloudManifestCopyVersion cmcv = new CloudManifestCopyVersion(inAccessNode.service, inAccessNode.container,
            outAccessNode.service, outAccessNode.container, logger);
        return cmcv;
    }
                    
    public CloudManifestCopyVersion(
            CloudStoreInf inService,
            String inContainer,
            CloudStoreInf outService,
            String outContainer,
            LoggerInf logger)
        throws TException
    {
        this.inService = inService;
        this.inContainer = inContainer;
        this.outService = outService;
        this.outContainer = outContainer;
        this.logger = logger;
        try {
            tFile = FileUtil.getTempFile("tmp", ".txt");
        } catch (TException  tex) {
            throw tex;
            
        } catch (Exception  ex) {
            throw new TException (ex);
        }
    }
    
    protected boolean needCopy(CloudList.CloudEntry inEntry, Stat stat)
        throws TException
    {
        CloudList.CloudEntry outEntry = null;
        try {
            // outManProp set to null if no output manifest exists at startup
            /*
            Output manifest not required to determine if item already exists
            Equivalent to a Glacier audit
            if (outManProp == null) {
                log(NEEDCOPYLOG, "needCopy outManProp null: true");
                return true;
            }
            */
            if (DEBUG) System.out.println(inEntry.dump("In-entrydump"));
            long startTestTime = DateUtil.getEpochUTCDate();
            String key = inEntry.getKey();
            key = StringEscapeUtils.unescapeXml(key);
            
            inEntry.setKey(key);
            Properties objectMeta = null;
            try {
                objectMeta = outService.getObjectMeta(outContainer, key);
            } catch (Exception ex) {
                log(NEEDCOPYLOG, "needCopy(" + key + ") Error getObjectMeta: true - ex:" + ex);
                return true;
            }
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("***META***", objectMeta));
            outEntry = CloudResponse.getCloudEntry(objectMeta);
            if (outEntry == null) {
                log(NEEDCOPYLOG, "needCopy(" + key + ")  no meta: needCopy=TRUE");
                return true;
            }
            if (DEBUG) System.out.println(outEntry.dump("out-entrydump"));
            if (inEntry.getDigest() == null) {
                log(NEEDCOPYLOG, "needCopy(" + key + ")  no inEntry digest - needCopy=TRUE");
                return true;
            }
            MessageDigest inCompDigest = inEntry.getDigest();
            String inDigestValue = inCompDigest.getValue();
            long inSize = inEntry.getSize();
            
            MessageDigest outCompDigest = outEntry.getDigest();
            String outDigestValue = outCompDigest.getValue();
            long outSize = outEntry.getSize();
            stat.metaTime += DateUtil.getEpochUTCDate()-startTestTime;
            if (inDigestValue.equals(outDigestValue) && (inSize == outSize)) {
                log(NEEDCOPYLOG, "needCopy(" + key + ")  needCopy=FALSE");
                return false;
            }
            log(NEEDCOPYLOG, "needCopy(" + key + ")  miss match: true"
                    + " - inSize:" + inSize
                    + " - outSize:" + outSize
                    + " - inDigestValue:" + inDigestValue
                    + " - outDigestValue:" + outDigestValue
            );
            return true;
            
        } catch (Exception ex) {
            if (DEBUG) System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    } 
    
    public void setOutManifestProp(String ark)
        throws TException
    {
        try {
            if (StringUtil.isAllBlank(ark)) {
                throw new TException.INVALID_OR_MISSING_PARM("setOutManifestProp ark missing)");
            }
            
            try {
                String key = ark + "|manifest";
                outManProp = outService.getObjectMeta(outContainer, key);
                if ((outManProp == null) || (outManProp.size() == 0)) {
                    outManProp = null;
                    log(10, "setOutManifestProp manifest ark empty:" + key);
                    return;
                }
                log(10, PropertiesUtil.dumpProperties("setOutManifestProp manfest found", outManProp));
                
            } catch (Exception ex) {
                outManProp = null;
                log(10, "setOutManifestProp manifest ark null");
            }
            
        } catch (Exception ex) {
            if (DEBUG) System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    } 
    
    protected CloudResponse copy(CloudList.CloudEntry entry, Stat stat)
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
            inSHA256 = inResponse.getSha256();
            if (StringUtil.isAllBlank(inSHA256)) {
                inSHA256 = CloudUtil.getDigestValue("sha-256", tFile, logger);
                log(1, "Calculate inSHA256:" + key);
            }
            String entrySHA256 = entry.getEtag();
            //System.out.println("inSHA256:" + inSHA256);
            //System.out.println("outSHA256:" + outSHA256);
            long startPutTime = DateUtil.getEpochUTCDate();
            //System.out.println("inSize=" + tFile.length());
            CloudResponse outResponse = outService.putObject(outContainer, key, tFile);
            if (outResponse.getException() != null) {
                //System.out.println("CloudManifestCopyTime: getException not null:" + outResponse.getException());
                throw outResponse.getException();
            }
            String outSHA256 = outResponse.getSha256();
            //System.out.println("CloudManifestCopyTime: after putObject");
            long endPutTime = DateUtil.getEpochUTCDate() - startPutTime;
            String isoDate = DateUtil.getCurrentIsoDate();
            if (!entrySHA256.equals(outSHA256)) {
                String msg = "$$$Digests:"
                        + " - inSHA256:" + inSHA256
                        + " - entrySHA256:" + entrySHA256
                        + " - outSHA256:" + outSHA256
                        ;
                log(1, msg);
                System.out.println(msg);
                throw new TException.INVALID_DATA_FORMAT("Copied content invalid:"
                        + " - key=" + key
                        + " - insize=" + tFile.length()
                        + " - entry.size=" + entry.size
                        + " - entrySHA256=" + entrySHA256
                        + " - outSHA256=" + outSHA256
                );
            }
            stat.getTime += endGetTime;
            stat.putTime += endPutTime;
            stat.objSize += entry.size;
            return outResponse;
            
        } catch (TException tex) {
            if (DEBUG) System.out.println("TException:" + tex);
            if (DEBUG) tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            if (DEBUG) System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    } 
    
    public CloudResponse copyRetry(CloudList.CloudEntry entry, Stat stat, int retryCnt)
        throws TException
    {
        TException retEx = null;
        for (int retry=1; retry <= retryCnt; retry++) {
            try {
                CloudResponse response = copy(entry, stat);
                return response;

            } catch (TException tex) {
                retEx = tex;
                String key = entry.getKey();
                if (key == null) key = "";
                key = StringEscapeUtils.unescapeXml(key);
                String errMsg =  "copyRetry(" + retry + "):"
                        + " - key:" + key
                        + " - Exception:" + retEx;
                log(3, errMsg);
                System.out.println(errMsg);
                tex.printStackTrace();
            } 
            long expSleep = 1000*(5*retry);
            if (retry == retryCnt) break;
            try {
                System.out.println(MESSAGE  + "sleep:" + expSleep);
                Thread.sleep (expSleep);
            } catch (Exception slex) { }
        }
        throw retEx;
    } 
    
    public void copyObject(String ark, CloudManifestCopyVersion.Stat stat)
        throws TException
    {
        try {
            long startTime = DateUtil.getEpochUTCDate();
            stat.objCnt++;
            setOutManifestProp(ark);
            CloudList cloudList = getCloudList(ark);
            List<CloudList.CloudEntry> list = cloudList.getList();
            log(">>>Entries:" + list.size());
            long cnt = 0;
            for (CloudList.CloudEntry entry : list) {
                long timeVal = DateUtil.getEpochUTCDate();
                if (showEntry || ((cnt%1000) == 0)) 
                    log(6, "***(" + cnt + "):" + entry.key + "***" 
                            + " - time=" + timeVal
                            + " - size=" + entry.size
                    );
                if (needCopy(entry, stat)) {
                    copyRetry(entry, stat, 3);
                    stat.fileCopyCnt++;
                    stat.fileCopySize += entry.getSize();
                }
                cnt++;
            }
            long endTime = DateUtil.getEpochUTCDate();
            String isoDate = DateUtil.getCurrentIsoDate();
            if (DEBUG) System.out.println("***copyObject timing cloud[" + isoDate + "]:"
                    + " - trans=" + (endTime - startTime)
            );
            
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
            int actualCnt = versionMap.getActualCnt();
            if ((current == 0) ||(actualCnt == 0)) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "Empty Manifest.xml for object:" + arkS
                );
            }
            for (int v=1; v <= current; v++) {
                List<FileComponent> versionList = VersionMapUtil.getVersion(versionMap, v);
                for (FileComponent component : versionList) {
                    String key = component.getLocalID();
                    MessageDigest digest = component.getMessageDigest();
                    CloudUtil.KeyElements keyElements = CloudUtil.getKeyElements(key);
                    //CloudResponse response = inService.getObjectList(inContainer, key);
                    if (keyElements.versionID != v) continue;
                    //Need to allow zero length files - if (component.getSize() == 0) continue;
                    cloudList.add(inContainer, 
                            key, 
                            component.getSize(), 
                            digest.getValue(), 
                            component.getMimeType(), 
                            null, 
                            digest, 
                            null);
           
                    if ((cloudList.size() % 100) == 0) {
                        String msg =  "dump[" + cloudList.size() + "]" + key;
                        log(msg);
                    }
                }
            }
            addManifest(arkS, cloudList);
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
    
    protected void log(String msg)
    {
        logger.logMessage(msg, 5, true);
    }
    
    protected void logxx(String msg, int lvl)
    {
        logger.logMessage(msg, lvl, true);
    }
    
    protected void log(int lvl, String msg)
    {
        logger.logMessage(msg, lvl, true);
    }
    
    public static class Test {
        public String val = "val";
    }
    
    public static class Stat
    {
        public String id = null;
        public long metaTime = 0;
        public long getTime = 0;
        public long putTime = 0;
        public long objSize = 0;
        public long objCnt = 0;
        public long fileCopyCnt = 0;
        public long fileCopySize = 0;
        public DateState start = new DateState();
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
            buf.append(" - objCnt:" + objCnt);
            buf.append(" - fileCopyCnt:" + fileCopyCnt);
            buf.append(" - fileCopySize:" + fileCopySize);
            buf.append(" - objSize:" + objSize);
            buf.append(" - metaTime:" + metaTime);
            buf.append(" - getTime:" + getTime);
            buf.append(" - putTime:" + putTime);
            return buf.toString();
        }
    }
}
