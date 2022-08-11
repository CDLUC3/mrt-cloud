package org.cdlib.mrt.s3.aws;

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
import com.amazonaws.services.s3.model.StorageClass;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.commons.lang3.StringEscapeUtils;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.cloud.VersionMapUtil;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

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
public class AWSObjectStorageClassConvert {
    
    protected static final String NAME = "AWSObjectStorageClassConvert";
    protected static final String MESSAGE = NAME + ": ";
    protected static boolean DEBUG = false;
    protected static boolean convertstat = false;
    
    
    protected AWSS3Cloud service = null;
    protected String bucket = null;
    protected LoggerInf logger = null;
    protected StorageClass targetStorageClass = null;
                    
    public AWSObjectStorageClassConvert(
            AWSS3Cloud service,
            String bucket,
            StorageClass targetStorageClass,
            LoggerInf logger)
        throws TException
    {
        this.service = service;
        this.bucket = bucket;
        this.targetStorageClass = targetStorageClass;
        this.logger = logger;
        
    }
    
    public static void main(String[] args) throws IOException,TException 
    {
        String bucket     = "uc3-s3mrt5001-dev";
	String ark        = "ark:/99999/fk4rx9wpk";
        StorageClass targetStorageClass = StorageClass.ReducedRedundancy;
        //StorageClass targetStorageClass = StorageClass.Standard;
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        try {
            AWSS3Cloud service = AWSS3Cloud.getAWSS3(logger);
            AWSObjectStorageClassConvert objectConvert 
                    = new AWSObjectStorageClassConvert(service, bucket, targetStorageClass, logger);
            objectConvert.convertObject(ark);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public CloudResponse convert(CloudList.CloudEntry entry, ObjectStats stats)
        throws TException
    {
        
        try {
            String key = entry.getKey();
            key = StringEscapeUtils.unescapeXml(key);
            entry.setKey(key);
            long startTime = DateUtil.getEpochUTCDate();
            CloudResponse response = service.convertStorageClass(bucket, key, targetStorageClass);
            long inTime = DateUtil.getEpochUTCDate();
            long duration = inTime-startTime;
            float sdd = entry.getSize() / duration;
            stats.fileCnt++;
            stats.size += entry.getSize();
            stats.time += duration;
            if (convertstat) System.out.println("convert:+" + key + "+" + entry.getSize() + "+" + duration + "+" + sdd);
            return response;
            
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
    
    public ObjectStats convertObject(String ark)
        throws TException
    {
        try {
            long startTime = DateUtil.getEpochUTCDate();

            CloudList cloudList = getCloudList(ark);
            List<CloudList.CloudEntry> list = cloudList.getList();
            if (DEBUG) System.out.println(">>>Entries:" + list.size());
            long cnt = 0;
            ObjectStats stats = new ObjectStats(ark);
            for (CloudList.CloudEntry entry : list) {
                if (entry.getSize() >= 5000000000L) {
                    String msg = "BIG COMPONENT:"
                            + " - key=" + entry.key
                            + " - size=" + entry.getSize();
                    logger.logMessage(msg, 0, true);
                    System.out.println(msg);
                    stats.bigCnt++;
                    continue;
                }
                long timeVal = DateUtil.getEpochUTCDate();
                if (DEBUG)
                    System.out.println("***(" + cnt + "):" + entry.key + "*** - time=" + timeVal);
                CloudResponse response = convert(entry, stats);
                if (DEBUG) System.out.println("Convert key:" + response.isStorageClassConverted());
                cnt++;
            }
            long endTime = DateUtil.getEpochUTCDate();
            String isoDate = DateUtil.getCurrentIsoDate();
            if (DEBUG) System.out.println("***copyObject timing cloud[" + isoDate + "]:"
                    + " - trans=" + (endTime - startTime)
            );
            stats.validateCnt = validateStorageClass(ark, list);
            return stats;
            
        } catch (TException tex) {
            throw tex;
            
        } finally {
            close();
        }
    }
    
    public ObjectStats testObject(String ark)
        throws TException
    {
        ObjectStats stats = new ObjectStats(ark);
        try {
            long startTime = DateUtil.getEpochUTCDate();

            CloudList cloudList = getCloudList(ark);
            List<CloudList.CloudEntry> list = cloudList.getList();
            if (DEBUG) System.out.println(">>>Entries:" + list.size());
            long cnt = 0;
            for (CloudList.CloudEntry entry : list) {
                if (entry.getSize() >= 5000000000L) {
                    String msg = "BIG COMPONENT:"
                            + " - key=" + entry.key
                            + " - size=" + entry.getSize();
                    logger.logMessage(msg, 0, true);
                    System.out.println(msg);
                    stats.bigCnt++;
                    continue;
                }
            }
            return stats;
            
        } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
            System.out.println("Not found:" + ark);
            return stats;
            
        } catch (TException tex) {
            throw tex;
            
        } finally {
            close();
        }
    }
    
    public int validateStorageClass(String ark, List<CloudList.CloudEntry> list)
        throws TException
    {
        for (CloudList.CloudEntry entry : list) {
            if (entry.getSize() >= 5000000000L) {
                    String msg = "BIG COMPONENT VALIDATE FAIL:"
                            + " - key=" + entry.key
                            + " - size=" + entry.getSize();
                    logger.logMessage(msg, 0, true);
                    System.out.println(msg);
                    continue;
            }
            String key = entry.key;
            StorageClass storageClass = null;
            for (int t=1; t<=5; t++) {
                storageClass = service.getStorageClass(bucket, key);
                if (storageClass == targetStorageClass) break;
                String msg = "Warning retry validate(" + t + "):ConvertStorageClass: Conversion fails on StorageClass:" 
                        + " - bucket=" + bucket
                        + " - key=" + key;
                System.out.println(msg);
                try {
                    Thread.sleep(t * 3000L);
                } catch (Exception sex) { }
            }
            if (storageClass != targetStorageClass) {
                throw new TException.INVALID_DATA_FORMAT("StorageClass validate mismatch:"
                        + " - key:" + entry.key
                        + " - storageClass:" + storageClass
                        + " - targetStorageClass:" + targetStorageClass
                );

            }
            if (DEBUG) System.out.println("StorageClass match:"
                        + " - key:" + entry.key
                        + " - storageClass:" + storageClass
                        + " - targetStorageClass:" + targetStorageClass
            );
        }
        return list.size();
    }
    
    protected CloudList getCloudList(String arkS)
        throws TException
    {
        
        CloudList cloudList = new CloudList();
        try {
            Identifier ark = new Identifier(arkS);
            VersionMap versionMap = getVersionMap(ark);
            int current = versionMap.getCurrent();
            for (int v=1; v <= current; v++) {
                List<FileComponent> versionList = VersionMapUtil.getVersion(versionMap, v);
                for (FileComponent component : versionList) {
                    String key = component.getLocalID();
                    MessageDigest digest = component.getMessageDigest();
                    CloudUtil.KeyElements keyElements = CloudUtil.getKeyElements(key);
                    //CloudResponse response = inService.getObjectList(inContainer, key);
                    if (keyElements.versionID != v) continue;
                    //Need to allow zero length files - if (component.getSize() == 0) continue;
                    cloudList.add(bucket, key, 
                            component.getSize(), 
                            digest.getValue(),
                            component.getMimeType(),
                            null);
                    
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
            
            CloudList.CloudEntry entry = new CloudList.CloudEntry(bucket, key, 10, "", "", null);
            cloudList.add(entry);
            if (DEBUG)System.out.println(entry.dump("addManifest"));

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
            InputStream manifestXMLIn = service.getManifest(bucket, objectID);
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
        
    }

    public void setShowEntry(boolean DEBUG) {
        this.DEBUG = DEBUG;
    }

    public String getBucket() {
        return bucket;
    }
    
    
    public static class Test {
        public String val = "val";
    }
    
    public static class ObjectStats {
        public String ark = null;
        public long fileCnt = 0;
        public long validateCnt = 0;
        public long bigCnt = 0;
        public long size = 0;
        public long time = 0;
        public ObjectStats(String ark) {
            this.ark = ark;
        }
    }
}
