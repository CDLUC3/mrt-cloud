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
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.openstack.utility.OpenStackCmdAbs;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.pairtree.PairtreeCloud;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudUtil;
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
public class ComponentListManifest1 {
    
    protected static final String NAME = "CloudManifestCopy";
    protected static final String MESSAGE = NAME + ": ";
    
    
    protected CloudStoreInf inService = null;
    protected String inContainer = null;
    protected String storageClass = null;
    protected LoggerInf logger = null;
                    
    public ComponentListManifest1(
            String storageClass,
            CloudStoreInf inService,
            String inContainer,
            LoggerInf logger)
        throws TException
    {
        this.inService = inService;
        this.inContainer = inContainer;
        this.storageClass = storageClass;
        this.logger = logger;
    }
    
    public CloudList buildCloudList(String arkS)
        throws TException
    {
        
        CloudList cloudList = new CloudList();
        try {
            Identifier ark = new Identifier(arkS);
            VersionMap versionMap = getVersionMap(ark);
            int current = versionMap.getCurrent();
                System.out.println("***Current:" + current);
            addManifest(arkS, cloudList);
            for (int v=1; v <= current; v++) {
                System.out.println("Version:" + v);
                List<FileComponent> versionList = VersionMapUtil.getVersion(versionMap, v);
                for (FileComponent component : versionList) {
                    String key = component.getLocalID();
                    System.out.println("key:" + key);
                    MessageDigest digest = component.getMessageDigest();
                    CloudUtil.KeyElements keyElements = CloudUtil.getKeyElements(key);
                    //CloudResponse response = inService.getObjectList(inContainer, key);
                    //if (keyElements.versionID != v) continue;
                    //Need to allow zero length files - if (component.getSize() == 0) continue;
                    CloudList.CloudEntry entry = new CloudList.CloudEntry()
                            .sKey(key)
                            .sContainer(inContainer)
                            .sSize(component.getSize())
                            .sContentType(component.getMimeType())
                            .sDigest(digest)
                            .sStorageClass(storageClass)
                            .sVersion(v)
                            .sCreated(component.getCreated());
                    cloudList.add(entry);
                    if (true) {
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
        File tFile = FileUtil.getTempFile("tmpman", ".xml");
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
            
        } finally {
            try {
                tFile.delete();
            } catch (Exception ex) { } 
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
            String xml = StringUtil.streamToString(manifestXMLIn, "utf-8");
            System.out.println(xml);
            manifestXMLIn = inService.getManifest(inContainer, objectID);
            return ManifestSAX.buildMap(manifestXMLIn, logger);

        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    
    public static class Test {
        public String val = "val";
    }
}
