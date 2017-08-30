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
import java.util.ArrayList;
import java.util.HashMap;
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
public class BuildArchiveDirectory
    extends ComponentListDirectory
{
    
    protected static final String NAME = "BuildArchiveDirectory";
    protected static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    
    protected String storageClass = null;
    protected CloudList versionCloudList = null;
    protected HashMap<String, String> filter = new HashMap();
    public BuildArchiveDirectory(
            String storageClass,
            CloudStoreInf inService,
            String inContainer,
            File outDir,
            Identifier buildArk,
            int buildVersion,
            List<String> filterList,
            LoggerInf logger)
        throws TException
    {
        super(inService, inContainer, outDir, logger);
        buildFilter(filterList);
        this.storageClass = storageClass;
        String arkS = buildArk.getValue();
        ComponentListManifest componentListManifest = new ComponentListManifest(storageClass, inService, inContainer, logger);
        versionCloudList = componentListManifest.buildCloudList(arkS, buildVersion);
    }
    
    private void buildFilter(List<String> filterList)
    {
        if (filter == null) {
            filter = null;
            return;
        }
        for (String key: filterList) {
            filter.put(key, "d");
        }
    }
    
    public void process()
        throws TException 
    {
        buildDir(versionCloudList);
    }
    
    public File getOutFile(String key)
        throws TException
    {
        String PRODUCER = "producer";
        try {
            String parts[] = key.split("\\|");
            if (parts.length < 3) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "bad key:" + key);
            }
            String fileID = parts[2];
            //System.out.println("fileID:" + fileID);
            
            String fileName = null;
            String fileDir = null;
            int pos = fileID.lastIndexOf('/');
            if (pos < 0) return null;
            
            fileName = fileID.substring(pos + 1);
            fileDir = fileID.substring(0, pos);
            if (DEBUG) System.out.println("fileID:" + fileID
                    + " - fileName=" + fileName
                    + " - fileDir=" + fileDir
            );
            if (fileDir.indexOf(PRODUCER) != 0) return null;
            
            fileDir = fileDir.substring(PRODUCER.length());
            fileName = fileFilter(fileName);
            if (fileName == null) return null;
            
            
            if (DEBUG) System.out.println("buildDir:" + buildDir.getAbsolutePath());
            File outDir = new File(buildDir, fileDir);
            if (DEBUG) System.out.println("outDir:" + outDir.getAbsolutePath());
            if (!outDir.exists()) {
                boolean okOutDir = outDir.mkdirs();
                if (!okOutDir) {
                    throw new TException.INVALID_OR_MISSING_PARM(MESSAGE 
                            + "unable to build directory for:" + outDir.getAbsolutePath());
                }
            }
            File retFile = new File(outDir, fileName);
            return retFile;
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public String fileFilter(String fileName)
        throws TException
    {
        if (filter != null) {
            if (DEBUG) System.out.println("hash:"  + fileName + ":" + filter.get(fileName));
            String val = filter.get(fileName);
            if (val == null) return fileName;
            return null;
        }
        if (fileName.indexOf("mrt-") == 0) {
            return null;
        }
        return fileName;
    }
}
