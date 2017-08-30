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
abstract class ComponentListDirectory {
    
    protected static final String NAME = "CloudManifestCopy";
    protected static final String MESSAGE = NAME + ": ";
    
    protected int version = 0;
    protected CloudStoreInf inService = null;
    protected String inContainer = null;
    protected File buildDir = null;
    protected LoggerInf logger = null;
                    
    public ComponentListDirectory(
            CloudStoreInf inService,
            String inContainer,
            File outDir,
            LoggerInf logger)
        throws TException
    {
        this.inService = inService;
        this.inContainer = inContainer;
        this.buildDir = outDir;
        this.logger = logger;
        if (!outDir.exists()) {
            boolean built = outDir.mkdirs();
            if (!built) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "unable to build directory for:" + outDir);
            }
        }
    }
    
    public void buildDir(CloudList cloudList)
        throws TException
    {
        
        try {
            List<CloudList.CloudEntry> list = cloudList.getList();
            for (CloudList.CloudEntry entry : list) {
                addEntry(entry);
            }
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected void addEntry(CloudList.CloudEntry entry)
        throws TException
    {
        
        try {
            File outFile = getOutFile(entry.getKey());
            if (outFile == null) return;
            copy(entry, outFile);
            
        } catch (TException me) {
            throw me;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected void copy(CloudList.CloudEntry entry, File outFile)
        throws TException
    {
        
        try {
            CloudResponse response = new CloudResponse();
            System.out.println("copy - "
                    + " - inContainer:" + inContainer
                    + " - outFile:" + outFile.getAbsolutePath()
            );
            inService.getObject(inContainer, entry.getKey(), outFile, response);
            Exception ex = response.getException();
            if (ex != null) {
                if (ex instanceof TException) {
                    throw (TException)ex;
                }
                throw new TException(ex);
            }
            
        } catch (TException me) {
            me.printStackTrace();
            throw me;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    abstract protected File getOutFile(String key)
        throws TException;
    
    protected String fileFilter(String fileName)
        throws TException
    {
        
        return fileName;
    }
    
    public static class Test {
        public String val = "val";
    }
}
