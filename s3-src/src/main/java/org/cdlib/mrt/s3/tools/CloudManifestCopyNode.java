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
public class CloudManifestCopyNode {
    
    protected static final String NAME = "CloudManifestCopyNode";
    protected static final String MESSAGE = NAME + ": ";
    
    
    protected CloudManifestCopy cmc = null;
    protected String nodeName = null;
    protected long inNode = 0;
    protected long outNode = 0;
    protected NodeIO nodeIO = null;
    protected LoggerInf logger = null;
                    
    public CloudManifestCopyNode(
            String nodeName,
            long inNode,
            long outNode,
            LoggerInf logger)
        throws TException
    {
        this.logger = logger;
        this.nodeName = nodeName;
        this.inNode = inNode;
        this.outNode = outNode;
        this.nodeIO = new NodeIO(nodeName, logger);
        nodeIO.printNodes("CloudManifestCopyNode" + nodeName);
        setServices();
    }
    
    private void setServices() 
        throws TException
    {
        NodeIO.AccessNode inAccessNode = nodeIO.getAccessNode(inNode);
        NodeIO.AccessNode outAccessNode = nodeIO.getAccessNode(outNode);
        System.out.println("setServices:" 
                + " - inAccessNode.container=" + inAccessNode.container
                + " - outAccessNode.container=" + outAccessNode.container
        );
        cmc = new CloudManifestCopy(inAccessNode.service, inAccessNode.container,
            outAccessNode.service, outAccessNode.container, logger);
    }
    
    public CloudResponse copy(CloudList.CloudEntry entry)
        throws TException
    {
        return cmc.copy(entry);
    }
    
    public void copyObject(String ark)
        throws TException
    {
        cmc.copyObject(ark);
    }
    
    public void close() 
    {
        cmc.close();
    }

    public void setShowEntry(boolean showEntry) {
        cmc.setShowEntry(showEntry);
    }
    
    
    public static class Test {
        public String val = "val";
    }
}
