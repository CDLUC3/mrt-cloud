package org.cdlib.mrt.s3.tools;

/*
 * Copyright 2010-2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A delete of the License is located at
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
import java.nio.charset.Charset;
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
import static org.cdlib.mrt.s3.tools.ExtractUnique.MESSAGE;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFrame;

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
public class CloudObjectDeleteList {
    
    protected static final String NAME = "CloudObjectDeleteList";
    protected static final String MESSAGE = NAME + ": ";
    
    protected CloudObjectDelete cod = null;
    protected File deleteFile = null;
    protected BufferedReader br = null;
    protected LoggerInf logger = null;
    protected int maxout = 10000000;
    protected int fileCnt=0;
    protected int deleteCnt=0;
                    
    public CloudObjectDeleteList(
            CloudStoreInf service,
            String container,
            File deleteFile,
            LoggerInf logger)
        throws TException
    {
         try {
            this.cod = new CloudObjectDelete(service, container, false, logger);
            this.deleteFile = deleteFile;
            this.logger = logger;
            
            FileInputStream fis = new FileInputStream(this.deleteFile);
            br = new BufferedReader(new InputStreamReader(fis,
                    Charset.forName("UTF-8")));
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
    }  
    
    public static void main(String[] args) 
            throws TException 
    {
        
        System.out.println("Run main1");
        TFrame tFrame = null;
        try {
            String propertyList[] = {
                "resources/CloudDelete.properties"};
            tFrame = new TFrame(propertyList, "UniqueKeys");
            Properties runProp = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("lockFile", 10, 10);
            String container = runProp.getProperty("container");
            if (container == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing container");
            }
            String deleteFileS = runProp.getProperty("deleteFile");
            if (deleteFileS == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing deleteFile");
            }
            File deleteFile = new File(deleteFileS);
            
            CloudStoreInf service = OpenstackCloud.getOpenstackCloud(runProp, logger);
            CloudObjectDeleteList codl  = new CloudObjectDeleteList(service, container, deleteFile, logger);
            codl.process();
            
                    
            
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
                if (line.length() < 4) continue;
                processDelete(line);
                fileCnt++;
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        } finally {
             try {
                 br.close();
             } catch (Exception ex) { }
        }
        
    }
    
    public void processDelete(String arkS)
        throws TException
    {
        System.out.println(
                "*********************************\n"
                + "Start processs: ark:" + arkS + "\n"
                + "*********************************\n"
        );
        int processCnt = cod.deleteObject(arkS);
        deleteCnt += processCnt;
        System.out.println(
                "*********************************\n"
                + "Items Processed:" + processCnt + "\n"
                + "Current Deleted:" + deleteCnt + "\n"
                + "*********************************\n"
        );
    }
}
