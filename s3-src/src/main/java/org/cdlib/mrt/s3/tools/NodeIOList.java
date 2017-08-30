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
import java.net.URLDecoder;
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
import org.cdlib.mrt.s3.service.NodeIO;
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
public class NodeIOList {
    
    protected static final String NAME = "CloudObjectDeleteList";
    protected static final String MESSAGE = NAME + ": ";
    
    protected NodeIO nio = null;
    protected File getListFile = null;
    protected BufferedReader br = null;
    protected LoggerInf logger = null;
    protected String base = null;
    protected int maxout = 10000000;
    protected int fileCnt=0;
    protected int getCnt=0;
    protected long timeNode = 0;
    protected long lengthNode = 0;
    protected long timeIO = 0;
    protected long lengthIO = 0;
    protected long timeStore = 0;
    protected long lengthStore = 0;
                    
    public NodeIOList(
            String nodeName,
            String base, 
            File getListFile,
            LoggerInf logger)
        throws TException
    {
         try {
            this.base = base;
            this.nio = new NodeIO(nodeName, logger);
            this.getListFile = getListFile;
            this.logger = logger;
            
            FileInputStream fis = new FileInputStream(this.getListFile);
            br = new BufferedReader(new InputStreamReader(fis,
                    Charset.forName("UTF-8")));
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
    }
    
    public static void main(String[] args) throws Exception {

        LoggerInf logger = new TFileLogger("lockFile", 10, 10);
        File urlFile = new File("/apps/replic/test/audit-nostore/urls.txt");
        //File urlFile = new File("/apps/replic/test/audit-nostore/bad1.txt");
        NodeIOList list = new NodeIOList("nodes-dev", "http://store-AWS-dev.cdlib.org:35121", urlFile, logger);
        list.process();
    }
    
    
    public void process()
        throws TException
    {
        String line = null;
        File tempFile = FileUtil.getTempFile("temp", ".txt");
        try {
            while ((line = br.readLine()) != null) {
                if (fileCnt > maxout) break;
                if (StringUtil.isAllBlank(line)) continue;
                if (line.startsWith("#")) continue;
                if (line.length() < 4) continue;
                fileCnt++;
                System.out.println("****(" + fileCnt + ")line=" + line);
                processGet(line, tempFile);
                processGetIO(line, tempFile);
                processGetStore(line, tempFile);
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        } finally {
             try {
                 br.close();
             } catch (Exception ex) { }
             tempFile.delete();
        }
        
    }
    
    public void processGet(String urlS, File outFile)
        throws TException
    {
        try {
            long start  = System.nanoTime();
            nio.getFile(urlS, outFile);
            long rectime = (System.nanoTime()-start);
            timeNode +=  rectime;
            lengthNode += outFile.length();
            double recperbyte = rectime/outFile.length();
            double totperbyte = timeNode/lengthNode;
            System.out.println("---GET(" + fileCnt + "):"
                    + " - reclength:" + outFile.length()
                    + " - recperbyte:" + recperbyte
                    + " - tottime:" + timeNode
                    + " - totlength:" + lengthNode
                    + " - totperbyte:" + totperbyte
            );
        } catch (Exception ex) {
            System.out.println(">>>processGet: Exception: "+ ex);
            ex.printStackTrace();
        }
    }
    
    public void processGetIO(String urlS, File outFile)
        throws TException
    {
        try {
            long start  = System.nanoTime();
            InputStream io = nio.getInputStream(urlS);
            FileUtil.stream2File(io, outFile);
            long rectime = (System.nanoTime()-start);
            timeIO +=  rectime;
            lengthIO += outFile.length();
            double recperbyte = rectime/outFile.length();
            double totperbyte = timeIO/lengthIO;
            System.out.println("---GETIO(" + fileCnt + "):"
                    + " - reclength:" + outFile.length()
                    + " - recperbyte:" + recperbyte
                    + " - tottime:" + timeIO
                    + " - totlength:" + lengthIO
                    + " - totperbyte:" + totperbyte
            );
        } catch (Exception ex) {
            System.out.println(">>>processGet: Exception: "+ ex);
            ex.printStackTrace();
        }
    }
    
    
    
    public void processGetStore(String urlS, File outFile)
        throws TException
    {
        try {
            long start  = System.nanoTime();
            getStoreFile(urlS, outFile);
            long rectime = (System.nanoTime()-start);
            timeStore +=  rectime;
            lengthStore += outFile.length();
            double recperbyte = rectime/outFile.length();
            double totperbyte = timeStore/lengthStore;
            System.out.println("---STORE(" + fileCnt + "):"
                    + " - reclength:" + outFile.length()
                    + " - recperbyte:" + recperbyte
                    + " - tottime:" + timeStore
                    + " - totlength:" + lengthStore
                    + " - totperbyte:" + totperbyte
            );
        } catch (Exception ex) {
            System.out.println(">>>processGetStore: Exception: "+ ex);
        }
    }
    
    public void getStoreFile(String storageURLS, File outFile)
        throws TException
    { 
        try {
            if (StringUtil.isAllBlank(storageURLS)) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "getAccessKey - storageURL missing");
            }
            int pos = storageURLS.indexOf("/content/");
            if (pos < 0) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "getAccessKey - content not found as part of path:" 
                        + storageURLS);
            }
            String path = storageURLS.substring(pos);
            String retURLS = base + path;
            //System.out.println("retURLS:" + retURLS);
            FileUtil.url2File(logger, retURLS, outFile);
            
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
}
