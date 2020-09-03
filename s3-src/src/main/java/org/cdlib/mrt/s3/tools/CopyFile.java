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
import java.util.ArrayList;
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
import org.cdlib.mrt.utility.Checksums;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFrame;

/**
 * Routine used for copying content from one node key to another
 * 
 * Format table:
 * <inNode>,<outNode>,<data sha256>,<key>
 * 
 * dloy
 */
public class CopyFile {
    
    protected static final String NAME = "CopyFile";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean doDeleteOutput = false;
    
    protected NodeIO nio = null;
    protected File getListFile = null;
    protected File tempFile = null;
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
    protected ArrayList<CopyEntry> copyEntries = new ArrayList();
                    
    public CopyFile(
            String nodeConfig,
            File getListFile,
            LoggerInf logger)
        throws TException
    {
         try {
            this.nio = NodeIO.getNodeIOConfig(nodeConfig, logger);
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
        File runFile = new File("/apps/replic/tasks/audit/200804-audit-404/test/copy.txt");
        String nodeConfig = "nodes-stage";
        CopyFile copyFile = new CopyFile(nodeConfig, runFile, logger);
        copyFile.process();
    }
    
    
    public void process()
        throws TException
    {
        String line = null;
        tempFile = FileUtil.getTempFile("temp", ".txt");
        try {
            while ((line = br.readLine()) != null) {
                if (fileCnt > maxout) break;
                if (StringUtil.isAllBlank(line)) continue;
                if (line.startsWith("#")) continue;
                if (line.length() < 4) continue;
                fileCnt++;
                System.out.println("****(" + fileCnt + ")line=" + line);
                CopyEntry copyEntry = processEntry(line);
                
                System.out.println("process status:" + copyEntry.entryStatus.toString());
                if (copyEntry.ex != null) {
                    System.out.println("Exception:" + copyEntry.ex);
                    copyEntry.ex.printStackTrace();
                }
                copyEntries.add(copyEntry);
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
    
    public CopyEntry processEntry(String line)
        throws TException
    {
        //System.out.println("ProcessEntry:" + line);
        CopyEntry copyEntry = new CopyEntry();
        try {
            long start  = System.nanoTime();
            copyEntry = buildCopyEntry(line); 
            copy(copyEntry);
            return copyEntry;
            
        } catch (TException tex) {
            copyEntry.setEx(tex);
            return copyEntry;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public CopyEntry buildCopyEntry(String line)
        throws TException
    {
        CopyEntry copyEntry = null;
        try {
            long start  = System.nanoTime();
            String[] parts = line.split("\\s*\\,\\s*", 4);
            if (parts.length != 4) {
                throw new TException.INVALID_OR_MISSING_PARM("CopyFile line requires <inNode>,<outNode>,<key>:" + line);
            }
            long inNode = getNode(parts[0]);
            long outNode = getNode(parts[1]);
            String manSha256 = parts[2];
            String key = parts[3];
            copyEntry = new CopyEntry(inNode, outNode, manSha256, key);
            copyEntry.setAccess(nio);
            return copyEntry;
            
        } catch (TException tex) {
            System.out.println("buildCopyEntry exception:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public void copy(CopyEntry copyEntry)
        throws TException
    {
        
        try {
            String key = copyEntry.key;
            CloudStoreInf inService = copyEntry.inAccessNode.service;
            String inBucket = copyEntry.inAccessNode.container;
            CloudStoreInf outService = copyEntry.outAccessNode.service;
            String outBucket = copyEntry.outAccessNode.container;
            String manSha256 = copyEntry.manSha256;
            String inSha256 = getSha256(inService, inBucket, key);
            if (StringUtil.isAllBlank(inSha256)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("inSha256 not found for:" 
                        + " - inBucket:" + inBucket
                        + " - key:" + key
                );
            }
            if (!manSha256.equals(inSha256)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("manSha256 does not match input sha256:" 
                        + " - inSha256:" + inSha256
                        + " - manSha256:" + manSha256
                );
            }
            String outSha256 = getSha256(outService, outBucket, key);
            if (StringUtil.isAllBlank(outSha256)) {
                doCopy(copyEntry);
                return;
            }
            if (inSha256.equals(outSha256)) {
                copyEntry.setStatus("matching");
                printMeta(copyEntry.outAccessNode, key);
                if (doDeleteOutput) {
                    doOutputDelete(copyEntry);
                }
                return;
            }
            
            copyEntry.setStatus("mismatch");
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public void doCopy(CopyEntry copyEntry)
        throws TException
    {
        if (false) {
            copyEntry.setStatus("test");
            System.out.println(copyEntry.dump("doCopy test"));
            return;
        }
        try {
            String key = copyEntry.key;
            CloudStoreInf inService = copyEntry.inAccessNode.service;
            String inBucket = copyEntry.inAccessNode.container;
            CloudStoreInf outService = copyEntry.outAccessNode.service;
            String outBucket = copyEntry.outAccessNode.container;
            CloudResponse response = new CloudResponse(inBucket, key);
            inService.getObject(inBucket, key, tempFile, response);
            doEx(response);
            response = outService.putObject(outBucket, key, tempFile);
            doEx(response);
            String outDataSha256 = getDataSha256(outService, outBucket, key);
            if (!copyEntry.manSha256.equals(outDataSha256)) {
                throw new TException.FIXITY_CHECK_FAILS("no match manifest and data Digests:" 
                        + " - manSha256:" + copyEntry.manSha256
                        + " - outDataSha256:" + outDataSha256
                );
            }
            
            copyEntry.setStatus("copied");
            
            System.out.println(copyEntry.dump("doCopy complete"));
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    
    public void doOutputDelete(CopyEntry copyEntry)
        throws TException
    {
        
        try {
            String key = copyEntry.key;
            CloudStoreInf outService = copyEntry.outAccessNode.service;
            String outBucket = copyEntry.outAccessNode.container;
            CloudResponse response = outService.deleteObject(outBucket, key);
            System.out.println(response.dump("***Delete***"));
            
        } catch (TException tex) {
            System.out.println("Delete Exception:" + tex);
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    private void doEx(CloudResponse response) 
        throws TException
    {
        Exception ex = response.getException();
        if (ex != null) {
            if (ex instanceof  TException) {
                throw (TException) ex;
            } else {
                throw new TException(ex);
            }
        }
    }
    
    public String getSha256(CloudStoreInf service, String bucket, String key)
        throws TException
    {
        
        try {
            
            Properties prop = service.getObjectMeta(bucket, key);
            //System.out.println(PropertiesUtil.dumpProperties("getSha256", prop));
            if ((prop == null) || (prop.size() == 0)) {
                return null;
            }
            String dataSha256 = prop.getProperty("sha256");
            if (dataSha256 == null) {
                dataSha256 = getDataSha256(service, bucket, key);
                System.out.println("WARNING calculated:" + dataSha256);
            }
            return dataSha256;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public void printMeta(NodeIO.AccessNode accessNode, String key)
        throws TException
    {
        
        try {
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            Properties prop = service.getObjectMeta(bucket, key);
            System.out.println(PropertiesUtil.dumpProperties("printMeta", prop));
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public String getDataSha256(CloudStoreInf service, String bucket, String key)
        throws TException
    {
        String dataSha256 = null;
        try {

            CloudResponse responseDigest = new CloudResponse(bucket, key);
            InputStream inStream = service.getObject(bucket, key, responseDigest);
            String [] types = new String[1];
            types[0] = "sha256";
            Checksums checksums = Checksums.getChecksums(types, inStream);
            dataSha256 = checksums.getChecksum("sha256");
            
            return dataSha256;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public long getNode(String nodeS)
        throws TException
    {
        try {
            if (StringUtil.isAllBlank(nodeS)) {
                throw new TException.INVALID_OR_MISSING_PARM("getNode value missing");
            }
            return Long.parseLong(nodeS);
            
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM("getNode value invalid: "+ nodeS);
        }
    }
    
    
    
    public static class CopyEntry {
    
        public enum EntryStatus {copied, error, matching, mismatch, test, empty};
        
        public EntryStatus entryStatus = EntryStatus.empty;
        public Exception ex = null;
        public Long inNode = null;
        public Long outNode = null;
        public String key = null;
        public String manSha256 = null;
        public NodeIO.AccessNode inAccessNode = null;
        public NodeIO.AccessNode outAccessNode = null;
        public CopyEntry() { }
        public CopyEntry(Long inNode, Long outNode, String manSha256, String key)
        {
            this.inNode = inNode;
            this.outNode = outNode;
            this.manSha256 = manSha256;
            this.key = key;
            //System.out.println(dump("CopyEntry"));
        }

    
        public void setAccess(NodeIO nio)
            throws TException
        {
            try {
                inAccessNode = nio.getAccessNode(inNode);
                if (inAccessNode == null) {
                    throw new TException.INVALID_OR_MISSING_PARM("inAccessNode fail:"
                        + " - inNode=" + inNode
                    );
                }
                    
                outAccessNode = nio.getAccessNode(outNode);
                if (outAccessNode == null) {
                    throw new TException.INVALID_OR_MISSING_PARM("outAccessNode fail:"
                        + " - outAccessNode=" + outAccessNode
                    );
                }

            } catch (TException tex) {
                throw tex;
                
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM("AccessNode fail:"
                        + " - inNode=" + inNode
                        + " - outNode=" + outNode
                );
            }
        }
        
        public String dump(String header)
        {
            String exprt = "";
            if (ex != null) {
                exprt = " - ex:" + ex.toString();
            }
            String out = header
                    + " - status:" + entryStatus.toString()
                    + " - inNode:" + inNode
                    + " - outNode:" + outNode
                    + " - manSha256:" + manSha256
                    + " - key:" + key
                    + exprt ;
            return out;
        
        }
        
        public void setEx(Exception ex) 
        {
            this.ex = ex;
            setStatus("error");
        }
        
        
        public NodeIO.AccessNode getInAccessNode() {
            return inAccessNode;
        }

        public void setInAccessNode(NodeIO.AccessNode inAccessNode) {
            this.inAccessNode = inAccessNode;
        }

        public NodeIO.AccessNode getOutAccessNode() {
            return outAccessNode;
        }

        public void setOutAccessNode(NodeIO.AccessNode outAccessNode) {
            this.outAccessNode = outAccessNode;
        }

        public EntryStatus getEntryStatus() {
            return entryStatus;
        }

        public void setStatus(String entryStatusS) {
            this.entryStatus = EntryStatus.valueOf(entryStatusS);
        }
        
        public void setEntryStatus(EntryStatus entryStatus) {
            this.entryStatus = entryStatus;
        }
    }
            
}
