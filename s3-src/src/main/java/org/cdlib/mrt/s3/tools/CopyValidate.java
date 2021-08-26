/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.tools;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.ManifestStr;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.StringUtil;
/**
 * 
 *
 * @author replic
 */
public class CopyValidate 
{
    protected static final String NAME = "CopyValidate";
    protected static final String MESSAGE = NAME + ": ";
    
    protected VersionMap versionMap1 = null;
    protected VersionMap versionMap2 = null;
    protected Identifier id1 = null;
    protected Identifier id2 = null;
    protected Long node1 = null;
    protected Long node2 = null;
    protected NodeIO nodeIO = null;
    protected String nodePath = null;
    protected LoggerInf logger = null;
    protected NodeIO.AccessNode accessNode1 = null;
    protected NodeIO.AccessNode accessNode2 = null;
    protected HashMap<String, FileComponent> versionHash1 = null;
    protected HashMap<String, FileComponent> versionHash2 = null;
    protected int detailLog = 15;
    protected int perfileLog = 10;
    protected int genLog = 5;
    
    
    public CopyValidate(
            Identifier id1,
            Identifier id2,
            Long node1,
            Long node2,
            NodeIO nodeIO,
            LoggerInf logger)
        throws TException
    {
        this.id1 = id1;
        this.id2 = id2;
        this.node1 = node1;
        this.node2 = node2;
        this.nodeIO = nodeIO;
        this.logger = logger;
        set();
    }
    
    
    public static void main(String[] args) 
            throws IOException,TException 
    {
        try {
            LoggerInf logger = new TFileLogger("lockFile", 10, 10);
            Identifier id1 = new Identifier ("ark:/28722/bk0003f046h");
            Identifier id2 = new Identifier ("ark:/28722/bk0003f046h");
            long node1 = 9502;
            long node2 = 2002;
            NodeIO nodeIO = NodeIO.getNodeIOConfig("yaml:", logger);
            //nodeIO.printNodes("yaml dump");
            CopyValidate copyValidate = new CopyValidate(id1, id2, node1, node2, nodeIO, logger);
            copyValidate.test();
            copyValidate.matchMeta();
            
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
    
    protected void set() 
        throws TException
    {
        accessNode1 = setAccessNode(node1);
        accessNode2 = setAccessNode(node2);
        versionMap1 = getVersionMap(accessNode1, id1);
        versionMap2 = getVersionMap(accessNode2, id2);
        versionHash1 = getVersionHash(versionMap1);
        versionHash2 = getVersionHash(versionMap2);
    }
    
    public void test() 
        throws TException
    {
        validateCurrent();
        validateHash();
    }
    
    protected NodeIO.AccessNode setAccessNode(Long node) 
        throws TException
    {
        try {
               NodeIO.AccessNode an = nodeIO.getAccessNode(node);
               if (an == null) {
                   throw new TException.INVALID_OR_MISSING_PARM("Node not supported:" + node);
               }
               return an;
                
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    
    /**
     * Get version content information from a specific manifext.txt
     * @param versionFile manifest file
     * @return Version file content
     * @throws TException
     */
    protected VersionMap getVersionMap(NodeIO.AccessNode an, Identifier objectID)
            throws TException
    {
        try {
            CloudStoreInf service = an.service;
            String bucket = an.container;
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
    
    protected void validateCurrent()
            throws TException 
    {
        try {
            int current1 = versionMap1.getCurrent();
            int current2 = versionMap2.getCurrent();
            if (current1 != current2) {
                throw new TException.INVALID_DATA_FORMAT("Current match fails:"
                        + " - node1=" + node1
                        + " - id1=" + id1.getValue()
                        + " - current1=" + current1
                        + " - node2=" + node2
                        + " - id2=" + id2.getValue()
                        + " - current2=" + current2
                );
            }
            
            
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
    
    protected void validateHash()
            throws TException 
    {
        try {
            int v12 = matchHash("vm1-vm2", versionMap1.getCurrent(), versionMap1, versionHash2);
            int v21 = matchHash("vm2-vm1", versionMap2.getCurrent(), versionMap2, versionHash1);
            log(genLog, "CopyValidate files present match:" + (v12 + v21));
            
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
            
  
    
    protected int matchHash(
            String testName, int verCnt, VersionMap inMap, HashMap<String, FileComponent> testHash)
        throws TException
    {
        try {
                
            //int verCnt = inMap.getVersionCount();
            int testCnt = 0;
            for (int i=1; i<=verCnt; i++) {
                List<FileComponent> comps = inMap.getVersionComponents(i);
                
                for (FileComponent comp: comps) {
                    String key = comp.getLocalID();
                    String [] parts = key.split("\\|",3);

                    String testKey = "" + i + "=" + parts[1] + "|" + parts[2];
                    FileComponent testComp = testHash.get(testKey);
                    if (testComp == null) {
                        throw new TException.INVALID_DATA_FORMAT(MESSAGE + "matchHash - value not found:"
                                + " - testKey:" + testKey
                                + " - testName:" + testName
                                + " - testComp:" + testComp
                        );
                    }
                    MessageDigest inDigest = comp.getMessageDigest();
                    MessageDigest testDigest = testComp.getMessageDigest();
                   
                    if (!inDigest.getValue().equals(testDigest.getValue())) {
                        throw new TException.INVALID_DATA_FORMAT(MESSAGE + "matchHash - digests do not match"
                                + " - key:" + key
                                + " - inDigest.getValue():" + inDigest.getValue()
                                + " - testDigest.getValue():" + testDigest.getValue()
                        );
                    }
                    testCnt++;
                }
            }
            return testCnt;
            
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
    
    
    public static HashMap<String, FileComponent> getVersionHash(VersionMap versionMap)
         throws TException
    {
        HashMap<String, FileComponent> hash = new HashMap<String, FileComponent>();
        try {
                
            int verCnt = versionMap.getVersionCount();
            for (int i=1; i<=verCnt; i++) {
                List<FileComponent> comps = versionMap.getVersionComponents(i);
                
                for (FileComponent comp: comps) {
                    String key = comp.getLocalID();
                    String [] parts = key.split("\\|",3);
                    String testKey = "" + i + "=" + parts[1] + "|" + parts[2];
                    //System.out.println("getversionHash:" + testKey);
                    hash.put(testKey, comp);
                }
            }
            return hash;
                
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    
    public static HashMap<String, FileComponent> getKeyHash(VersionMap versionMap)
         throws TException
    {
        HashMap<String, FileComponent> hash = new HashMap<String, FileComponent>();
        try {
                
            int verCnt = versionMap.getVersionCount();
            for (int i=1; i<=verCnt; i++) {
                List<FileComponent> comps = versionMap.getVersionComponents(i);
                String match = "|" + i + "|";
                for (FileComponent comp: comps) {
                    String key = comp.getLocalID();
                    hash.put(key, comp);
                }
            }
            return hash;
                
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected boolean matchContent(NodeIO.AccessNode accNode, FileComponent component)
        throws TException
    {
        try {
            CloudStoreInf service = accNode.service;
            String bucket = accNode.container;
            long node = accNode.nodeNumber;
            String key = component.getLocalID();
            log(detailLog, "test"
                        + " - node=" + node
                        + " - key=" + key
            );
            MessageDigest compDigest = component.getMessageDigest();
            String digestValue = compDigest.getValue();
            long size = component.getSize();
            log(detailLog, "test"
                    + " - node=" + node
                    + " - key=" + key
                    + " - size=" + size
                    + " - digestValue=" + digestValue
            );
            String [] types = {
                        "sha256"
                    };
            CloudChecksum ccsum = CloudChecksum.getChecksums(types, service, bucket, key);
            ccsum.process();
            String cchk = ccsum.getChecksum("sha256");
            log(detailLog, "datasum(sha256)=" + cchk);
            CloudChecksum.CloudChecksumResult result = ccsum.validateSizeChecksum(digestValue, "sha256", size, logger);
            if (!result.checksumMatch) {
                String msg = "MatchContent fail"
                        + " - node=" + node
                        + " - key=" + key
                        + result.dump("MatchContent fail");
                log(detailLog, msg);
                throw new TException.INVALID_DATA_FORMAT(
                        "MatchContent fail"
                        + " - node=" + node
                        + " - key=" + key

                                + result.dump("MatchContent fail"));
            }
            return true;
            
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
    
    
    protected boolean matchMeta(NodeIO.AccessNode accNode, FileComponent component)
        throws TException
    {
        try {
            CloudStoreInf service = accNode.service;
            String bucket = accNode.container;
            long node = accNode.nodeNumber;
            String key = component.getLocalID();
            log(detailLog, "test"
                        + " - node=" + node
                        + " - key=" + key
            );
            MessageDigest compDigest = component.getMessageDigest();
            String digestValue = compDigest.getValue();
            long size = component.getSize();
            log(detailLog, "test"
                    + " - node=" + node
                    + " - key=" + key
                    + " - size=" + size
                    + " - digestValue=" + digestValue
            );
            Properties objectMeta = service.getObjectMeta(bucket, key);
            if (objectMeta == null) {
                throw new TException.INVALID_DATA_FORMAT(
                        "matchMeta metadata not found"
                        + " - node=" + node
                        + " - bucket=" + bucket
                        + " - key=" + key);
            }
            String metaSha256= objectMeta.getProperty("sha256");
            if (StringUtil.isAllBlank(metaSha256)) {
                throw new TException.INVALID_DATA_FORMAT(
                        "matchMeta sha256 not supported - is empty"
                        + " - node=" + node
                        + " - bucket=" + bucket
                        + " - key=" + key);
            }
            String sizeS= objectMeta.getProperty("size");
            if (StringUtil.isAllBlank(metaSha256)) {
                throw new TException.INVALID_DATA_FORMAT(
                        "matchMeta size not found - is empty"
                        + " - node=" + node
                        + " - bucket=" + bucket
                        + " - key=" + key);
            }
            long metaSize = Long.parseLong(sizeS);
            if ((size == metaSize) && digestValue.equals(metaSha256)) {
                log(perfileLog, "matchMeta match"
                    + " - node=" + node
                    + " - key=" + key
                    + " - size=" + size
                    + " - digestValue=" + digestValue
                    );
                return true;
            }
            throw new TException.INVALID_DATA_FORMAT(
                    "matchMeta fail:"
                    + " - node=" + node
                    + " - bucket=" + bucket
                    + " - size=" + size
                    + " - metaSize=" + metaSize
                    + " - digestValue=" + digestValue
                    + " - metaSha256=" + metaSha256
            );
            
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
    protected int doMatch(NodeIO.AccessNode accNode, HashMap<String, FileComponent> keyHash)
        throws TException
    {
        try {
            Set<String> keys = keyHash.keySet();
            int testedCnt = 0;
            for (String key : keys) {
                if (StringUtil.isAllBlank(key)) continue;
                FileComponent component = keyHash.get(key);
                matchContent(accNode, component);
                testedCnt++;
            }
            return testedCnt;
            
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
    
    protected int doMatchMeta(NodeIO.AccessNode accNode, HashMap<String, FileComponent> keyHash)
        throws TException
    {
        try {
            Set<String> keys = keyHash.keySet();
            int testedCnt = 0;
            for (String key : keys) {
                if (StringUtil.isAllBlank(key)) continue;
                FileComponent component = keyHash.get(key);
                matchMeta(accNode, component);
                testedCnt++;
            }
            return testedCnt;
            
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
    
    protected boolean match()
        throws TException
    {
        int m1Cnt = 0;
        int m2Cnt = 0;
        try {
            HashMap<String, FileComponent> keyHash1 = getKeyHash(versionMap1);
            m1Cnt = doMatch(accessNode1, keyHash1);
            HashMap<String, FileComponent> keyHash2 = getKeyHash(versionMap2);
            m2Cnt = doMatch(accessNode2, keyHash2);
            log(genLog, "CopyValidate matchCnt=" + (m1Cnt + m2Cnt));
            return true;
            
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
    
    protected boolean matchMeta()
        throws TException
    {
        int m1Cnt = 0;
        int m2Cnt = 0;
        try {
            HashMap<String, FileComponent> keyHash1 = getKeyHash(versionMap1);
            m1Cnt = doMatchMeta(accessNode1, keyHash1);
            HashMap<String, FileComponent> keyHash2 = getKeyHash(versionMap2);
            m2Cnt = doMatchMeta(accessNode2, keyHash2);
            log(genLog, "CopyValidate matchCnt=" + (m1Cnt + m2Cnt));
            return true;
            
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
    
    public static VersionMap getVersionMap(File mapFile, LoggerInf logger)
        throws TException
    {
        try {
            FileInputStream inStream = new FileInputStream(mapFile);
            VersionMap map = ManifestSAX.buildMap(inStream, logger);
            return map;
            
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

    public void setMatchLog(int matchLog) {
        this.detailLog = matchLog;
    }

    public void setTestLog(int testLog) {
        this.genLog = testLog;
    }
    
    protected void log(int lvl, String msg)
        throws TException
    {
        logger.logMessage(msg, lvl, true);
    }
}
