/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.tools;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.security.MessageDigest;
import java.util.Properties;
import org.cdlib.mrt.utility.*;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import static org.cdlib.mrt.utility.MessageDigestValue.getAlgorithm;
/**
 *
 * @author replic
 */
public class CloudChecksum {
    
    protected static final String NAME = "CloudChecksum";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final int BUFSIZE = 32000000;
    protected ArrayList<Digest> digestList = new ArrayList();
    protected int segCnt = 0;
    protected long runTime = 0;
    protected long inputSize = 0;
    protected CloudStoreInf service = null;
    protected String bucket = null;
    protected String key = null;
    protected Long metaObjectSize = null;
    protected long physicalObjectSize = 0L;
    protected String metaSha256 = null;
    protected byte[] buf = new byte[BUFSIZE];
    
    public static CloudChecksum getChecksums(String [] types, CloudStoreInf service, String bucket, String key)
        throws TException
    {
        CloudChecksum checksums = new CloudChecksum(types, service, bucket, key);
        //checksums.process();
        return checksums;
    }
    
    
    public CloudChecksum(String [] types, CloudStoreInf service, String bucket, String key)
        throws TException
    {
        digestList = new ArrayList();
        for (String checksumType : types) {
            Digest digest = new Digest(checksumType);
            digestList.add(digest);
        }
        this.service = service;
        this.bucket = bucket;
        this.key = key;
        if (DEBUG) System.out.println("CloudChecksum:"
                + " - bucket=" + bucket
                + " - key=" + key
        );
        try {
            Properties metaProp = service.getObjectMeta(bucket, key);
            if ((metaProp == null) || (metaProp.size() == 0)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "object not found:"
                        + " - service=" + service.getType()
                        + " - bucket=" + bucket
                        + " - key=" + key
                );
            }
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("response", metaProp));
            CloudResponse response = new CloudResponse(bucket, key);
            response.setFromProp(metaProp);
            metaObjectSize = response.getStorageSize();
            metaSha256 = response.getSha256();
            
        } catch (TException tex) {
                tex.printStackTrace();
                throw tex;
                
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
	public static void main(String[] args) {
            LoggerInf logger = new TFileLogger("jtest", 50, 50);
            try {
                String [] types = {
                    "md5",
                    "sha256"
                };
                String jarBase = "jar:nodes-stage";
                NodeIO nodeIO = NodeIO.getNodeIOConfig(jarBase, logger) ;
                NodeIO.AccessNode accessNode = nodeIO.getAccessNode(5001);
                CloudStoreInf service = accessNode.service;
                String bucket = accessNode.container;
                String key = "ark:/28722/k23j3911v|1|system/mrt-object-map.ttl";
                String keyBig = "ark:/28722/k23x83k93|1|producer/artiraq.org/static/opencontext/kenantepe/full/Fieldphotos/2005/AreaF/F7L06135T19.JPG";
                String keyReallyBig = "ark:/99999/fk48k8jp86|1|producer/Asha_G.tar.gz";
                CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, keyBig);
                System.out.println("begin:"
                        + " - metaObjectSize=" + cloudChecksum.getMetaObjectSize()
                        + " - metaSha256=" + cloudChecksum.getMetaSha256()
                );
                cloudChecksum.process();
                cloudChecksum.dump("the test");
                
               
                for (String type : types) {
                    String checksum = cloudChecksum.getChecksum(type);
                    System.out.println("getChecksum(" + type + "):" + checksum);
                }

             } catch (TException tex) {
                tex.printStackTrace();
            } catch (Exception ex) {
                ex.printStackTrace();;
            }
    }
        
    public void dump(String header)
    {
        System.out.println(header + "stats:\n" 
                        + " - bucket=" + getBucket() + "\n" 
                        + " - key=" + getKey() + "\n"
                        + " - bufsize=" + BUFSIZE + "\n"
                        + " - metaObjectSize=" + getMetaObjectSize() + "\n"
                        + " - segCnt=" + getSegCnt() + "\n"
                        + " - runTime=" + getRunTime() + "\n"
                );
    }
        
    public void process()
        throws TException
    {
        try {
            long startTime = System.currentTimeMillis();
            inputSize = 0;
            int addLen;
            long start = 0;
            long stop = BUFSIZE - 1;
            complete:
            while (true) {
                addLen = add(start, stop);
                if (addLen <= 0) break;
                inputSize += addLen;
                segCnt += 1;
                if (inputSize >= metaObjectSize) break;
                start += BUFSIZE;
                stop += BUFSIZE;
            }
            
            long stopTime = System.currentTimeMillis();
            runTime = stopTime-startTime;
                
            for (Digest digest: digestList) {
                finishDigest(digest);
                digest.inputSize = inputSize;
            } 
            
        } catch (TException tex) {
            throw tex;
           
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
    }
        
    private int add(long start, long stop)
        throws Exception
    {
        
        InputStream inputStream = null;
        int inputSize = 0;
        try {
            CloudResponse streamResponse = new CloudResponse(bucket, key);
            int len;
            inputStream = service.getRangeStream(bucket, key, start, stop, streamResponse);
            if ((inputStream == null) || (streamResponse.getException() != null)) {
                throw streamResponse.getException();
            }
            while ((len = inputStream.read(buf)) >= 0) {
                inputSize += len;
                for (Digest digest: digestList) {
                    digest.algorithm.update(buf, 0, len);
                }
            }
            return inputSize;
            
        } catch (TException tex) {
            throw tex;
           
        } catch (Exception ex) {
            throw new TException(ex);
            
        } finally {
            try {
                inputStream.close();
            } catch (Exception ex) {}
        }
    }
    
    public String getChecksum(String checksumType)
        throws TException
    {
        MessageDigestType mdt = getAlgorithm(checksumType);
        if (mdt == null) {
            throw new TException.INVALID_OR_MISSING_PARM("Digest type not found:" + checksumType);
        }
        for (Digest digest: digestList) {
            if (digest.type == mdt) return digest.checksum;
        }
        return null;
    }
    
    public Digest getDigest(String checksumType)
        throws TException
    {
        MessageDigestType mdt = getAlgorithm(checksumType);
        if (mdt == null) {
            throw new TException.INVALID_OR_MISSING_PARM("Digest type not found:" + checksumType);
        }
        for (Digest digest: digestList) {
            if (digest.type == mdt) return digest;
        }
        return null;
    }
    
    public static void finishDigest(Digest inDigest)
        throws TException
    {
        try {
            
            byte[] digest = inDigest.algorithm.digest();
            StringBuffer hexString1 = new StringBuffer();
            for (int i=0;i<digest.length;i++) {
                String val = Integer.toHexString(0xFF & digest[i]);
                if (val.length() == 1) val = "0" + val;
                hexString1.append(val);
            }
            inDigest.checksum = hexString1.toString();
           
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
    }

    public Long getMetaObjectSize() {
        return metaObjectSize;
    }

    public String getMetaSha256() {
        return metaSha256;
    }

    public int getSegCnt() {
        return segCnt;
    }

    public long getRunTime() {
        return runTime;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }
     
    
    
    public static MessageDigestType getAlgorithm(String algorithmS)
    {
        if (StringUtil.isEmpty(algorithmS)) {
            return null;
        }
        algorithmS = algorithmS.toLowerCase();
        algorithmS = StringUtil.strip(algorithmS, "-");
        try {
            return MessageDigestType.valueOf(algorithmS);
        } catch (Exception ex) {

        }
        return null;
    }

    public ArrayList<Digest> getDigestList() {
        return digestList;
    }

    public long getInputSize() {
        return inputSize;
    }
    
    
    /**
     * Get the download size and checksum for this file
     */
    public CloudChecksumResult validateSizeChecksum(
            String passedChecksum,
            String passedChecksumTypeS,
            long passedFileSize,
            LoggerInf logger)
        throws TException
    {
        Digest matchDigest = null;
        if (logger.getMessageMaxLevel() >= 10) {
            String msg = MESSAGE + "validateSizeChecksum entered:"
                    + " inputFileSize=" + inputSize 
                    + " - passedFileSize=" + passedFileSize
                    + " - passedChecksumType=" + passedChecksumTypeS
                    + " - passedChecksum=" + passedChecksum;
            logger.logMessage(msg ,5, true);
        }
        if (StringUtil.isEmpty(passedChecksum)) {
            throw new TException.INVALID_OR_MISSING_PARM("passedChecksum missing");
        }
        if (StringUtil.isEmpty(passedChecksumTypeS)) {
            throw new TException.INVALID_OR_MISSING_PARM("passedChecksumType missing");
        }
        MessageDigestType passedChecksumType = getAlgorithm(passedChecksumTypeS);
        for (Digest digest : digestList) {
            if (digest.type == passedChecksumType) {
                matchDigest = digest;
                break;
            }
        }
        if (matchDigest == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                MESSAGE + "validateSizeChecksum - checksum types do not match:"
                    + " - passed passedChecksumType=" + passedChecksumType );
        }
        CloudChecksumResult result = new CloudChecksumResult();
        if (passedFileSize == metaObjectSize) {
            result.fileSizeMatch = true;
        }
        if (StringUtil.isNotEmpty(passedChecksum) 
            && StringUtil.isNotEmpty(matchDigest.checksum)) {
            
            if (passedChecksum.equals(matchDigest.checksum)) {
                result.checksumMatch = true;
            }
        }
        if (logger.getMessageMaxLevel() >= 10) {
            String msg = MESSAGE + "validateSizeChecksum result:"
                    + " result.fileSizeMatch=" + result.fileSizeMatch
                    + " result.checksumMatch=" + result.checksumMatch;
            logger.logMessage(msg ,0, true);
            //System.out.println(msg);
        }
        return result;
    }

    
    public static class Digest {
        public MessageDigestType type =  null;
        public MessageDigest algorithm = null;
        public String checksum = null;
        public String checksumType = null;
        public long inputSize = 0;
        public Digest(String inChecksumType)
            throws TException
        {
            try {
                type = getAlgorithm(inChecksumType);
                if (type == null) {
                    throw new TException.INVALID_OR_MISSING_PARM("Digest type not found:" + inChecksumType);
                }
                this.checksumType = type.getJavaAlgorithm();
                algorithm = MessageDigest.getInstance(checksumType);
                algorithm.reset();
                
            } catch (Exception ex) {
                throw new TException (ex);
            }
        }
        public String dump(String header) {
            String out = header + "\n"
                    + " - checksumType:" + checksumType + "\n"
                    + " - checksum:" + checksum + "\n"
                    + " - inputSize:" + inputSize + "\n";
            return out;
        }
        
        
    }

    /**
     * Result of fixity match
     */
    public class CloudChecksumResult
    {
        public boolean fileSizeMatch = false;
        public boolean checksumMatch = false;
        
    
        /**
         * Dump the content of this object to a string for logging
         * @param header header displayed in log entry
         */
        public String dump(String header)
        {
            StringBuffer buf = new StringBuffer(1000);
            buf.append(header + " [");

            buf.append(" - fileSizeMatch:" + fileSizeMatch);            
            buf.append(" - checksum:" + checksumMatch);
            buf.append("]");
            return buf.toString();
        }
    }
}
