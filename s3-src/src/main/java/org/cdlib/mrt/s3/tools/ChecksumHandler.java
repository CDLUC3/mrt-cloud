/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.tools;
import org.cdlib.mrt.s3.tools.*;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.security.MessageDigest;
import java.util.Properties;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.*;
import org.json.JSONObject;

/**
 *
 * @author replic
 */
public class ChecksumHandler {
    
    protected static final String NAME = "ChecksumHandler";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static int RETRY=5;
    protected ArrayList<Digest> digestList = new ArrayList();
    protected byte[] buf = null;
    protected long addBufTime = 0;
    protected long fillBufTime = 0;
    protected long fillBufBytes = 0;
        
    protected static final Logger log4j = LogManager.getLogger();
    
    public static ChecksumHandler getChecksumHandler(String [] types)
        throws TException
    {
        ChecksumHandler checksums = new ChecksumHandler(types);
        //checksums.process();
        return checksums;
    }
    
    public static ArrayList<Digest> getDigests(String [] types)
        throws TException
    {
        ArrayList<Digest> digestList = new ArrayList<>();
        HashMap<String,Digest> dedupHash = new HashMap<>();
        digestList = new ArrayList();
        for (String checksumType : types) {
            Digest digest = new Digest(checksumType);
            String algorithmType = digest.checksumType;
            Digest dedup = dedupHash.get(algorithmType);
            if (dedup != null) continue;
            dedupHash.put(algorithmType, digest);
            digestList.add(digest);
        }
        return digestList;
    }
    
    protected ChecksumHandler(String [] types)
        throws TException
    {
        digestList = getDigests(types);
        for (Digest digest : digestList) {
            if (DEBUG) System.out.println("***ChecksumHandler.digestList:" 
                    + " - digest.checksumType=" + digest.checksumType
                    + " - digest.inChecksumType=" + digest.inChecksumType
            );
        }
    }
    
    
    public void addBuff(byte[] bytes)
    {
        //System.out.println("addBuff - bytes.length:" + bytes.length);
        long addBufStart = System.currentTimeMillis();
        for (Digest digestType : digestList) {
            digestType.algorithm.update(bytes, 0, bytes.length);
        }
        addBufTime += (System.currentTimeMillis() - addBufStart);
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
    
    public void finishDigests()
        throws TException
    {   long finishMs = System.currentTimeMillis();
        for (Digest digest: digestList) {
            finishDigest(digest);
        }
        addBufTime += (System.currentTimeMillis() - finishMs);
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
            if (DEBUG) System.out.println("inDigest(" + inDigest.checksumType + ")=" + inDigest.checksum);
           
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
    }

    public HashMap<String, String> getDigestHashStandard() {
        
        HashMap<String,String> digestHash = new HashMap<>();
        for (Digest digest: digestList) {
            digestHash.put(digest.checksumType, digest.checksum);
            if (DEBUG) System.out.println("---ChecksumHandler.getDigestHashStandard---"
                    + " - checksumType:" + digest.checksumType
                    + " - checksum:" + digest.checksum
            );
        }
        return digestHash;
    }

    public HashMap<String, String> getDigestHashIn() {
        
        HashMap<String,String> digestHash = new HashMap<>();
        for (Digest digest: digestList) {
            digestHash.put(digest.inChecksumType, digest.checksum);
            if (DEBUG) System.out.println("---ChecksumHandler.getDigestHashIn---"
                    + " - inChecksumType:" + digest.inChecksumType
                    + " - checksum:" + digest.checksum
            );
        }
        return digestHash;
    }
    
    public JSONObject getDigestJson() 
    {
        JSONObject resp = new JSONObject();
        for (Digest digest: digestList) {
            resp.put(digest.inChecksumType, digest.checksum);
        }
        return resp;
    }
    
    public void dumpDigests(String header, HashMap<String,String> digests) 
    {
        if (digests != null) {
            Set<String> keys = digests.keySet();
            for (String key : keys) {
                String value = digests.get(key);
                System.out.println(header + "\n"
                        + " - " + key + "=" + value + "\n"
                );
            };
        }
    } 
    
    public Long fillBuff(InputStream inStream, ByteBuffer buffer,  final int buffSize)
       throws TException
    {
     
        try {
            long startTime = System.currentTimeMillis();
            log4j.debug("buffer:" + buffer.limit() + " - buffSize=" + buffSize);
            byte[] bytes = inStream.readNBytes(buffSize);
            long readSize = bytes.length;
            fillBufBytes += readSize;
            //System.out.println("byte.len=" + bytes.length);
            addBuff(bytes);
            buffer.put(bytes);
            fillBufTime += System.currentTimeMillis() - startTime;
            return readSize;
            
        } catch (Exception ex) {
            System.out.println("Ex:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public Long fillBuff(byte[] bytes)
       throws TException
    {
     
        try {
            long startTime = System.currentTimeMillis();
            long readSize = bytes.length;
            fillBufBytes += readSize;
            addBuff(bytes);
            fillBufTime += System.currentTimeMillis() - startTime;
            if (false) System.out.println("ChecksumHandler.fillBuff:"
                    + " - readSize:" + readSize
                    + " - fillBufBytes:" + fillBufBytes
            );
            return readSize;
            
        } catch (Exception ex) {
            System.out.println("Ex:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static Digest getDigest(
            CloudStoreInf service,
            String bucket,
            String key,
            int retry)
        throws TException
    {
        Digest digest = new Digest("sha256");
        digest.bucket = bucket;
        digest.key = key;
        Properties metaProp = null;
        for (int i=0; i < retry; i++) {
            try {
                metaProp = service.getObjectMeta(bucket, key);
            } catch (Exception mex) {
                if (i < (retry-1)) {
                    System.out.println(MESSAGE + "Meta exception retry(" + i + "):"
                        + " - service=" + service.getType()
                        + " - bucket=" + bucket
                        + " - key=" + key
                        + " - Exception=" + mex
                    );
                    try {
                        Thread.sleep(i * 1000);
                    } catch (Exception exs) { }
                    continue;
                }
                digest.setting = Digest.DigestSet.call_error;
                digest.error_message = "Excpetion:" + mex;
                return digest;
            }
        }
        if (metaProp == null) {
            digest.setting = Digest.DigestSet.system_error;
            digest.error_message = "Excpetion: null metaProp";
            return digest;
        }
        if (metaProp.isEmpty()) {
            digest.setting = Digest.DigestSet.not_found;
            return digest;
        }
        if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("response", metaProp));
        String lengthS = metaProp.getProperty("size");
        if (StringUtil.isAllBlank(lengthS)) {
            throw new TException.INVALID_OR_MISSING_PARM("input file length not found"
                    + " - bucket:" + bucket
                    + " - key:" + key
            );
        }
        digest.inputSize = Long.parseLong(lengthS);
        
        digest.checksum =  metaProp.getProperty("sha256");
        if (StringUtil.isAllBlank(digest.checksum)) {
            System.out.println("sha256 property missing using s3 content to generate:"
                    + " - bucket:" + bucket
                    + " - key:" + key
            );
            String sha256 = getSha256(service, bucket, key);
            if (StringUtil.isAllBlank(sha256)) {
                throw new TException.INVALID_OR_MISSING_PARM("input file sha256 not found"
                        + " - bucket:" + bucket
                        + " - key:" + key
                );
            } else {
                digest.checksum = sha256;
                System.out.println("Generated sha256:" + sha256);
            }
        }
        digest.setting = Digest.DigestSet.found;
        return digest;
    }
    
    public static String getSha256(CloudStoreInf service, String bucket, String key)
        throws TException
    {
        try {
    
            String [] types = new String[1];
            types[0] = "sha256";
            CloudChecksum cc = CloudChecksum.getChecksums(types, service, bucket, key);
            cc.process();
            return cc.getChecksum(types[0]);
            
        } catch (TException tex) {
                tex.printStackTrace();
                throw tex;
                
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public long getAddBufTime() {
        return addBufTime;
    }

    public long getFillBufTime() {
        return fillBufTime;
    }

    public long getFillBufBytes() {
        return fillBufBytes;
    }
    
    public static class Digest {
        public enum DigestSet {found, not_found, call_error, system_error, not_set, match};
        public String error_message = null;
        public DigestSet setting = DigestSet.not_set;
        public MessageDigestType type =  null;
        public MessageDigest algorithm = null;
        public String bucket = null;
        public String key = null;
        public String checksum = null;
        public String checksumType = null;
        public String inChecksumType = null;
        public long inputSize = 0;
        public Digest(String inChecksumType)
            throws TException
        {
            try {
                this.inChecksumType = inChecksumType;
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
                    + " - setting:" + setting + "\n"
                    + " - setting:" + error_message + "\n"
                    + " - checksumType:" + checksumType + "\n"
                    + " - checksum:" + checksum + "\n"
                    + " - inputSize:" + inputSize + "\n";
            return out;
        }
        
        
    }
    
    public enum MessageDigestType
    {
        sh3224("SHA3-224"),
        sh3256("SHA3-256"),
        sh3384("SHA3-384"),
        sh3512("SHA3-512"),
        md5("MD5"),
        sha1("SHA-1"),
        sha256("SHA-256"),
        sha384("SHA-384"),
        sha512("SHA-512");

        protected String javaAlgorithm = null;
        MessageDigestType(String javaAlgorithm)
        {
            this.javaAlgorithm = javaAlgorithm;
        }
        public String getJavaAlgorithm() { return javaAlgorithm; }
    }
}
