/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.tools;
import org.cdlib.mrt.s3.tools.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.security.MessageDigest;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.utility.*;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.tools.ChecksumHandler.Digest;
import static org.cdlib.mrt.utility.MessageDigestValue.getAlgorithm;
/**
 *
 * @author replic
 */
public class S3Reader {
    
    protected static final Logger log4j = LogManager.getLogger();
    protected static final String NAME = "CloudChecksum";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static int RETRY=5;
    protected int readBufSize = 0;
    protected int maxBufSize = 4096 * 50000;  // must be divisiable by 4096 - 204,800,000
    protected int nextCnt = 0;
    protected int segCnt = 0;
    protected long readTimeMs = 0;
    protected long inputSize = 0;
    protected CloudStoreInf service = null;
    protected String bucket = null;
    protected String key = null;
    protected long metaObjectSize = 0;
    protected long physicalObjectSize = 0L;
    protected String metaSha256 = null;
    protected long metaTimeMs = 0;
    protected byte[] buf = null;
    protected ByteBuffer byteBuf = null;
    
    // read buffer offsets
    protected long start = 0;
    protected long stop = readBufSize - 1;
    
    protected boolean more = false;
    
    public static S3Reader getS3Reader(CloudStoreInf service, String bucket, String key, Integer maxsize)
        throws TException
    {
        S3Reader checksums = new S3Reader(service, bucket, key, maxsize);
        //checksums.process();
        return checksums;
    }
    
    
    protected S3Reader(CloudStoreInf service, String bucket, String key, Integer maxsize)
        throws TException
    {
        if ((maxsize != null) && (maxsize > 10000)) {
            this.maxBufSize = maxsize;
        }
        this.service = service;
        this.bucket = bucket;
        this.key = key;
        if (DEBUG) System.out.println("CloudChecksum:"
                + " - bucket=" + bucket
                + " - key=" + key
        );
        try {
            long startMetaMs = System.currentTimeMillis();
            ChecksumHandler.Digest digest = ChecksumHandler.getDigest(service, bucket, key, 5);
             if (DEBUG) System.out.println(digest.dump("S3Reader"));
            if (digest.setting != Digest.DigestSet.found) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("S3Reader input not found:"
                    + " - bucket:" + this.bucket 
                    + " - key:" + this.key
                );
            }
            metaTimeMs = System.currentTimeMillis() - startMetaMs;
            metaObjectSize = digest.inputSize;
            metaSha256 = digest.checksum;
            log4j.debug("In stats:\n"
                    + " - bucket:" + this.bucket + "\n"
                    + " - key:" + this.key + "\n"
                    + " - metaObjectSize:" + metaObjectSize + "\n"
                    + " - metaSha256:" + metaSha256 + "\n"
            );
            
        } catch (TException tex) {
                tex.printStackTrace();
                throw tex;
                
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public void dump(String header)
    {
        System.out.println(header + "stats:\n" 
                        + " - bucket=" + getBucket() + "\n" 
                        + " - key=" + getKey() + "\n"
                        + " - bufsize=" + readBufSize + "\n"
                        + " - metaObjectSize=" + getMetaObjectSize() + "\n"
                        + " - segCnt=" + getSegCnt() + "\n"
                        + " - runTime=" + getReadTimeMs() + "\n"
                );
    }
    
    public long startRead()
    {
        
        start = 0;
        more = true;
        readBufSize = (int)metaObjectSize;
        if (metaObjectSize > this.maxBufSize) {
            readBufSize = this.maxBufSize;
        }
        buf = new byte[readBufSize];
        stop = buf.length - 1;
        if (DEBUG) System.out.println("***startRead:"
                + " - metaObjectSize="+ metaObjectSize
                + " - maxsize="+ this.maxBufSize
                + " - buffsize="+ readBufSize
                + " - start="+ start
                + " - stop="+ stop
                + " - more="+ more
        );
        return metaObjectSize;
    }
        
    public byte[] nextRead()
        throws TException
    {
        nextCnt++;
        int addLen = 0;
        ByteArrayOutputStream byteArray = null;
        try {
            long startTime = System.currentTimeMillis();
            byteArray = add(start, stop);
            addLen = byteArray.size();
            inputSize += addLen;
            long stopTime = System.currentTimeMillis();
            readTimeMs += stopTime-startTime;
            log4j.debug("***nextRead - "
                    + " - bucket:" + bucket
                    + " - key:" + key
                    + " - start:" + start
                    + " - stop:" + stop
                    + " - addLen:" + addLen
                    + " - inputSize:" + inputSize
                    + " - metaObjectSize:" + metaObjectSize
            );
            byte[] retBytes = byteArray.toByteArray();
            if (inputSize >= metaObjectSize) {
                more = false;
                return retBytes;
            }
            if (addLen <= 0)  {
                more = false;
                return retBytes;
            }
            start += readBufSize;
            stop += readBufSize;
            more = true;
            return retBytes;
            
        } catch (TException tex) {
            throw tex;
           
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
        
    private ByteArrayOutputStream add(long start, long stop)
        throws Exception
    {
        
        InputStream inputStream = null;
        try {
            CloudResponse streamResponse = new CloudResponse(bucket, key);
            int len;
            inputStream = getStream(start, stop, RETRY);
            if ((inputStream == null) || (streamResponse.getException() != null)) {
                throw streamResponse.getException();
            }
            
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            while ((len = inputStream.read(buf)) >= 0) {
                byteArray.write(buf, 0, len);
            }
            
            return byteArray;
            
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
       
    protected InputStream getStream(long start, long stop, int retry)
        throws TException
    {
        
        InputStream inputStream = null;
        CloudResponse streamResponse = null;
        Exception exi = null;
        String errMsg = "";
        
        // build empty input Stream
        if (metaObjectSize == 0) {
            byte[] emptyBuffer = new byte[0];
            InputStream emptyInputStream = new ByteArrayInputStream(emptyBuffer);
            return emptyInputStream;
        }
        
        try {
            for (int i=1; i <= retry; i++) {
                streamResponse = new CloudResponse(bucket, key);
                inputStream = service.getRangeStream(bucket, key, start, stop, streamResponse);
                if ((inputStream != null) && (streamResponse.getException() == null)) {
                    return inputStream;
                }
                exi = streamResponse.getException();
                int sleepMs = (3*i) * 1000;
                errMsg = MESSAGE + "***Data stream fail retry(" + i + "):"
                    + " - service=" + service.getType()
                    + " - bucket=" + bucket
                    + " - key=" + key
                    + " - start=" + start
                    + " - stop=" + stop
                    + " - sleepMs=" + sleepMs
                    + " - Exception=" + exi;
                System.out.println(errMsg);
                if (i==retry) break;
                try {
                    System.out.println(MESSAGE  + "sleep:" + sleepMs);
                    Thread.sleep(sleepMs);
                } catch (Exception exs) { }
            }
            if (exi != null) {
                exi.printStackTrace();
            }
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(MESSAGE + "unable to extract data for:"
                    + " - service=" + service.getType()
                    + " - bucket=" + bucket
                    + " - key=" + key
                    + " - start=" + start
                    + " - stop=" + stop
                    + " - Exception=" + exi
            );
            
        } catch (TException tex) {
                tex.printStackTrace();
                throw tex;
                
        } catch (Exception ex) {
            ex.printStackTrace();
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

    public int getReadBufSize() {
        return readBufSize;
    }

    public long getReadTimeMs() {
        return readTimeMs;
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

    public long getInputSize() {
        return inputSize;
    }
    

    public boolean isMore() {
        return more;
    }

    public int getMaxBufSize() {
        return maxBufSize;
    }

    public long getMetaTimeMs() {
        return metaTimeMs;
    }

    public int getNextCnt() {
        return nextCnt;
    }
}
