/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3v2.tools;

/**
 *
 * @author loy
 */
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import org.cdlib.mrt.s3v2.action.*;
import org.cdlib.mrt.s3.staging.service.RetrieveResponse;
import software.amazon.awssdk.core.sync.RequestBody;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import java.nio.ByteBuffer;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.tools.ChecksumHandler;
import org.cdlib.mrt.s3.tools.CloudChecksum;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
import org.cdlib.mrt.s3.tools.S3Reader;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class S3ToS3 {
    protected static final String NAME = "S3ToS3";
    protected static final String MESSAGE = NAME + ": ";
    
    final static boolean DEBUG = false;
    
    protected Long fromNode = null;
    protected Long toNode = null;
    protected AWSS3V2Cloud fromService = null;
    protected String fromBucket = null;
    protected String fromKey = null;
    protected AWSS3V2Cloud toService = null;
    protected String toBucket = null;
    protected String toKey = null;
    protected long fromMetaObjectSize = 0;
    protected String fromMetaSha256 = null;
    protected S3Reader s3Reader = null;
    protected ReadToS3 readToS3 = null;
    protected S3ToS3Status runStatus = null;
    
    protected static String [] digestTypesS = {"sha256"};
    //protected String keyName = null;
    //protected String [] digestTypesS = null;
    // see https://www.baeldung.com/aws-s3-multipart-upload
    
    protected static final Logger log4j = LogManager.getLogger();   
    public static S3ToS3 getS3ToS3(String jarBase, long fromNode, String fromKey, long toNode, String toKey, Integer maxBufSize) 
        throws TException
    {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        if (DEBUG) System.out.println("\n***mainTest***\n"
                + " - fromNode=" + fromNode + "\n"
                + " - fromKey=" + fromKey + "\n"
                + " - toNode=" + toNode + "\n"
                + " - toKey=" + toKey + "\n"
                + " - maxBufSize=" + maxBufSize + "\n"
        );
        try {
            //String jarBase = "yaml:2";
            //String jarBase = "jar:nodes-remote";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(jarBase, logger) ;
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(fromNode);
            CloudStoreInf service = accessNode.service;
            if (!(service instanceof AWSS3V2Cloud)) {
                throw new TException.INVALID_OR_MISSING_PARM("S3ToS3 fromService not AWSS3V2Cloud");
            }
            AWSS3V2Cloud fromService = (AWSS3V2Cloud)service;
            String fromBucket = accessNode.container;
            
            
            accessNode = nodeIO.getAccessNode(toNode);
            service = accessNode.service;
            if (!(service instanceof AWSS3V2Cloud)) {
                throw new TException.INVALID_OR_MISSING_PARM("S3ToS3 service not AWSS3V2Cloud");
            }
            AWSS3V2Cloud toService = (AWSS3V2Cloud)service;
            String toBucket = accessNode.container;
            return new S3ToS3(fromService, fromBucket, fromKey, toService, toBucket, toKey, maxBufSize);
            

         } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }   
    
    public static S3ToS3 getS3ToS3(long fromNode, String fromKey, long toNode, String toKey, Integer maxBufSize) 
        throws TException
    {
        return getS3ToS3("yaml:2", fromNode, fromKey, toNode, toKey, maxBufSize);
    }
        
    public static S3ToS3 getS3ToS3(
            AWSS3V2Cloud fromService, 
            String fromBucket, 
            String fromKey, 
            AWSS3V2Cloud toService, 
            String toBucket, 
            String toKey, 
            Integer maxBufSize) 
        throws TException
    {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        if (DEBUG) System.out.println("\n***mainTest***\n"
                + " - fromBucket=" + fromBucket + "\n"
                + " - fromKey=" + fromKey + "\n"
                + " - toBucket=" + toBucket + "\n"
                + " - toKey=" + toKey + "\n"
                + " - maxBufSize=" + maxBufSize + "\n"
        );
        try {
            
            return new S3ToS3(fromService, fromBucket, fromKey, toService, toBucket, toKey, maxBufSize);
            

         } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    
    protected S3ToS3(
            AWSS3V2Cloud fromService,
            String fromBucket,
            String fromKey,
            AWSS3V2Cloud toService,
            String toBucket,
            String toKey,
            Integer maxBuffer)
        throws TException
    {
        this.fromService = fromService;
        this.fromBucket = fromBucket;
        this.fromKey = fromKey;
        this.toService = toService;
        this.toBucket = toBucket;
        this.toKey = toKey;
        this.s3Reader = S3Reader.getS3Reader(fromService, fromBucket, fromKey, maxBuffer);
        this.fromMetaObjectSize = this.s3Reader.getMetaObjectSize();
        this.fromMetaSha256 = this.s3Reader.getMetaSha256();
        this.readToS3 = ReadToS3.getReadToS3(toService.getS3Client(), toBucket, toKey, digestTypesS, this.s3Reader);
        runStatus = new S3ToS3Status(this.fromBucket, this.fromKey, this.toBucket, this.toKey);
        if (DEBUG) System.out.println("S3ToS3 constructor:"
                + " - fromService:" + fromService.getS3Type()
                + " - fromBucket:" + this.fromBucket
                + " - fromKey:" + this.fromKey
                + " - toService:" + toService.getS3Type()
                + " - toBucket:" + this.toBucket
                + " - toKey:" + this.toKey
        );
    }
    
    public ChecksumHandler.Digest toExistsDelete()
        throws TException
    {
        try {
            ChecksumHandler.Digest toDigest = ChecksumHandler.getDigest(toService, toBucket, toKey, 5);
            //System.out.println(toDigest.dump("S3ToS3"));
            if (toDigest.setting == ChecksumHandler.Digest.DigestSet.found) {
                deleteTo();
            }
            return toDigest;
            
        } catch (TException tex) {
                tex.printStackTrace();
                throw tex;
                
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    
    public ChecksumHandler.Digest toExistsException()
        throws TException
    {
        try {
            ChecksumHandler.Digest digest = ChecksumHandler.getDigest(toService, toBucket, toKey, 5);
            System.out.println(digest.dump("S3ToS3"));
            if (digest.setting != ChecksumHandler.Digest.DigestSet.not_found) {
                throw new TException.REQUEST_ITEM_EXISTS("S3ToS3: Item already exists"
                    + " - bucket:" + this.toBucket 
                    + " - key:" + this.toKey
                );
            }
            return digest;
            
        } catch (TException tex) {
                tex.printStackTrace();
                throw tex;
                
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    public S3ToS3Status copyOver(boolean audit)
        throws TException
    { 
        runStatus.fromKey = this.fromKey;
        runStatus.fromBucket = this.fromBucket;
        runStatus.toKey = this.toKey;
        runStatus.toBucket = this.toBucket;
        try {
            ChecksumHandler.Digest toDigest = toExistsDelete();
            if (toDigest.setting == ChecksumHandler.Digest.DigestSet.match) return null;
            long s3ToS3StartMS = System.currentTimeMillis();
            RetrieveResponse retrieveResponse = readToS3.doUpload();
            if (DEBUG)retrieveResponse.dump("S3ToS3");
            validateTo(audit,runStatus);
            runStatus.readMs = s3Reader.getReadTimeMs();
            runStatus.metaMs = s3Reader.getMetaTimeMs();
            runStatus.fillMs = retrieveResponse.totalFillMs;
            runStatus.writeMs = retrieveResponse.totalWriteMs;
            runStatus.s3ToS3Ms = (System.currentTimeMillis() - s3ToS3StartMS);
            runStatus.nextCnt = s3Reader.getNextCnt();
            
            if (DEBUG) System.out.println(runStatus.dump("COPYOVER"));
            return runStatus;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
        }
    }

    public S3Reader getS3Reader() {
        return s3Reader;
    }
    
    public static CloudChecksum dataCheck (
        boolean audit,
        String [] types, 
        CloudStoreInf service, 
        String bucket, 
        String key)
    throws TException
    {
        try {
            // CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, key);
            CloudChecksum cloudChecksum = CloudChecksum.getChecksums(types, service, bucket, key, 2000000);
            if (DEBUG) System.out.println("s3 properties:"
                    + " - metaObjectSize=" + cloudChecksum.getMetaObjectSize()
                    + " - metaSha256=" + cloudChecksum.getMetaSha256()
            );
            if (audit) {
                cloudChecksum.process();
                if (DEBUG) cloudChecksum.dump("the test");


                for (String type : types) {
                    String checksum = cloudChecksum.getChecksum(type);
                    if (DEBUG) System.out.println("getChecksum(" + type + "):" + checksum);
                }
            }
            return cloudChecksum;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw  new TException(ex);
        }
    }
    
    public CloudChecksum returnFromCloudChecksum(boolean audit)
        throws TException
    {
        return  dataCheck (
            audit,
            digestTypesS, 
            fromService, 
            fromBucket, 
            fromKey);
    }
    
    public CloudChecksum returnToCloudChecksum(boolean audit)
        throws TException
    {
        return  dataCheck (
            audit,
            digestTypesS, 
            toService, 
            toBucket, 
            toKey);
    }
    
  
    
    public void  validateTo(boolean audit, S3ToS3Status status)
        throws TException
    {
        runStatus.audit = audit;
        Long fromMetaSize = s3Reader.getMetaObjectSize();
        String fromMetaSha256 = s3Reader.getMetaSha256();
        Long toMetaSize = null;
        String toMetaSha256 = null;
        CloudChecksum fromCloudChecksum = null;
        CloudChecksum toCloudChecksum = null;
        try {
            String msg = " - fromBucket:" + fromBucket
                        + " - fromKey:" + fromKey
                        + " - toBucket:" + toBucket
                        + " - toKey:" + toKey;
            
            if (!readToS3.isExec()) {
                throw new TException.INVALID_ARCHITECTURE("S3S3 - Validation requested for non-exec process:"
                        + msg
                );
            }
            if (DEBUG) System.out.println("\n!!!!validate!!!: "
                    + " - audit=" + audit
            );
            
            long startToCloudChecksumMs = System.currentTimeMillis();
            toCloudChecksum = returnToCloudChecksum(audit);
            runStatus.toAuditMs = System.currentTimeMillis() - startToCloudChecksumMs;
            toMetaSize = toCloudChecksum.getMetaObjectSize();
            toMetaSha256 = toCloudChecksum.getMetaSha256();
            
            String msgContent = " - fromMetaSize=" + fromMetaSize
                        + " - toMetaSize=" + toMetaSize
                        + " - fromMetaSha256=" + fromMetaSha256
                        + " - toMetaSha256=" + toMetaSha256;
            
            if ((fromMetaSize == null)
                    || (toMetaSize == null)
                    || (fromMetaSha256 == null)
                    || (toMetaSha256 == null)
                ) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "required value missing:" + msgContent);
            }
            
            if (DEBUG) System.out.println("VALIDATE CONTENT:" + msgContent);

            if (!fromMetaSize.equals(toMetaSize)) {
                throw new TException.FIXITY_CHECK_FAILS(MESSAGE + "fromMetaSize and toMetaSize mismatch:"
                            + " - fromMetaSize:" + fromMetaSize
                            + " - toMetaSize:" + toMetaSize
                            + " - msg:" + msg);
            }
            if (!fromMetaSha256.equals(toMetaSha256)) {
                throw new TException.FIXITY_CHECK_FAILS(MESSAGE + "fromMetaSha256 and toMetaSha256 mismatch:"
                            + " - fromMetaSha256:" + fromMetaSha256
                            + " - toMetaSha256:" + toMetaSha256
                            + " - msg:" + msg);
            }
            if (audit) {
                String auditSha256 = toCloudChecksum.getChecksum("sha256");
                if (auditSha256 == null) {
                    throw new TException.INVALID_OR_MISSING_PARM("sha256 not found");
                }
                if (!auditSha256.equals(fromMetaSha256)) {
                    throw new TException.FIXITY_CHECK_FAILS(MESSAGE + "auditSha256 and fromMetaSha256 mismatch:" 
                            + " - auditSha256:" + auditSha256
                            + " - fromMetaSha256:" + fromMetaSha256
                            + " - msg:" + msg
                    );
                }
                 if (DEBUG) System.out.println("S3ToS3 - audit match:" + auditSha256);
            }
            status.matchSize = toMetaSize;
            status.matchSha256 = toMetaSha256;
             if (DEBUG) System.out.println("Validate - audit:" + audit + " - bucket:" + fromBucket + " - key:" + fromKey + "\n"
                    + " - from size:" + fromMetaSize + " - digest:" + fromMetaSha256 + "\n"
                    + " - to   size:" + toMetaSize + " - digest:" + toMetaSha256 + "\n"
            );
             if (DEBUG) System.out.println("-------------------------------------------------------------------");        
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw  new TException(ex);
        }
        
    }
    public void deleteTo()
        throws TException
    {
        ChecksumHandler.Digest digest = ChecksumHandler.getDigest(toService, toBucket, toKey, 3);
        
        System.out.println("deleteTo status:"
                + " - toBucket:" + toBucket
                + " - toKey:" + toKey
                + " - status:" + digest.setting
        );
        if (digest.setting == ChecksumHandler.Digest.DigestSet.not_found) {
            return;
        }
        CloudResponse response = toService.deleteObject(toBucket, toKey);
        System.out.println("deleteTo:"
                + " - toBucket:" + toBucket
                + " - toKey:" + toKey
        );
    }
    
    public void setExec(boolean exec)
        throws TException
    {
        if (readToS3 == null) {
            throw new TException.INVALID_OR_MISSING_PARM("SetExec - readToS3 null");
        }
        readToS3.setExec(exec);
        System.out.println("set readToS3:" + exec);
    }
    
    public CloudResponse fileLoadCopy(File tmpFile)
        throws TException
    { 
        long startFileLoadMs = System.currentTimeMillis();
        try {
            CloudResponse fromResponse = new CloudResponse(fromBucket,fromKey);
            fromService.getObject(fromBucket, fromKey, tmpFile, fromResponse);
            if (fromResponse.err() || (fromResponse.getException() != null)) {
                System.out.println("from.getObjectFails:" + fromResponse.getException());
                return fromResponse;
            }
            long fromFileMs = System.currentTimeMillis();
            CloudResponse toResponse = toService.putObject(toBucket, toKey, tmpFile);
            
            if (toResponse.err() || (toResponse.getException() != null)) {
                System.out.println("to.getObjectFails:" + toResponse.getException());
                return toResponse;
            }
            long toFileMs = System.currentTimeMillis();
            long runFileMs = System.currentTimeMillis() - startFileLoadMs;
            
            System.out.println("fileLoadCopy:\n"
                    + " - fromFileMs:" + (fromFileMs-startFileLoadMs) + "\n"
                    + " - toFileMs  :" + (toFileMs-fromFileMs) + "\n"
                    + " - runFileMs :" + runFileMs + "\n"
                    + " - fileSize :" + tmpFile.length() + "\n"
            );
            return toResponse;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } finally {
            if (tmpFile.exists()) {
                try {
                    tmpFile.delete();
                } catch (Exception ef) { }
            }
        }
    }
    
    public void  validateFile(boolean audit)
        throws TException
    {
        CloudChecksum fromCloudChecksum = null;
        CloudChecksum toCloudChecksum = null;
        Long fromMetaSize = null;
        Long fromAuditSize = null;
        String fromMetaSha256 = null;
        String fromAuditSha256 = null;
        Long toMetaSize = null;
        Long toAuditSize = null;
        String toMetaSha256 = null;
        String toAuditSha256 = null;
        try {
            System.out.println("\n!!!!validate!!!");
            fromCloudChecksum = returnFromCloudChecksum(false);
            CloudChecksum.Digest fromDigest = fromCloudChecksum.getDigest("sha256");
            CloudChecksum.Digest toDigest = null;
            toCloudChecksum = returnToCloudChecksum(audit);
            toDigest = toCloudChecksum.getDigest("sha256");
                
            System.out.println("\nVALIDATE -------------------------------------------------------------------");
            fromMetaSize = fromCloudChecksum.getMetaObjectSize();
            fromMetaSha256 = fromCloudChecksum.getMetaSha256();
            fromAuditSize = fromDigest.inputSize;
            fromAuditSha256 = fromDigest.checksum;
            System.out.println("FROM - bucket:" + fromBucket + " - key:" + fromKey + "\n"
                    + " - meta  size:" + fromMetaSize + " - digest:" + fromMetaSha256 + "\n"
                    + " - audit size:" + fromAuditSize + " - digest:" + fromAuditSha256 + "\n"
            );
            
                toMetaSize = toCloudChecksum.getMetaObjectSize();
                toMetaSha256 = toCloudChecksum.getMetaSha256();
                toAuditSize = toDigest.inputSize;
                toAuditSha256 = toDigest.checksum;
                
                System.out.println("TO - bucket:" + toBucket + " - key:" + toKey + "\n"
                        + " - meta  size:" + toMetaSize + " - digest:" + toMetaSha256 + "\n"
                        + " - audit size:" + toAuditSize + " - digest:" + toAuditSha256 + "\n"
                );
            
            System.out.println("-------------------------------------------------------------------");        
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw  new TException(ex);
        }
        
    }

    public String getFromMetaSha256() {
        return fromMetaSha256;
    }
    
    public static class S3ToS3Status
    {
        public Boolean audit = null;
        public Long fromNode = null;
        public Long toNode = null;
        public String fromKey = null;
        public String fromBucket = null;
        public String toKey = null;
        public String toBucket = null;
        public Long matchSize = null;
        public String matchSha256 = null;
        public Long readMs = null;
        public Long toAuditMs = null;
        public Long metaMs = null;
        public Long fillMs = null;
        public Long writeMs = null;
        public Long s3ToS3Ms = null;
        public Integer nextCnt = null;
        
        public S3ToS3Status(
            String fromBucket, 
            String fromKey, 
            String toBucket, 
            String toKey) {
            this.fromBucket = fromBucket;
            this.fromKey = fromKey;
            this.toBucket = toBucket;
            this.toKey = toKey;
        }
        
        public String dump(String header)
        {
            
            String statusOut =  header + ":" + matchSize +  "\n"
                    + " - fromBucket:" + fromBucket + "\n"
                    + " - fromKey:" + fromKey + "\n"
                    + " - toBucket:" + toBucket + "\n"
                    + " - toKey:" + toKey + "\n"
                    + " - audit:" + audit + "\n"
                    + " - s3Reader.getReadTime():" + readMs + "\n"
                    + " - s3Reader.getMetaTimeMs:" + metaMs + "\n"
                    + " - readToS3.totalFillMs:" + fillMs + "\n"
                    + " - readToS3.totalWriteMs:" + writeMs + "\n"
                    + " - toAuditMs:" + toAuditMs + "\n"
                    + " - s3ToS3 Time:" + s3ToS3Ms + "\n"
                    + " - nextCnt:" + nextCnt + "\n";
            return statusOut;
        }
    }
}