/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3.staging.service;


/**
 *
 * @author loy
 */
import org.cdlib.mrt.s3.service.CloudResponse;
import java.util.HashMap;
import java.util.Set;
import org.cdlib.mrt.core.MessageDigest;
import org.json.JSONObject;
import org.cdlib.mrt.utility.MessageDigestType;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.s3.staging.tools.ChecksumHandler;

/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

    
public class RetrieveResponse {
    public enum ProcessStatus  { unknown, ok, fail, missing; }
    public ProcessStatus processStatus = ProcessStatus.unknown;
    protected Exception ex = null;
    public String key = "";
    public long totalReadBytes = 0;
    public long totalWriteBytes = 0;

    public long startMs = 0;
    public long endMs = 0;
    public long startIS = 0;
    public long beginIS = 0;
    public long completeMultiPartAdd = 0;
    public long totalFillMs = 0;
    public Long checksumMs = null;
    protected ChecksumHandler checksumHandler = null;
    public HashMap<String,String> digestHash = null;
    public HashMap<String,String> digestHashStandard = null;
    
    public RetrieveResponse() {
    }
    public RetrieveResponse(String key) {
        this.key = key;
    }
    
    public RetrieveResponse(ChecksumHandler checksumHandler) {
        this.checksumHandler = checksumHandler;
    }
    
    public RetrieveResponse(String key,
            ChecksumHandler checksumHandler) {
        this.key = key;
        this.checksumHandler = checksumHandler;
    }
    
    public long totalReadBytes() {
        return totalReadBytes;
    }

    public long totalWriteBytes() {
        return totalWriteBytes;
    }

    public long totalFillMs() {
        return totalFillMs;
    }

    public long totalTimeMs() {
        return endMs - startMs;
    }

    public long totalTimeMsAdj() {
        return (endMs - startMs) - (beginIS - startIS);
    }

    public long totalWriteMs() {
        return (endMs - startMs) - (beginIS - startIS) - totalFillMs;
    }

    public long toStreamMs() {
        return beginIS - startIS;
    }

    public double bytesPerMs()  {
        return (double) totalWriteBytes() / totalTimeMs(); 
    }

    public double bytesPerMsAdj()  {
        return (double) totalWriteBytes() / totalTimeMsAdj(); 
    }

    public double bytesPerMsWrite()  {
        return (double) totalWriteBytes() / totalWriteMs(); 
    }

    public void dump(String header) {
        System.out.println("***" + header + "***"
                + "\n - key=" + key
                + "\n - totalWriteBytes=" + totalWriteBytes() 
                + "\n - totalTimeMs=" + totalTimeMs() 
                + "\n - totalFillMs=" + totalFillMs() 
                + "\n - totalTimeMsAdj=" + totalTimeMsAdj() 
                + "\n - toStreamMs=" + toStreamMs() 
                + "\n - bytesPerMs=" + bytesPerMs()  
                + "\n - bytesPerMsAdj=" + bytesPerMsAdj() 
                + "\n - bytesPerMsWrite=" + bytesPerMsWrite() 
        );
    }

    public JSONObject getJson() 
    {

        JSONObject resp = new JSONObject();
        resp.put("key", getKey());
        resp.put("length", length());
        JSONObject digests = checksumHandler.getDigestJson();
        resp.put("digests", digests);
        JSONObject cnts = new JSONObject();
        cnts.put("totalReadBytes", totalReadBytes());
        cnts.put("totalWriteBytes", totalWriteBytes());
        cnts.put("totalTimeMs", totalTimeMs()); 
        cnts.put("totalFillMs", totalFillMs()); 
        cnts.put("totalTimeMsAdj", totalTimeMsAdj()); 
        cnts.put("toStreamMs", toStreamMs()); 
        if (checksumMs != null) {
            cnts.put("checksumMs", checksumMs);
        }
        cnts.put("bytesPerMs", bytesPerMs());  
        cnts.put("bytesPerMsAdj", bytesPerMsAdj());
        cnts.put("bytesPerMsWrite", bytesPerMsWrite());
        resp.put("counts", cnts);
        return resp;
    }
    
    public JSONObject getJsonDelete() 
    {

        JSONObject resp = new JSONObject();
        resp.put("length", length());
        JSONObject digests = checksumHandler.getDigestJson();
        resp.put("digests", digests);
        JSONObject cnts = new JSONObject();
        cnts.put("totalReadBytes", totalReadBytes());
        cnts.put("totalWriteBytes", totalWriteBytes());
        cnts.put("totalTimeMs", totalTimeMs()); 
        cnts.put("totalFillMs", totalFillMs()); 
        cnts.put("totalTimeMsAdj", totalTimeMsAdj()); 
        cnts.put("toStreamMs", toStreamMs()); 
        if (checksumMs != null) {
            cnts.put("checksumMs", checksumMs);
        }
        cnts.put("bytesPerMs", bytesPerMs());  
        cnts.put("bytesPerMsAdj", bytesPerMsAdj());
        cnts.put("bytesPerMsWrite", bytesPerMsWrite());
        resp.put("counts", cnts);
        return resp;
    }

    public long length() {
        return totalWriteBytes;
    }

    public HashMap<String, String> getDigestHash() {
        return digestHash;
    }

    public HashMap<String, String> getDigestHashStandard() {
        return digestHashStandard;
    }

    public void setDigestHash(HashMap<String, String> digestHash) {
        this.digestHash = digestHash;
        if (digestHash == null) {
            System.out.println("digestHash null");
        } else {
            System.out.println("digestHash length=" + this.digestHash.size());
        }
    }

    public void setFromChecksumHandler() {
        if (checksumHandler == null) return;
        this.digestHash = checksumHandler.getDigestHashIn();
        if (digestHash == null) {
            System.out.println("digestHash null");
        } else {
            System.out.println("digestHash length=" + this.digestHash.size());
            System.out.println("+++dh:" + digestHash.get("md5"));
        }
        this.digestHashStandard = checksumHandler.getDigestHashStandard();
        if (digestHashStandard == null) {
            System.out.println("digestHashStandard null");
        } else {
            System.out.println("digestHashStandard length=" + this.digestHashStandard.size());
            System.out.println("+++dhs:" + digestHashStandard.get("MD5"));
        }
        totalFillMs = checksumHandler.getFillBufTime();
        totalWriteBytes = checksumHandler.getFillBufBytes();
        checksumMs = checksumHandler.getAddBufTime();
    }
    
    public void setFromCloudResponse(CloudResponse cloudResponse)
        throws TException
    {
        setException(cloudResponse.getException());
        String cloudStatus = cloudResponse.getStatus().toString();
        processStatus = ProcessStatus.valueOf(cloudStatus);
    }
    
    public String getSha256() {
        if (digestHashStandard == null) return null;
        return digestHashStandard.get("SHA-256");
    }
    
    public String getMd5() {
        if (digestHashStandard == null) return null;
        return digestHashStandard.get("MD5");
    }
    
    public void setStatusOK() {
        processStatus = ProcessStatus.ok;
    }
    
    public void setStatusFail() {
        processStatus = ProcessStatus.fail;
    }
    
    public void setException(Exception ex) {
        processStatus = ProcessStatus.fail;
        this.ex = ex;
    }

    public Exception getExceotion() {
        return ex;
    }
    
    

    public ChecksumHandler getChecksumHandler() {
        return checksumHandler;
    }

    public void setChecksumHandler(ChecksumHandler checksumHandler) {
        this.checksumHandler = checksumHandler;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
           
    
}