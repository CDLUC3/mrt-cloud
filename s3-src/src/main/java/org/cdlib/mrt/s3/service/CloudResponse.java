/*
Copyright (c) 2005-2010, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*********************************************************************/
package org.cdlib.mrt.s3.service;
//import com.amazonaws.services.s3.model.Owner;
//import com.amazonaws.services.s3.model.StorageClass;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.CloudProperties;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.s3.service.CloudUtil;

//import org.jets3t.service.model.StorageObject;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.PropertiesUtil;




/**
 * Cloud response to a cloud storage request
 * @author dloy
 */
public class CloudResponse
{
    private static final boolean DEBUG = false;
    public enum ResponseStatus{ok, fail, missing, dark, unknown};
    public enum ResponseStorageClass{standard, offline, reduced, other, infrequent};
    public enum S3StorageClass{
        Standard, ReducedRedundancy, Glacier, StandardInfrequentAccess, OneZoneInfrequentAccess, 
        IntelligentTiering, DeepArchive, Outposts, GlacierInstantRetrieval, Other};
   
    private int httpStatus = 0;
    private ResponseStatus status = ResponseStatus.ok;
    private CloudList cloudList = new CloudList();
    private Exception exception = null;

    private URL returnURL = null;
    private String bucketName = null;
    private Identifier objectID = null;
    private Integer versionID = null;
    private String fileID = null;
    private String storageKey = null;
    private String errMsg = null;
    private String md5 = null;
    private String sha256 = null;
    private String storageClassString = null;
    private String mimeType = null;
    private long storageSize = 0;
    private boolean match = false;
    private CloudProperties fileMeta = new CloudProperties();
    private ResponseStorageClass storageClassType = null;
    private S3StorageClass storageClassS3 = null;
    //private StorageClass inputStorageClass = null;
    //private StorageClass targetStorageClass = null;
    private ResponseStorageClass inputStorageClassRSC = null;
    private ResponseStorageClass targetStorageClassRSC = null;
    private boolean storageClassConverted = false;
    private Properties metaProp = null;
    private DateState modified = null;
    private Boolean onGoingRestore = null;
    private String glacierRestore = null;
    
    public static CloudResponse get(
            String bucketName,
            Identifier objectID,
            Integer versionID,
            String fileID)
    {
        CloudResponse response = new CloudResponse();
        response.set(bucketName, objectID, versionID, fileID);
        return response;
    }
    
    public static CloudResponse get(
            String bucketName,
            String storageKey)
    {
        CloudResponse response = new CloudResponse(bucketName, storageKey);
        return response;
    }
    
    public static CloudResponse get(
            String bucketName,
            Identifier objectID)
    {
        CloudResponse response = new CloudResponse();
        response.setManifest(bucketName, objectID);
        return response;
    }
    
    public static CloudResponse get(
            Properties metaProp)
    {
        CloudResponse response = new CloudResponse(metaProp);
        return response;
    }
    
    public CloudResponse() {} 
    
    public CloudResponse(
            String bucketName,
            Identifier objectID,
            Integer versionID,
            String fileID)
    {
        set(bucketName, objectID, versionID, fileID);
    }
    
    public CloudResponse(
            String bucketName,
            String storageKey)
    {
        set(bucketName, storageKey);
    }
    
    public CloudResponse(
            Properties metaProp)
    {
        this.metaProp = metaProp;
        setFromProp(metaProp);
    }
    
    public void set(
            String bucketName,
            Identifier objectID,
            Integer versionID,
            String fileID)
    {
        this.bucketName = bucketName;
        this.objectID = objectID;
        this.versionID = versionID;
        this.fileID = fileID;
        this.storageKey = objectID.getValue() + "|" + versionID + "|" + fileID;
    }
    
    public void setManifest(
            String bucketName,
            Identifier objectID)
    {
        this.bucketName = bucketName;
        this.objectID = objectID;
        this.storageKey = objectID.getValue() + "|manifest";
    }
    
    public void set(
            String bucketName,
            String storageKey)
    {
        this.bucketName = bucketName;
        this.storageKey = storageKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public String getFileID() {
        return fileID;
    }

    public void setFileID(String fileID) {
        this.fileID = fileID;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public Identifier getObjectID() {
        return objectID;
    }

    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
    }

    public CloudList getCloudList() {
        return cloudList;
    }

    public void setObjectList(CloudList cloudList) {
        this.cloudList = cloudList;
    }
    
    public void addObject(CloudList.CloudEntry entry) 
    {
        cloudList.add(entry);
        //System.out.println("addObject called:" + cloudList.getList().size());
    }
   
    public ResponseStatus getStatus() {
        return status;
    }

    public void setStatus(ResponseStatus status) {
        this.status = status;
    }

    public String getStorageKey() {
        return storageKey;
    }

    public void setStorageKey(String storageKey) {
        this.storageKey = storageKey;
    }

    public Integer getVersionID() {
        return versionID;
    }

    public void setVersionID(Integer versionID) {
        this.versionID = versionID;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public CloudProperties getFileMeta() {
        return fileMeta;
    }

    public Properties getFileMetaProperties() {
        if ((fileMeta == null) || (fileMeta.size() == 0)) return null;
        return fileMeta.getProperties();
    }

    public void setFileMeta(CloudProperties fileProp)
           
    {
        this.fileMeta = fileProp;
        if (fileProp != null) {
            setFromProp();
        }
    }

    public void addFileMeta(Properties prop) {
        if (prop == null) return;
        
        if (DEBUG) System.out.println("***addFileMeta storageKey1=" + storageKey);
        try {
            Enumeration e = prop.propertyNames();
            String key = null;
            String value = null;

            while( e.hasMoreElements() )
            {
                key = (String)e.nextElement();
                value = prop.getProperty(key);
                String testValue = fileMeta.getProperty(key);
                if ((testValue != null) && !value.equals(testValue))
                    throw new RuntimeException("setFileProp duplicate key exists:" + key
                            + " - value=" + value
                            + " - testValue=" + testValue
                            );
                fileMeta.setProperty(key, value);
                if (DEBUG) System.out.println("**addFileMeta - key=" + key + " - value=" + value);
            }
            
        } catch (Exception ex) {
            throw new RuntimeException("setFilePrope:" + ex);
        }
    }

    public void setFileMeta(Properties prop) {
        if (prop == null) return;
        setFileMetaClear();
        addFileMeta(prop);
    }
    
    public void setFileMetaClear()
    {
        fileMeta.clear();
    }
    
    public int getFileMetaSize()
    {
        return fileMeta.size();
    }
    
    public String getFileMetaProperty(String key)
    {
        if ((fileMeta == null) || (fileMeta.size() == 0)) return null;
        return fileMeta.getProperty(key);
    }
    
    public void setFileMetaProperty(String key, String value)
    {
        fileMeta.setProperty(key, value);
    }

    public boolean err()
    {
        if (status == ResponseStatus.ok) return false;
        return true;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public boolean isMatch() {
        return match;
    }

    public void setMatch(boolean match) {
        this.match = match;
    }

    public long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    public void setStorageSize(String storageSizeS) {
        if (StringUtil.isEmpty(storageSizeS)) {
            this.storageSize = 0;
        } else {
            try {
                this.storageSize = Long.parseLong(storageSizeS);
            } catch (Exception ex) {
                this.storageSize = 0;
            }
        }
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }
    
    public void dumpVar(String header)
    {
        System.out.println(PropertiesUtil.dumpProperties(header, metaProp));
        String objectDisp = (objectID != null) ? objectID.getValue() : "null";
        String modifiedDisp = (modified != null) ? modified.getIsoZDate() : "null";
        System.out.println(header + "\n"
            + " - httpStatus:" + httpStatus + "\n"
            + " - exception:" + exception + "\n"
            + " - returnURL:" + returnURL + "\n"
            + " - bucketName:" + bucketName + "\n"
            + " - objectID:" + objectDisp + "\n"
            + " - versionID:" + versionID + "\n"
            + " - fileID:" + fileID + "\n"
            + " - storageKey:" + storageKey + "\n"
            + " - errMsg:" + errMsg + "\n"
            + " - md5:" + md5 + "\n"
            + " - sha256:" + sha256 + "\n"
            + " - storageClassString:" + storageClassString + "\n"
            + " - mimeType:" + mimeType + "\n"
            + " - storageSize:" + storageSize + "\n"
            + " - match:" + match + "\n"
            + " - storageClassType:" + storageClassType + "\n"
                    
            + " - storageClassS3:" + storageClassS3 + "\n"
            + " - inputStorageClassRSC:" + inputStorageClassRSC + "\n"
            + " - targetStorageClassRSC:" + targetStorageClassRSC + "\n"
            + " - storageClassConverted:" + storageClassConverted + "\n"
            + " - modified:" + modifiedDisp + "\n"
            + " - onGoingRestore:" + onGoingRestore + "\n"
            + " - glacierRestore:" + glacierRestore + "\n"
        );
    }
    
    
    public String dump(String header) {
        StringBuffer buf = new StringBuffer();
        buf.append("CloudResponse **" + header + "**");
        if (objectID == null) {
            buf.append(" - objectID=EMPTY element");
        }
        if (objectID != null) {
            buf.append(" - objectID=" + objectID.toString());
        }
        if (versionID != null) {
            buf.append(" - versionID=" + versionID);
        }
        if (fileID != null) {
            buf.append(" - fileID=" + fileID);
        }
        if (storageKey != null) {
            buf.append(" - storageKey=" + storageKey);
        }
        buf.append(" - status=" + status);
        
        buf.append(fileMeta.dump("\nCloudResponse"));
        
        CloudList cloudList = getCloudList();
        List<CloudList.CloudEntry>list = cloudList.getList();
        if ((list != null) && (list.size() > 0)) {
            buf.append("\nCloudList(" + list.size() + "):\n");
            for (CloudList.CloudEntry entry: list) {
                buf.append(entry.dumpLine() + "\n");
            }
        }
        return buf.toString();
    }
    
    public void setFromProp()
    {
        if (fileMeta == null) return;
        if (getFileMetaProperty("size") != null) setStorageSize(getFileMetaProperty("size"));
        if (getFileMetaProperty("key") != null) setStorageKey(getFileMetaProperty("key"));
        if (getFileMetaProperty("digest") != null) setMd5(getFileMetaProperty("digest"));
        if (getFileMetaProperty("etag") != null) setMd5(getFileMetaProperty("etag"));
        if (getFileMetaProperty("mimetype") != null) setMimeType(getFileMetaProperty("mimetype"));
        if (getFileMetaProperty("sha256") != null) setSha256(getFileMetaProperty("sha256"));
        if (getFileMetaProperty("bucket") != null) setBucketName(getFileMetaProperty("bucket"));
        if (getFileMetaProperty("storageClass") != null) setStorageClassString(getFileMetaProperty("storageClass"));
        String key = getStorageKey();
        if (StringUtil.isNotEmpty(key)) {
            try {
                CloudUtil.KeyElements elements = CloudUtil.getKeyElements(key);
                setObjectID(elements.objectID);
                setVersionID(elements.versionID);
                setFileID(elements.fileID);
            } catch (Exception ex) { }
        }
    }
    
    public void setFromProp(Properties prop)
    {
        this.metaProp = prop;
        if (prop.getProperty("size") != null) setStorageSize(prop.getProperty("size"));
        if (prop.getProperty("key") != null) setStorageKey(prop.getProperty("key"));
        if (prop.getProperty("digest") != null) setMd5(prop.getProperty("digest"));
        if (prop.getProperty("etag") != null) setMd5(prop.getProperty("etag"));
        if (prop.getProperty("mimetype") != null) setMimeType(prop.getProperty("mimetype"));
        if (prop.getProperty("sha256") != null) setSha256(prop.getProperty("sha256"));
        if (prop.getProperty("bucket") != null) setBucketName(prop.getProperty("bucket"));
        String storageClass = prop.getProperty("storageClass");
        if (storageClass == null) {
            storageClass = "Standard";
        }
        setStorageClassString(storageClass);
        storageClassS3 = getS3StorageClass(storageClass);
        storageClassType = getResponseStorageClass(storageClass);
        
        String key = getStorageKey();
        if (StringUtil.isNotEmpty(key)) {
            try {
                CloudUtil.KeyElements elements = CloudUtil.getKeyElements(key);
                setObjectID(elements.objectID);
                setVersionID(elements.versionID);
                setFileID(elements.fileID);
            } catch (Exception ex) { }
        }
        glacierRestore = prop.getProperty("glacierRestore");
        if (glacierRestore != null) {
            String onGoingRestoreS = prop.getProperty("ongoingRestore");
            if (onGoingRestoreS != null) {
                onGoingRestore = Boolean.parseBoolean(onGoingRestoreS);
            }
        }
        String modifiedS = prop.getProperty("modified");
        modified = new DateState(modifiedS);
    }
    
    public boolean setCloudResponse(Properties prop)
    {
        if ((prop == null) || prop.isEmpty()) return false;
        setFromProp(prop);
        return true;
    }
    
    public static CloudList.CloudEntry getCloudEntry(Properties metaProp) 
    {
        if ((metaProp == null) || (metaProp.size() == 0)) {
            return null;
        }
        CloudList.CloudEntry entry = new CloudList.CloudEntry();
        entry.setEtag(metaProp.getProperty("etag"));
        entry.setContainer(metaProp.getProperty("bucket"));
        String sizeS = metaProp.getProperty("size");
        Long size = null;
        if (sizeS != null) {
            try {
                size = Long.parseLong(sizeS);
            } catch (Exception ex) {
                size = null;
            }
        }
        entry.setSize(size);
        entry.setContentType(null);
        entry.lastModified = metaProp.getProperty("modified");
        entry.setStorageClass(metaProp.getProperty("storageClass"));
        entry.setKey(metaProp.getProperty("key"));
        String localSha256S = metaProp.getProperty("sha256");
        if (localSha256S != null) {
            MessageDigest localSha256 = null;
            try {
                localSha256 = new MessageDigest(localSha256S,"sha256");
                entry.setDigest(localSha256);
            } catch (Exception xx) { }
            entry.setDigest(localSha256);
        }
        return entry;
    }

    public String getSha256() {
        return sha256;
    }

    public void setSha256(String sha256) {
        this.sha256 = sha256;
    }

    public String getStorageClassString() {
        return storageClassString;
    }

    public void setStorageClassString(String storageClass) {
        this.storageClassString = storageClass;
    }

    public boolean isStorageClassConverted() {
        return storageClassConverted;
    }

    public void setStorageClassConverted(boolean storageClassConverted) {
        this.storageClassConverted = storageClassConverted;
    }

    public URL getReturnURL() {
        return returnURL;
    }

    public void setReturnURL(URL returnURL) {
        this.returnURL = returnURL;
    }

    public void setStorageClassType(String storageClassString) {
        this.storageClassType = getResponseStorageClass(storageClassString);
    }
    
    public ResponseStorageClass getInputStorageClassRSC() {
        return inputStorageClassRSC;
    }

    public void setInputStorageClassRSC(ResponseStorageClass inputStorageClassRSC) {
        this.inputStorageClassRSC = inputStorageClassRSC;
    }

    public ResponseStorageClass getTargetStorageClassRSC() {
        return targetStorageClassRSC;
    }

    public void setTargetStorageClassRSC(ResponseStorageClass targetStorageClassRSC) {
        this.targetStorageClassRSC = targetStorageClassRSC;
    }

    public void setInputStorageClassRSC(String inputStorageClassS) {
        this.inputStorageClassRSC = getResponseStorageClass(inputStorageClassS);
    }

    public void setTargetStorageClassRSC(String  targetStorageClassS) {
        this.targetStorageClassRSC = getResponseStorageClass(targetStorageClassS);
    }
    
    public static ResponseStorageClass getResponseStorageClass(String stringStorageClass)
    {
        ResponseStorageClass rsc = null;
        if (stringStorageClass == null) return null;
        switch (stringStorageClass) {
            case "Standard":
                rsc=ResponseStorageClass.standard;
                break;
                
            case "STANDARD":
                rsc=ResponseStorageClass.standard;
                break;
                
            case "Glacier":
                rsc=ResponseStorageClass.offline;
                break;
                
            case "GLACIER":
                rsc=ResponseStorageClass.offline;
                break;
                
            case "DeepArchive":
                rsc=ResponseStorageClass.offline;
                break;
                
            case "DEEP_ARCHIVE":
                rsc=ResponseStorageClass.offline;
                break;
                
            case "ReducedRedundancy":
                rsc=ResponseStorageClass.reduced;
                break;
                
            case "REDUCED_REDUNDANCY":
                rsc=ResponseStorageClass.reduced;
                break;
                
            case "StandardInfrequentAccess":
                rsc=ResponseStorageClass.infrequent;
                break;
                
            case "STANDARD_IA":
                rsc=ResponseStorageClass.infrequent;
                break;
                
            case "GlacierInstantRetrieval":
                rsc=ResponseStorageClass.infrequent;
                break;
                
            case "GLACIER_IR":
                rsc=ResponseStorageClass.infrequent;
                break;
                
            case "OneZoneInfrequentAccess":
                rsc=ResponseStorageClass.infrequent;
                break;
                
            case "ONEZONE_IA":
                rsc=ResponseStorageClass.infrequent;
                break;
                
            default:
                rsc=ResponseStorageClass.other;
                break;
        }
        return rsc;
    }
    
    
    
    public static S3StorageClass getS3StorageClass(String stringStorageClass)
    {
        S3StorageClass s3sc = null;
        if (stringStorageClass == null) return null;
        switch (stringStorageClass) {
            case "Standard":
                s3sc = S3StorageClass.Standard;
                break;
                
            case "STANDARD":
                s3sc = S3StorageClass.Standard;
                break;
                
            case "Glacier":
                s3sc = S3StorageClass.Glacier;
                break;
                
            case "GLACIER":
                s3sc = S3StorageClass.Glacier;
                break;
                
            case "DeepArchive":
                s3sc = S3StorageClass.DeepArchive;
                break;
                
            case "DEEP_ARCHIVE":
                s3sc = S3StorageClass.DeepArchive;
                break;
                
            case "ReducedRedundancy":
                s3sc = S3StorageClass.ReducedRedundancy;
                break;
                
            case "REDUCED_REDUNDANCY":
                s3sc = S3StorageClass.ReducedRedundancy;
                break;
                
            case "StandardInfrequentAccess":
                s3sc = S3StorageClass.StandardInfrequentAccess;
                break;
                
            case "STANDARD_IA":
                s3sc = S3StorageClass.StandardInfrequentAccess;
                break;
                
            case "GlacierInstantRetrieval":
                s3sc = S3StorageClass.GlacierInstantRetrieval;
                break;
                
            case "GLACIER_IR":
                s3sc = S3StorageClass.GlacierInstantRetrieval;
                break;
                
            case "OneZoneInfrequentAccess":
                s3sc = S3StorageClass.OneZoneInfrequentAccess;
                break;
                
            case "ONEZONE_IA":
                s3sc = S3StorageClass.OneZoneInfrequentAccess;
                break;
                
            default:
                s3sc = S3StorageClass.Other;
                break;
        }
        return s3sc;
    }

    public ResponseStorageClass getStorageClassType() {
        return storageClassType;
    }

    public void setStorageClassType(ResponseStorageClass storageClassType) {
        this.storageClassType = storageClassType;
    }

    public S3StorageClass getStorageClassS3() {
        return storageClassS3;
    }

    public void setStorageClassS3(S3StorageClass storageClassS3) {
        this.storageClassS3 = storageClassS3;
    }
}

