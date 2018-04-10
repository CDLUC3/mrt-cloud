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
package org.cdlib.mrt.s3.pairtree;
//import org.cdlib.mrt.s3.service.*;



import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.core.Identifier;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

import org.cdlib.mrt.utility.Checksums;

import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudStoreAbs;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.PropertiesUtil;

/**
 * Specific SDSC Storage Cloud handling
 * @author dloy
 */
public class PairtreeCloud
    extends CloudStoreAbs
    implements CloudStoreInf
{
    private static final boolean DEBUG = false;
    private static final boolean ALPHANUMERIC = false;
    private static final String FILEPROPERTIES = "component.properties";
    private static final String DIGESTNAME = "digest";
    private boolean pairPath = false;
    
    public static PairtreeCloud getPairtreeCloud(
            boolean pairPath,
            LoggerInf logger)
        throws TException
    {
        return new PairtreeCloud(pairPath, logger);
    }
    
    protected PairtreeCloud(
            boolean pairPath,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.pairPath = pairPath;
    }
    
    protected File getBase(String filePath)
        throws TException
    {
        try {
            if (StringUtil.isEmpty(filePath)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getBase missing filePath");
            }
            filePath = filePath.trim();
            File baseDir = new File(filePath);
            if (!baseDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getBase file does not exist:" + filePath);
                
            }
            return baseDir;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException (ex);
        }
    }
    
    protected File getDir(File baseDir, String key)
        throws TException
    {
        try {
            if (pairPath) {
                String pairKey = key;
                int endArk = key.indexOf('|');
                if (endArk >= 0) {
                    String [] parts = key.split("\\|");
                    File outFile = buildPairDirectoryRetry(baseDir, parts[0], 5);
                    for (int p=1; p<parts.length ; p++) {
                        String part = parts[p];
                        //String name = PairtreeUtilLocal.getPairName(part);
                        String name = part;
                        outFile = new File(outFile, name);
                    }
                    
                    outFile.mkdirs();
                    return outFile;
                }
                return buildPairDirectoryRetry(baseDir, pairKey, 5);
            } else {
                String keyName = PairtreeUtilLocal.getPairName(key);
                File retFile = new File(baseDir, keyName);
                retFile.mkdir();
                return retFile;
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException (ex);
        }
    }    
    protected File getDirRead(File baseDir, String key)
        throws TException
    {
        try {
            if (pairPath) {
                String pairKey = key;
                int endArk = key.indexOf('|');
                if (endArk >= 0) {
                    String [] parts = key.split("\\|");
                    File outFile = PairtreeUtilLocal.buildPairDirectory(baseDir, parts[0]);
                    for (int p=1; p<parts.length ; p++) {
                        String part = parts[p];
                        //String name = PairtreeUtilLocal.getPairName(part);
                        String name = part;
                        outFile = new File(outFile, name);
                    }
                    if (!outFile.exists()) {
                        return null;
                    }
                    //outFile.mkdirs();
                    return outFile;
                }
                return PairtreeUtilLocal.getPairDirectory(baseDir, pairKey);
            } else {
                String keyName = PairtreeUtilLocal.getPairName(key);
                File retFile = new File(baseDir, keyName);
                retFile.mkdir();
                return retFile;
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException (ex);
        }
    }
    
    public static File buildPairDirectoryRetry(
            File baseDir,
            String pairKey,
            int retryCnt)
        throws TException
     {       
        File dir = null;
        
        String msg = " - baseDir:" + baseDir.getAbsolutePath()
                    + " - pairKey:" + pairKey;
        for (int i=1; i<=retryCnt; i++) {
            dir = PairtreeUtilLocal.buildPairDirectory(baseDir, pairKey);
            if (dir != null) {
                if (i > 1) {
                    System.out.println("***RECOVER(" + i + ") - buildPairDirectoryRetry:"
                            + msg
                    );
                }
                return dir;
            }
            System.out.println("***WARNING(" + i + ") - buildPairDirectoryRetry: unable to build directory:"
                    + msg
            );
            try{
                Thread.sleep(1000*i);
            } catch (Exception ex) { }
        }
        System.out.println("***FAIL - buildPairDirectoryRetry: unable to build directory:"
                + msg
        );
        throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "buildPairDirectoryRetry: unable to build directory:"
                + msg
        );
     }
    
    protected CloudResponse putObject(
            CloudResponse response,
            File inputFile)
        throws TException
    {        
        try {
            if (!isValidFile(inputFile)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - file not valid");
            }
            String bucketName = response.getBucketName();
            File baseDir = getBase(bucketName);
            String key = response.getStorageKey();
            File pairDir = getDir(baseDir, key);
            File component = new File(pairDir, "component.txt");
            String [] types = {"md5", "sha256"};
            Checksums checkInput = Checksums.getChecksums(types, inputFile);
            String md5HexInput = checkInput.getChecksum("md5");
            if (!component.exists() || (component.length() != inputFile.length())) {
                InputStream iStream = new FileInputStream(inputFile);
                FileUtil.stream2File(iStream, component);
                if (false) System.out.println("***Component copied:"
                            + " - bucket=" + response.getBucketName()
                            + " - storageKey=" + response.getStorageKey()
                            + " - file=" + component.getAbsolutePath()
                    );
            } else {
                System.out.println("***Component exists - not copied:"
                            + " - bucket=" + response.getBucketName()
                            + " - storageKey=" + response.getStorageKey()
                            + " - file=" + component.getAbsolutePath()
                    );
            }
            
            
            Checksums checkComponent = Checksums.getChecksums(types, component);
            String md5HexComponent = checkComponent.getChecksum("md5");
            //String md5HexComponent = CloudUtil.getDigestValue(component, logger);
            if (!md5HexComponent.equals(md5HexInput)) {
                InputStream iStream = new FileInputStream(inputFile);
                FileUtil.stream2File(iStream, component);
                checkComponent = Checksums.getChecksums(types, component);
                md5HexComponent = checkComponent.getChecksum("md5");
                if (!md5HexComponent.equals(md5HexInput)) {
                    throw new TException.FIXITY_CHECK_FAILS(MESSAGE + "Component copy fails "
                            + " - response.bucket=" + response.getBucketName()
                            + " - response.storage.key=" + response.getStorageKey()
                            + " - inputFile length=" + inputFile.length()
                            + " - component length=" + component.length()
                            + " - inputFile md5=" + md5HexInput
                            + " - componentFile md5=" + md5HexComponent
                            );
                }
                System.out.println("***Component match fail replace:"
                            + " - bucket=" + response.getBucketName()
                            + " - storageKey=" + response.getStorageKey()
                            + " - file=" + component.getAbsolutePath()
                    );
            }
            Path componentPath = component.toPath();
            BasicFileAttributes attr = Files.readAttributes(componentPath, BasicFileAttributes.class);
            FileTime ft = attr.lastModifiedTime();
            response.setFileMetaProperty("update", ft.toString());
            response.setMd5(md5HexComponent);
            response.setSha256(checkComponent.getChecksum("sha256"));
            response.setFileMetaProperty("sha256", response.getSha256());
            response.setFileMetaProperty("key", key);
            response.setFileMetaProperty("digestType", "md5");
            response.setFileMetaProperty(DIGESTNAME, md5HexComponent);
            response.setFileMetaProperty("size", "" + component.length());
            response.setFileMetaProperty("bucket", bucketName);
            Properties prop = response.getFileMetaProperties();
            String loadString = PropertiesUtil.buildLoadProperties(prop);
            File propFile = new File(pairDir, FILEPROPERTIES);
            FileUtil.string2File(propFile, loadString);
                        
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
    }
    
    protected CloudResponse putObjectSave(
            CloudResponse response,
            File inputFile)
        throws TException
    {        
        try {
            if (!isValidFile(inputFile)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - file not valid");
            }
            String bucketName = response.getBucketName();
            File baseDir = getBase(bucketName);
            String key = response.getStorageKey();
            File pairDir = getDir(baseDir, key);
            File component = new File(pairDir, "component.txt");
            String md5HexInput = CloudUtil.getDigestValue(inputFile, logger);
            if (!component.exists() || (component.length() != inputFile.length())) {
                InputStream iStream = new FileInputStream(inputFile);
                FileUtil.stream2File(iStream, component);
                if (false) System.out.println("***Component copied:"
                            + " - bucket=" + response.getBucketName()
                            + " - storageKey=" + response.getStorageKey()
                            + " - file=" + component.getAbsolutePath()
                    );
            } else {
                System.out.println("***Component exists - not copied:"
                            + " - bucket=" + response.getBucketName()
                            + " - storageKey=" + response.getStorageKey()
                            + " - file=" + component.getAbsolutePath()
                    );
            }
            
            String md5HexComponent = CloudUtil.getDigestValue(component, logger);
            if (!md5HexComponent.equals(md5HexInput)) {
                InputStream iStream = new FileInputStream(inputFile);
                FileUtil.stream2File(iStream, component);
                md5HexComponent = CloudUtil.getDigestValue(component, logger);
                if (!md5HexComponent.equals(md5HexInput)) {
                    throw new TException.FIXITY_CHECK_FAILS(MESSAGE + "Component copy fails "
                            + " - response.bucket=" + response.getBucketName()
                            + " - response.storage.key=" + response.getStorageKey()
                            + " - inputFile length=" + inputFile.length()
                            + " - component length=" + component.length()
                            + " - inputFile md5=" + md5HexInput
                            + " - componentFile md5=" + md5HexComponent
                            );
                }
                System.out.println("***Component match fail replace:"
                            + " - bucket=" + response.getBucketName()
                            + " - storageKey=" + response.getStorageKey()
                            + " - file=" + component.getAbsolutePath()
                    );
            }
            response.setMd5(md5HexInput);
            response.setFileMetaProperty("key", key);
            response.setFileMetaProperty("digestType", "md5");
            response.setFileMetaProperty(DIGESTNAME, md5HexInput);
            response.setFileMetaProperty("size", "" + component.length());
            Properties prop = response.getFileMetaProperties();
            String loadString = PropertiesUtil.buildLoadProperties(prop);
            File propFile = new File(pairDir, FILEPROPERTIES);
            FileUtil.string2File(propFile, loadString);
                        
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
    }
    
    @Override
    public CloudResponse putObject(
            String bucketName,
            String key,
            File inputFile)
        throws TException
    { 
        if (StringUtil.isEmpty(bucketName)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - bucketName not valid");
        }
        if (StringUtil.isEmpty(key)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - key not valid");
        }
        CloudResponse response = new CloudResponse(bucketName, key);
        return putObject(response, inputFile);
    }
    
    @Override
    public CloudResponse putObject(
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID,
            File inputFile)
        throws TException
    {
        CloudResponse response = new CloudResponse(bucketName, objectID, versionID, fileID);
        String key = CloudUtil.getKey(objectID, versionID, fileID, ALPHANUMERIC);
        response.setStorageKey(key);
        return putObject(response, inputFile);
    }
    
//    @Override
    public CloudResponse putObject(
            String bucketName,
            String key,
            File inputFile,
            Properties cloudProperties)
        throws TException
    {
        if (StringUtil.isEmpty(bucketName)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - bucketName not valid");
        }
        if (StringUtil.isEmpty(key)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - key not valid");
        }
        CloudResponse response = new CloudResponse(bucketName, key);
        response.setFileMeta(cloudProperties);
        return putObject(response, inputFile);
        
    }
    
//    @Override
    public CloudResponse putObject(
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID,
            File inputFile,
            Properties cloudProperties)
        throws TException
    {
        if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("putObject", cloudProperties));
        CloudResponse response = new CloudResponse(bucketName, objectID, versionID, fileID);
        String key = CloudUtil.getKey(objectID, versionID, fileID, ALPHANUMERIC);
        response.setStorageKey(key);
        response.setFileMeta(cloudProperties);
        return putObject(response, inputFile);
    }
    
    @Override
    public CloudResponse putManifest(
            String bucketName,
            Identifier objectID,
            File inputFile)
        throws TException
    {
        CloudResponse response = new CloudResponse(bucketName, objectID, null, null);
        String key = CloudUtil.getManifestKey(objectID, ALPHANUMERIC);
        response.setStorageKey(key);
        return putObject(response, inputFile);
    }

    @Override
    public CloudResponse deleteObject (
            String bucketName,
            String key)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(bucketName, key);
            response.setStorageKey(key);
            
            File baseDir = getBase(bucketName);
            File pairDir = getDir(baseDir, key);
            File [] files = pairDir.listFiles();
            if ((files == null) || (files.length == 0)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Unable to delete object - object not found:" 
                        + pairDir.getCanonicalPath());
            }
            PairtreeUtilLocal.removePairDirectory(pairDir);
            return response;
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }

    @Override
    public CloudResponse deleteObject (
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(bucketName, objectID, versionID, fileID);
            String key = CloudUtil.getKey(objectID, versionID, fileID, ALPHANUMERIC);
            response.setStorageKey(key);
            deleteObject(bucketName, key);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }

    @Override
    public CloudResponse deleteManifest (
            String bucketName,
            Identifier objectID)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(bucketName, objectID, null, null);
            String key = CloudUtil.getManifestKey(objectID, ALPHANUMERIC);
            response.setStorageKey(key);
            deleteObject(bucketName, key);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }

    @Override
    public InputStream getObject(
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID,
            CloudResponse response)
        throws TException
    {
        try {
            response.set(bucketName, objectID, versionID, fileID);
            String key = CloudUtil.getKey(objectID, versionID, fileID, ALPHANUMERIC);
            return getObject(bucketName, key, response);
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }

    @Override
    public InputStream getObject(
            String bucketName,
            String key,
            CloudResponse response)
        throws TException
    {
        try {
            response.setBucketName(bucketName);
            response.setStorageKey(key);
            
            File baseDir = getBase(bucketName);
            File pairDir = getDirRead(baseDir, key);
            if (pairDir == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Component not found for:" + key);
            }
            File component = new File(pairDir, "component.txt");
            if (!component.exists()) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Component not found for:" + key);
            }
            InputStream respStream = new FileInputStream(component);
            
            File propFile = new File(pairDir, FILEPROPERTIES);
            if (!propFile.exists()) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE + "getObject - " + FILEPROPERTIES + " missing");
            }
            Properties fileProp = PropertiesUtil.loadFileProperties(propFile);
            response.setFileMeta(fileProp);
            response.setFromProp();
            return respStream;
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }

    @Override
    public InputStream getObjectStreaming(
            String container,
            String key,
            CloudResponse response)
        throws TException
    {
        return getObject(container, key, response);
    }
    
    @Override
    public void getObject(
            String bucketName,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    { 
        InputStream inStream = getObject(bucketName, key,response);
        FileUtil.stream2File(inStream, outFile);
    }

    @Override
    public void restoreObject(
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        getObject(container, key, outFile, response);
    }
    
    @Override
    public Properties getObjectMeta(
            String bucketName,
            String key)
        throws TException
    {
        
        CloudResponse response = new CloudResponse(bucketName, key);
        try {
            File baseDir = getBase(bucketName);
            File pairDir = getDirRead(baseDir, key);
            if (pairDir == null) {
                return new Properties();
            }
            
            File propFile = new File(pairDir, FILEPROPERTIES);
            if (!propFile.exists()) {
                return null;
            }
            Properties fileProp = PropertiesUtil.loadFileProperties(propFile);
            return fileProp;
            
        } catch (Exception ex) {
                System.out.println("HERE Exception");
            ex.printStackTrace();
            return null;
        }
    }


    @Override
    public InputStream getManifest(
            String bucketName,
            Identifier objectID)
        throws TException
    {
        CloudResponse response = null;
        response = new CloudResponse(bucketName, objectID, null, null);
        return getManifest(bucketName, objectID,  response);
    }

    @Override
    public InputStream getManifest(
            String bucketName,
            Identifier objectID,
            CloudResponse response)
        throws TException
    {
        try {
            response.set(bucketName, objectID, null, null);
            String key = CloudUtil.getManifestKey(objectID, ALPHANUMERIC);
            response.setStorageKey(key);
            return getObject(bucketName, key, response);
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
   
//    @Override
    public InputStream getObject(
            String bucketName,
            Identifier objectID,
            int versionID,
            String fileID)
        throws TException
    {
        CloudResponse response = null;
        response = new CloudResponse(bucketName, objectID, versionID, fileID);
        return getObject(bucketName, objectID, versionID, fileID, response);
    }

            
//    @Override
    public CloudResponse getObjectList (
            String bucketName,
            Identifier objectID,
            Integer versionID,
            String fileID)
        throws TException
    {
        CloudResponse response = null;
        try {
            throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getObjectList: this repository form does not support this function");
            
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    @Override
    public CloudResponse getObjectList (
            String bucketName,
            String key)
        throws TException
    {
        CloudResponse response = null;
        try {
            throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getObjectList: this repository form does not support this function");
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    @Override
    public CloudResponse getObjectList (
            String bucketName,
            String key,
            int limit)
        throws TException
    {
        CloudResponse response = null;
        try {
            throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getObjectList: this repository form does not support this function");
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }

    public CloudResponse getObjectList (
            String bucketName)
        throws TException
    {
        CloudResponse response = null;
        try {
            throw new TException.UNIMPLEMENTED_CODE(MESSAGE + "getObjectList: this repository form does not support this function");
            
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    public CloudResponse validateMd5(String bucketName, String key, String inMd5)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(bucketName, key);
            InputStream componentStream = getObject(bucketName, key, response);
            if (response.getException() != null) {
                response.setMatch(false);
                return response;
            }
            String retMd5 = response.getFileMetaProperty(DIGESTNAME);
            if (DEBUG) System.out.println("RETMD5=" + retMd5);            
            if (retMd5.equals(inMd5)) response.setMatch(true);
            else response.setMatch(false);
            response.setMd5(retMd5);
            return response;
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    public boolean isAlphaNumericKey() 
    {
        return ALPHANUMERIC;
    }
    
    
}

