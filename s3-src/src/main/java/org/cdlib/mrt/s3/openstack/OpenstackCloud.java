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
package org.cdlib.mrt.s3.openstack;
//import org.cdlib.mrt.s3.service.*;



import java.io.ByteArrayInputStream;
import org.cdlib.mrt.s3.service.*;

import org.cdlib.mrt.core.Identifier;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.CloudProperties;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.openstack.utility.CloudConst;
import org.cdlib.mrt.openstack.utility.HttpGetOpenStack;
import org.cdlib.mrt.openstack.utility.OpenStackAuth;
import org.cdlib.mrt.openstack.utility.OpenStackCmdAbs;
import org.cdlib.mrt.openstack.utility.OpenStackCmdDLO;
import org.cdlib.mrt.openstack.utility.ResponseValues;
import org.cdlib.mrt.openstack.utility.SegmentValues;
import org.cdlib.mrt.openstack.utility.XValues;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudStoreAbs;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.utility.MessageDigestValue;

/**
 * Specific SDSC Storage Cloud handling
 * @author dloy
 */
public class OpenstackCloud
    extends CloudStoreAbs
    implements CloudStoreInf
{
    private static final boolean DEBUG = false;
    private static final boolean ALPHANUMERIC = false;
    private static final int SIZE_MD5_EXTENSION = 3;
    private static final String DISTRIBUTED_TRIGGER = "__";
    
    protected OpenStackCmdAbs cmd = null;
    
    public static OpenstackCloud getOpenstack(InputStream propStream, LoggerInf logger)
        throws TException
    {
        return new OpenstackCloud(propStream, logger);
    }
    
    protected OpenstackCloud(
            InputStream propStream,
            LoggerInf logger)
        throws TException
    {
        super(propStream, logger);
        setStorageService();
    }
    
    public static OpenstackCloud getOpenstackCloud(Properties prop, LoggerInf logger)
        throws TException
    {
        return new OpenstackCloud(prop, logger);
    }
    
    protected OpenstackCloud(
            Properties prop,
            LoggerInf logger)
        throws TException
    {
        super(prop, logger);
        setStorageService();
    }

    protected void setStorageService()
        throws TException
    {
        cmd = new OpenStackCmdDLO(cloudProp);
    }
    
    public CloudResponse putObject(
            CloudResponse response,
            File inputFile)
        throws TException
    {   
        try {
            if (!isValidFile(inputFile)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - file not valid");
            }
            String container = response.getBucketName();
            String key = response.getStorageKey();
            if (DEBUG)System.out.println("FILE size:" + inputFile.length());
            String md5Hex = CloudUtil.getDigestValue(inputFile, logger);
            byte[] md5Bytes = FixityTests.toByteArray(md5Hex);
            response.setMd5(md5Hex);
            Properties metaProp = null;
            if (response.getFileMetaSize() > 0) {
                metaProp = response.getFileMetaProperties();
            }
            
            container = setContainer(container, key, true);
            SegmentValues values = cmd.uploadFile(
                    container,
                    key,
                    inputFile,
                    null,
                    metaProp,
                    300000);
                        
        } catch (Exception ex) {
            handleException(response, ex);
            
        } finally {
            //FileUtil.removeFile(inputFile);
        }
        return response;
    }
    
    @Override 
    public CloudResponse putObject(
            String container,
            String key,
            File inputFile)
        throws TException
    { 
        if (StringUtil.isEmpty(container)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - container not valid");
        }
        if (StringUtil.isEmpty(key)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - key not valid");
        }
        CloudResponse response = new CloudResponse(container, key);
        return putObject(response, inputFile);
    }
    
    @Override
    public CloudResponse putObject(
            String container,
            Identifier objectID,
            int versionID,
            String fileID,
            File inputFile)
        throws TException
    {
        CloudResponse response = new CloudResponse(container, objectID, versionID, fileID);
        String key = CloudUtil.getKey(objectID, versionID, fileID, ALPHANUMERIC);
        response.setStorageKey(key);
        return putObject(response, inputFile);
    }
    
//    @Override
    public CloudResponse putObject(
            String container,
            String key,
            File inputFile,
            Properties fileMeta)
        throws TException
    { 
        if (StringUtil.isEmpty(container)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - container not valid");
        }
        if (StringUtil.isEmpty(key)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "putObject - key not valid");
        }
        CloudResponse response = new CloudResponse(container, key);
        response.setFileMeta(fileMeta);
        return putObject(response, inputFile);
    }
    
    //@Override
    public CloudResponse putObject(
            String container,
            Identifier objectID,
            int versionID,
            String fileID,
            File inputFile,
            Properties fileMeta)
        throws TException
    {
        CloudResponse response = new CloudResponse(container, objectID, versionID, fileID);
        String key = CloudUtil.getKey(objectID, versionID, fileID, ALPHANUMERIC);
        response.setStorageKey(key);
        response.setFileMeta(fileMeta);
        return putObject(response, inputFile);
    }
    
    @Override
    public CloudResponse putManifest(
            String container,
            Identifier objectID,
            File inputFile)
        throws TException
    {
        CloudResponse response = new CloudResponse(container, objectID, null, null);
        String key = CloudUtil.getManifestKey(objectID, ALPHANUMERIC);
        response.setStorageKey(key);
        return putObject(response, inputFile);
    }

    @Override
    public CloudResponse deleteObject (
            String container,
            String key)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(container, key);
            response.setStorageKey(key);
            
            container = setContainer(container, key, false);
            cmd.delete(container, key, CloudConst.LONG_TIMEOUT);
            return response;
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }

    @Override
    public CloudResponse deleteObject (
            String container,
            Identifier objectID,
            int versionID,
            String fileID)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(container, objectID, versionID, fileID);
            String key = CloudUtil.getKey(objectID, versionID, fileID, ALPHANUMERIC);
            response.setStorageKey(key);
            
            container = setContainer(container, key, false);
            cmd.delete(container, key, CloudConst.LONG_TIMEOUT);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }

    @Override
    public CloudResponse deleteManifest (
            String container,
            Identifier objectID)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(container, objectID, null, null);
            String key = CloudUtil.getManifestKey(objectID, ALPHANUMERIC);
            response.setStorageKey(key);
            
            container = setContainer(container, key, false);
            cmd.delete(container, key, CloudConst.LONG_TIMEOUT);
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
        return response;
        
    }

    @Override
    public InputStream getObject(
            String container,
            Identifier objectID,
            int versionID,
            String fileID,
            CloudResponse response)
        throws TException
    {
        try {
            response.set(container, objectID, versionID, fileID);
            String key = CloudUtil.getKey(objectID, versionID, fileID, ALPHANUMERIC);
            return getObject(container, key, response);
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }

    @Override
    public InputStream getObject(
            String container,
            String key,
            CloudResponse response)
        throws TException
    {
        try {
            container = setContainer(container, key, false);
            ResponseValues values = cmd.retrieveRetry(container, key, CloudConst.LONG_TIMEOUT, 4);
            CloudProperties cloudProp = values.getCloudProperties();
            response.setFileMeta(cloudProp);
            return values.inputStream;
            
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
            String container,
            String key,
            File outFile,
            CloudResponse response)
        throws TException
    {
        try {
            container = setContainer(container, key, false);
            OpenStackAuth openStackAuth =cmd.getOpenStackAuth();
            ResponseValues values = HttpGetOpenStack.getFile(openStackAuth, container, key, outFile, CloudConst.LONG_TIMEOUT, logger);
            CloudProperties cloudProp = values.getCloudProperties();
            response.setFileMeta(cloudProp);
            if (!StringUtil.isAllBlank(values.getSha_256())){
                response.setFileMetaProperty("sha-256", values.getSha_256());
            }
            
        } catch (Exception ex) {
            handleException(response, ex);
        }
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
            CloudList cloudList = null;
            try {
                CloudResponse search = getObjectList(bucketName, key);
                cloudList = search.getCloudList();
                if (DEBUG) System.out.println("getObjectMeta:" + cloudList.size());
                List<CloudList.CloudEntry> list = cloudList.getList();
                if (DEBUG) System.out.println("Entries:" + list.size());
            } catch (Exception ex) {
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
                return null;
            }
            response.setObjectList(cloudList);
            CloudList list = response.getCloudList();
            if (list == null) {
                return null;
            }
            if (list.size() == 0) {
                return new Properties();
            }
            CloudList.CloudEntry entry = list.get(0);
            Properties prop = entry.getProp();
            return prop;
            
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public InputStream getManifest(
            String container,
            Identifier objectID)
        throws TException
    {
        CloudResponse response = null;
        response = new CloudResponse(container, objectID, null, null);
        return getManifest(container, objectID,  response);
    }

    @Override
    public InputStream getManifest(
            String container,
            Identifier objectID,
            CloudResponse response)
        throws TException
    {
        try {
            response.set(container, objectID, null, null);
            String key = CloudUtil.getManifestKey(objectID, ALPHANUMERIC);
            response.setStorageKey(key);
            return getObject(container,key,response);
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
   
//    @Override
    public InputStream getObject(
            String container,
            Identifier objectID,
            int versionID,
            String fileID)
        throws TException
    {
        CloudResponse response = null;
        response = new CloudResponse(container, objectID, versionID, fileID);
        return getObject(container, objectID, versionID, fileID, response);
    }


//    @Override
    public CloudResponse getObjectList (
            String container,
            Identifier objectID,
            Integer versionID,
            String fileID)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(container, objectID, versionID, fileID);
            String key = CloudUtil.getKey(objectID, versionID, fileID, ALPHANUMERIC);
            response.setStorageKey(key);
            
            container = setContainer(container, key, false);
            CloudList cloudList = cmd.getList(container, key, CloudConst.LONG_TIMEOUT);
            response.setObjectList(cloudList);
            return response;
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    
    public CloudResponse getObjectListOriginal (
            String container,
            String prefix)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(container, prefix);
            response.setStorageKey(prefix);
            CloudList cloudList = cmd.getList(container, prefix, CloudConst.LONG_TIMEOUT);
            response.setObjectList(cloudList);
            return response;
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    @Override
    public CloudResponse getObjectList (
            String container,
            String prefix)
        throws TException
    {
        CloudResponse response = null;
        try {
            container = setContainer(container, prefix, false);
            response = new CloudResponse(container, prefix);
            response.setStorageKey(prefix);
            CloudList totalList = new CloudList();
            String marker = null;
            
            CloudList list = null;
            CloudList.CloudEntry entry = null;
            for (int i=0; i < 500; i++) {
                if (DEBUG) System.out.println("***getObjectList[" + i + "," + totalList.size() + ")]:"
                        + " - container=" + container
                        + " - prefix=" + prefix
                        + " - marker=" + marker
                );
                int retry = 0;
                for (; retry < 3; retry++) {
                    try {
                        list = cmd.getListFull(container, prefix, marker, 1000, CloudConst.LONG_TIMEOUT);
                        if (list == null) {
                            if (DEBUG) System.out.println("***list(" + retry + "): null");
                        } else {
                            if (DEBUG) System.out.println("***list(" + retry + "): size=" + list.size());
                        }
                        break;
                        
                    } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
                        if (DEBUG) System.out.println("***list(" + retry + "): item not found");
                        throw rinf;
                        
                    } catch (TException.EXTERNAL_SERVICE_UNAVAILABLE esu) {
                        System.out.println("WARNING(" + retry + ") Service unavailable retry getObjectList"
                        + " - container=" + container
                        + " - prefix=" + prefix
                        + " - marker=" + marker
                        );
                        continue;
                    }
                }
                if (retry >= 3) {
                    throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                            "Retry exceeded getObjectList\n"
                        + " - container=" + container
                        + " - prefix=" + prefix
                        + " - marker=" + marker
                    );
                }
                //CloudList list = cmd.getListFull(container, ark, 20, marker, CloudConst.LONG_TIMEOUT);
                if (list == null) break;
                if (DEBUG) System.out.println(list.dump("Entries"));
                entry = cmd.getLastEntry(list);
                if (entry == null) break;
                marker = entry.getKey();
                addListEntries(totalList, list);
            }
            if (entry != null) {
                throw new TException.INVALID_ARCHITECTURE("Number of file entries exceeds maximum:" 
                        + totalList.size());
            }
            response.setObjectList(totalList);
            return response;
            
        } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
            throw rinf;
                        
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }
    
    protected void addListEntries(CloudList totalList, CloudList list)
        throws TException
    {
        List<CloudList.CloudEntry> entryList = list.getList();
        for (CloudList.CloudEntry entry : entryList) {
            totalList.add(entry);
        }
    }
    
    @Override
    public CloudResponse getObjectList (
            String container,
            String marker,
            int limit)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = new CloudResponse(container, marker);
            response.setStorageKey(marker);
            if(isDistributedContainer(container)) {
                throw new TException.INVALID_DATA_FORMAT("List operation may not be used on Distributed container:" 
                    + container);
            }
            CloudList cloudList = cmd.getList(container, marker, limit, CloudConst.LONG_TIMEOUT);
            response.setObjectList(cloudList);
            return response;
            
        } catch (Exception ex) {
            handleException(response, ex);
            return null;
        }
    }

    public CloudResponse getObjectList (
            String container)
        throws TException
    {
        return getObjectList(container, null);
    }
        
    
        
    @Override
    public StateHandler.RetState getState (
            String bucketName)
        throws TException
    {
        StateHandler stateHandler = null;
        try {
            stateHandler = StateHandler.getStateHandler(this, bucketName, logger);
            return stateHandler.process();
            
        } catch (Exception ex) {
            return StateHandler.getError(bucketName, NAME, "Error getState:" + ex);
        }
    }
    
    public CloudResponse validateMd5(String container, String key, String inMd5)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = getObjectList(container, key);
            if (response.getException() != null) {
                response.setMatch(false);
                return response;
            }
            CloudList objects = response.getCloudList();
            if ((objects == null) || (objects.size() == 0)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("item not found - key=" + key);
            }
            if (objects.size() != 1) {
                throw new TException.INVALID_OR_MISSING_PARM("key returns multiple objects - key=" + key
                        + " - count=" + objects.size());
            }
            CloudList.CloudEntry entry = objects.get(0);
            String retMd5 = entry.getEtag();
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
    
    /**
     * setContainer determines if the passed container is distributed and will 
     * automatically append digest characters to generate physical container
     * @param container physical or distributed container
     * @param key delimited component key - distributed extension ins built off
     * first element of key
     * @param addNewContainer true=add new container if not present, false=ignore add
     * @return physical key
     * @throws TException 
     */
    public String setContainer(String container, String key, boolean addNewContainer)
         throws TException
    {
        try {
            if (!isDistributedContainer(container)) {
                return container;
            }
            String base = key;
            int pos = key.indexOf("|");
            if (pos >= 0) {
                base = key.substring(0,pos);
            }
            byte[] bytes = base.getBytes("utf-8");
            InputStream stream = new ByteArrayInputStream(bytes);
            MessageDigestValue mdv = new MessageDigestValue(stream, "md5", logger);
            String md5 = mdv.getChecksum();
            String md5Trim = md5.substring(0,SIZE_MD5_EXTENSION);
            String retContainer = container + md5Trim;
            if (addNewContainer) {
                ResponseValues response = cmd.createContainerRetry(retContainer, 60000, 3);
            }
            return retContainer;
            
        } catch (TException tex) {
           throw tex;
           
        } catch (Exception ex) {
           throw new TException(ex);
        }
        
    }
    
    /**
     * Is this a distributed container?
     * Distributed container has a common prefix with trailing trigger
     * @param container
     * @return true=distributed container, false=physical container
     * @throws TException 
     */
    public static boolean isDistributedContainer(String container)
         throws TException
    {
        int pos = container.lastIndexOf(DISTRIBUTED_TRIGGER);
        if (pos != (container.length()-DISTRIBUTED_TRIGGER.length())) {
            return false;
        }
        return true;
    }
    
    @Override
    public Boolean isAlive(String bucketName)
    {
        String host = cmd.getOpenStackHost();
        host = "https://" + host + ":443";
        return isAliveTest(host);
    }
    
    public boolean isAlphaNumericKey() 
    {
        return ALPHANUMERIC;
    }
}

