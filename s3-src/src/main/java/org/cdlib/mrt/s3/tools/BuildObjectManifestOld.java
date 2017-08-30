package org.cdlib.mrt.s3.tools;

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
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;

import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.ManifestStr;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.core.Tika;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.FileUtil;

import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.PairtreeUtil;
import org.cdlib.mrt.utility.StringUtil;

/**
 * This routine generates a list of manifest URLS that can be used as a feed to inv zookeeper loader
 */
public class BuildObjectManifestOld {
    
    protected static final String NAME = "BuildObjectManifestOld";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    protected final static String NL = System.getProperty("line.separator");
    
    protected LoggerInf logger = null;
    protected File directory = null;
    protected CloudStoreInf cloud = null;
    protected String container = null;
    protected Tika tika = null;
   
    
    public static BuildObjectManifestOld getBuildObjectManifest(
            CloudStoreInf cloud,
            String container,
            File directory,
            LoggerInf logger)
        throws TException
    {
        return new BuildObjectManifestOld( cloud, container, directory, logger);
    }
    
    protected BuildObjectManifestOld(
            CloudStoreInf cloud,
            String container,
            File directory,
            LoggerInf logger)
        throws TException
    {
        this.cloud = cloud;
        this.container = container;
        if (StringUtil.isAllBlank(container)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "container/bucket not supplied");
        }
        this.directory = directory;
        this.logger = logger; 
        tika = new Tika(logger);
    }
    
    public File build(String objectIDS)
        throws TException
    {
        try {
            //String objectIDS = objectID.getValue();
            CloudList cloudList = getObjectList(objectIDS);
            ArrayList<CloudList.CloudEntry> entries = cloudList.getList();
            ArrayList<FileComponent> components = new ArrayList(entries.size());
            for (CloudList.CloudEntry entry : entries) {
                FileComponent component = buildComponent(objectIDS, entry);
                if (component == null) continue;
                components.add(component);
            }
            Identifier objectID = new Identifier(objectIDS);
            VersionMap map = new VersionMap(objectID, logger);
            map.addVersion(components);
            File mapFile = setManifest(objectIDS, map);
            add(objectIDS, mapFile);
            return mapFile;
            
        } catch (TException tex) {
            throw tex;
                    
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }  
    public CloudList getObjectList(String prefix)
        throws TException
    {
        CloudResponse response = cloud.getObjectList(container, prefix);
        
        CloudList cloudList = response.getCloudList();
        return cloudList;
    }
    
    public FileComponent buildComponent(String objectIDS, CloudList.CloudEntry entry)
        throws TException
    {
        try {
            if (entry.key.contains("|manifest")) {
                System.out.println("Skip:" + entry.key);
                return null;
            }
            FileComponent component = new FileComponent();
            Date date = DateUtil.getDateFromString(entry.lastModified.substring(0,23), "yyyy-MM-dd'T'HH:mm:ss.SSS");
            DateState dateState = new DateState(date);
            component.setCreated(dateState);
            component.addMessageDigest(entry.etag, "md5");
            component.setSize(entry.size);
            component.setLocalID(entry.key);
            CloudUtil.KeyElements keyEle = CloudUtil.getKeyElements(entry.key);
            component.setIdentifier(keyEle.fileID);
            setFile(entry.key, keyEle, component);
            System.out.println(component.dump(keyEle.fileID));
            return component;
            
        } catch (TException tex) {
            throw tex;
                    
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
    
    protected void setFile(String key, CloudUtil.KeyElements keyEle, FileComponent component)
        throws TException
    {
        File fileDir = directory;
        try {
            String objectIDS = keyEle.objectID.getValue();
            String pairName = PairtreeUtil.getPairName(objectIDS);
            File pairFile = new File(directory, pairName);
            pairFile.mkdirs();
            File versionFile = new File(pairFile, "" + keyEle.versionID);
            versionFile.mkdirs();
            String pathS = component.getIdentifier();
            File outFile = null;
            int pos = pathS.lastIndexOf("/");
            if (pos < 0) {
                outFile = new File(versionFile, pathS);
            } else {
                String dirPath = pathS.substring(0,pos);
                fileDir = new File(versionFile, dirPath);
                fileDir.mkdirs();
                String outName = pathS.substring(pos + 1);
                outFile = new File(fileDir, outName);
            }
            CloudResponse response = new CloudResponse();
            InputStream inStream = cloud.getObject(container, key, response);
            FileUtil.stream2File(inStream, outFile);
            component.setComponentFile(outFile);
            String mimeType = tika.getMimeType(outFile);
            component.setMimeType(mimeType);
            setDigest(component);
            System.out.println("add file:" + outFile.getAbsolutePath());
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
        
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
    }
    
    protected void setDigest(FileComponent component)
        throws TException
    {
        File file = component.getComponentFile();
        if (file == null) {
            throw new TException.INVALID_ARCHITECTURE(MESSAGE + "setDigest - file does note exist");
        }
        try {
            FixityTests test = new FixityTests(file, "md5", logger);
            MessageDigest digest = component.getMessageDigest();
            test.validateSizeChecksum(digest.getValue(), digest.getJavaAlgorithm(), component.getSize());
            FixityTests newTest = new FixityTests(file, "sha-256", logger);
            MessageDigest newDigest = 
                    new MessageDigest(newTest.getChecksum(), 
                    newTest.getChecksumJavaAlgorithm());
            component.setFirstMessageDigest(newTest.getChecksum(), newTest.getChecksumJavaAlgorithm());
            
        } catch (TException tex) {
            throw tex;
        
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
        
    }
    
    protected File setManifest(String objectIDS, VersionMap map)
        throws TException
    {
        File fileDir = directory;
        try {
            String pairName = PairtreeUtil.getPairName(objectIDS);
            File pairFile = new File(directory, pairName);
            pairFile.mkdirs();
            File outFile = new File(pairFile, "manifest.xml");
            ManifestStr manifestStr = new ManifestStr(map, outFile);
            String xml = FileUtil.file2String(outFile);
            System.out.println("Manifest:" + xml);
            return outFile;
            
        } catch (TException tex) {
            throw tex;
        
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
        
    }
    
    public void add(String objectIDS, File manifestFile)
        throws TException
    {
        try {
            Identifier objectID = new Identifier(objectIDS);
            CloudResponse response = cloud.putManifest(container, objectID, manifestFile);
            System.out.println("***ADD"
                    + " - status=" + response.getHttpStatus()
                    + " - container=" + container
                    + " - objectIDS=" + objectIDS
                    );
            
        } catch (TException tex) {
            throw tex;
        
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
        
    }
}
