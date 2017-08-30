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
import java.util.HashMap;
import java.util.List;
import java.util.Set;

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
 * this routine is used to build a manifest.xml file from content saved in cloud.
 * 
 * Note that the constructed manifest.xml will only contain additions and content replacement. 
 * If any component was deleted in later versions it cannot be identified by this routine.
 */
public class AddObjectComponent {
    
    protected static final String NAME = "AddObjectComponent";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    protected final static String NL = System.getProperty("line.separator");
    
    protected LoggerInf logger = null;
    protected File addFile = null;
    protected CloudStoreInf cloud = null;
    protected String container = null;
    protected Tika tika = null;
    protected boolean displayFile = false;
   
    /**
     * Get object for constructing manifest
     * @param cloud Cloud access object interface
     * @param container container/bucket of cloud storage containing object
     * @param directory local directory file to contain extracted content from cloud
     * @param logger Merritt logger
     * @returnBuildObjectManifest
     * @throws TException 
     */
    public static AddObjectComponent getAddObjectComponent(
            CloudStoreInf cloud,
            String container,
            LoggerInf logger)
        throws TException
    {
        return new AddObjectComponent( cloud, container, logger);
    }
    
    /**
     * Constructor
     * @param cloud Cloud access object interface
     * @param container container/bucket of cloud storage containing object
     * @param directory local directory file to contain extracted content from cloud
     * @param logger Merritt logger
     * @throws TException 
     */
    protected AddObjectComponent(
            CloudStoreInf cloud,
            String container,
            LoggerInf logger)
        throws TException
    {
        this.cloud = cloud;
        this.container = container;
        if (StringUtil.isAllBlank(container)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "container/bucket not supplied");
        }
        this.logger = logger; 
        tika = new Tika(logger);
    }
    
    /**
     * Add new component to file (e.g. fix)
     * @param objectID object identifier
     * @param versionID version identifier
     * @param fileID file identifier/path
     * @param inputFile file to be added
     * @param addIt true=physically add file
     * @param replaceIt true=replace file if already exists
     * @throws TException 
     */
    public void addComponent(
            String objectIDS,
            int versionID,
            String fileID,
            File inputFile,
            boolean addIt,
            boolean replaceIt)
        throws TException
    {
        Identifier objectID = new Identifier(objectIDS);
        addComponent(objectID, versionID, fileID, inputFile, addIt, replaceIt);
    }
    
    /**
     * Add new component to file (e.g. fix)
     * @param objectID object identifier
     * @param versionID version identifier
     * @param fileID file identifier/path
     * @param inputFile file to be added
     * @param addIt true=physically add file
     * @param replaceIt true=replace file if already exists
     * @throws TException 
     */
    public void addComponent(
            Identifier objectID,
            int versionID,
            String fileID,
            File inputFile,
            boolean addIt,
            boolean replaceIt)
        throws TException
    {
        
        InputStream fileStream = null;
        try {
            if (inputFile == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "inputFile missing");
            }
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "objectID missing");
            }
            System.out.println("***addComponent***:\n"
                    + " - inputFile=" + inputFile.getAbsolutePath() + "\n"
                    + " - objectID=" + objectID.getValue() + "\n"
                    + " - versionID=" + versionID + "\n"
                    + " - fileID=" + fileID + "\n"
                    + " - addIt=" + addIt + "\n"
                    + " - replaceIt=" + replaceIt + "\n"
                    );
            boolean fileExists = false;
            CloudResponse response = new CloudResponse(container, objectID, versionID, fileID);
            fileStream = cloud.getObject(container, objectID, versionID, fileID, response);
            displayResponse("component getObject", response);
            if (fileStream != null) {
                System.out.println("***Component exists");
                if (displayFile) {
                    String displayString = StringUtil.streamToString(fileStream, "utf-8");
                    System.out.println("****DisplayFile:\n" + displayString + "\n****\n");
                }
                closeStream(fileStream);
                fileExists = true;
            }
            if (fileExists && !replaceIt) {
                System.out.println("***Component exists but resplace not allowed - process terminates");
                return;
            }
            if (!addIt) {
                System.out.println("***Add not allowed - process terminates");
                return;
            }
            if (replaceIt) { // if replace do delete anyway because of corrupt content
                response = cloud.deleteObject(container, objectID, versionID, fileID);
                displayResponse("component delete response", response);
                System.out.println("***Component deleted");
            }
            if (false) return; // delete only
            response = cloud.putObject(container, objectID, versionID, fileID, inputFile);
            displayResponse("Component added", response);
            
        } catch (TException tex) {
            throw tex;
        
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
    } 
    
    /**
     * Add new component to file (e.g. fix)
     * @param key rackspace key
     * @param inputFile file to be added
     * @param addIt true=physically add file
     * @param replaceIt true=replace file if already exists
     * @throws TException 
     */
    public void addComponent(
            String key,
            File inputFile,
            boolean addIt,
            boolean replaceIt)
        throws TException
    {
        
        InputStream fileStream = null;
        try {
            if (inputFile == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "inputFile missing");
            }
            if (StringUtil.isAllBlank(key)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "key missing");
            }
            System.out.println("***addComponent***:\n"
                    + " - inputFile=" + inputFile.getAbsolutePath() + "\n"
                    + " - key=" + key +"\n"
                    + " - addIt=" + addIt + "\n"
                    + " - replaceIt=" + replaceIt + "\n"
                    );
            boolean fileExists = false;
            CloudResponse response = new CloudResponse(container, key);
            fileStream = cloud.getObject(container, key, response);
            displayResponse("component getObject", response);
            if (fileStream != null) {
                System.out.println("***Component exists");
                if (displayFile) {
                    String displayString = StringUtil.streamToString(fileStream, "utf-8");
                    System.out.println("****DisplayFile:\n" + displayString + "\n****\n");
                }
                closeStream(fileStream);
                fileExists = true;
            }
            if (fileExists && !replaceIt) {
                System.out.println("***Component exists but resplace not allowed - process terminates");
                return;
            }
            if (!addIt) {
                System.out.println("***Add not allowed - process terminates");
                return;
            }
            if (replaceIt) { // if replace do delete anyway because of corrupt content
                response = cloud.deleteObject(container, key);
                displayResponse("component delete response", response);
                System.out.println("***Component deleted");
            }
            if (false) return; // delete only
            response = cloud.putObject(container, key, inputFile);
            displayResponse("Component added", response);
            
        } catch (TException tex) {
            throw tex;
        
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
    } 
    
    protected void closeStream(InputStream inStream)
        throws TException
    {
        try {
            inStream.close();
        } catch (Exception ex) {
            System.out.println("***Manifest Exception:" + ex);
        }
    }
    
    protected void displayResponse(String header, CloudResponse response)
    {
        System.out.println(response.dump(header)
                + " - response.status=" + response.getStatus()
                + " - response.httpStatus=" + response.getHttpStatus()
                );
        Exception responseException = response.getException();
        if (responseException != null) {
            System.out.println("ResponseException:" + responseException);
        }
    }
}
