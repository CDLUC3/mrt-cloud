
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
package org.cdlib.mrt.s3.cloudhost;
//import org.cdlib.mrt.s3.service.*;




import org.cdlib.mrt.core.Identifier;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * Specific SDSC Storage Cloud handling
 * @author dloy
 */
public class CloudhostClient
{
    private static final boolean DEBUG = false;
    private static final boolean ALPHANUMERIC = false;
    protected static final String NAME = "AWSS3Cloud";
    protected static final String MESSAGE = NAME + ": ";
    protected String urlBase = null;
    protected int node = 0;
    protected  LoggerInf logger = null;
    
    protected CloudhostClient(
            LoggerInf logger)
        throws TException
    {
        this.logger = logger;
    }
    
    public static URL keyToURL (
            Integer nodeID, String key, String baseUrl, String function, String query)
        throws TException
    {
        try {
            if (baseUrl == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getKey - baseUrl required");
            }
            if (function == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getKey - function required");
            }
            if (key == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "getKey - key required");
            }

            String [] parts = key.split("\\|");
            if (parts.length < 3) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "keyToFile - outFile required");
            }
            Identifier objectID = new Identifier(parts[0]);
            
            int versionID = -1;
            try {
                versionID = Integer.parseInt(parts[1]);
                
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "version invalid:" + parts[1]);
            }
            
            return keyToURL (nodeID, objectID, versionID, parts[2],
                baseUrl, function, query);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static URL keyToURL (
            Integer nodeID, 
            Identifier objectID, 
            int versionID,
            String fieldID,
            String baseUrl, String function, String query)
        throws TException
    {
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "keyToURL - objectID required");
            }
            if (versionID < 0) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "keyToURL - versionID required");
            }
            if (StringUtil.isAllBlank(fieldID)) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "keyToURL - fieldID required");
            }
            String urlS =  baseUrl + "/" 
                    + function + "/" ;
            if (nodeID != null) {
                urlS += nodeID  + "/" ;
            }
            String objectEnc = URLEncoder.encode(objectID.getValue(), "utf-8");
            String fieldEnc = URLEncoder.encode(fieldID, "utf-8");
            urlS +=  objectEnc + "/" 
                    + versionID + "/" 
                    + fieldEnc;
            if (!StringUtil.isAllBlank(query)) {
                urlS += "?" + query;
            }
            System.out.println("URL:" + urlS);
            URL url = new URL(urlS);
            return url;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static URL getManifestURL(
            Integer nodeID, 
            Identifier objectID, 
            String baseUrl)
        throws TException
    {
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "keyToURL - objectID required");
            }
            String urlS =  baseUrl + "/" 
                    + "manifest/" ;
            if (nodeID != null) {
                urlS += nodeID  + "/" ;
            }
            URL url = new URL(urlS);
            return url;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static Integer getNodeID(String container)
        throws TException
    {
        if (true) return null; //dummy for now
        if (StringUtil.isAllBlank(container)) {
            return null;
        }
        int nodeID = 0;
        try {
            nodeID = Integer.parseInt(container);
        } catch (Exception ex) {
            throw new TException.INVALID_DATA_FORMAT(MESSAGE + "getNodeID - container not numeric:" + container);
        }
        return nodeID;
    }
    
    public static CloudhostServiceState getService(String base, long node, LoggerInf logger)
        throws TException
    {
        File tmp = null;
        try {
            String urlS = base + "/state/" + node + "?t=anvl";
            tmp = FileUtil.getTempFile("anvl", ".properties");
            FileUtil.url2File(logger, urlS, tmp, 4);
            CloudhostServiceState state = new CloudhostServiceState(tmp);
            return state;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (tmp != null) {
                try {
                    tmp.delete();
                } catch (Exception ex) { }
            }
        }
        
    }
    
    public static CloudhostMetaState getMeta(String base, long node, String key, LoggerInf logger)
        throws TException
    {
        File tmp = null;
        try {
            String encKey = URLEncoder.encode(key, "utf-8");
            String urlS = base + "/metadata/" + node + "/" + encKey + "?t=anvl";
            tmp = FileUtil.getTempFile("anvl", ".properties");
            FileUtil.url2File(logger, urlS, tmp, 4);
            CloudhostMetaState state = new CloudhostMetaState(tmp);
            return state;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (tmp != null) {
                try {
                    tmp.delete();
                } catch (Exception ex) { }
            }
        }
        
    }
    
    public static void getData(String base, long node, String key, File cloudFile, LoggerInf logger)
        throws TException
    {
        try {
            String encKey = URLEncoder.encode(key, "utf-8");
            String urlS = base + "/data/" + node + "/" + encKey;
            FileUtil.url2File(logger, urlS, cloudFile, 4);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
    }
    
    public static CloudhostDeleteState delete(String base, long node, String key, LoggerInf logger)
        throws TException
    {
        File tmp = null;
        try {
            String encKey = URLEncoder.encode(key, "utf-8");
            String urlS = base + "/delete/" + node + "/" + encKey + "?t=anvl";
            tmp = FileUtil.getTempFile("anvl", ".properties");
            InputStream inStream = HTTPUtil.deleteObject(urlS, 60000, 4);
            FileUtil.stream2File(inStream, tmp);
            CloudhostDeleteState state = new CloudhostDeleteState(tmp);
            return state;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (tmp != null) {
                try {
                    tmp.delete();
                } catch (Exception ex) { }
            }
        }
        
    }
    
    public static CloudhostAddState add(String base, long node, String key, File data, LoggerInf logger)
        throws TException
    {
        File tmp = null;
        try {
            String urlS = base + "/content/" + node +  "?t=anvl";
            tmp = FileUtil.getTempFile("anvl", ".properties");
            Properties prop = new Properties();
            prop.setProperty("key", key);
            HashMap<String, File> fileParts = new HashMap();
            fileParts.put("data", data);
            InputStream inStream = HTTPUtil.postMultipartObject2(urlS, prop, fileParts, 360000);
            FileUtil.stream2File(inStream, tmp);
            CloudhostAddState state = new CloudhostAddState(tmp);
            return state;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (tmp != null) {
                try {
                    tmp.delete();
                } catch (Exception ex) { }
            }
        }
        
    }
    
    public static CloudhostFixityState fixity(String base, long node, String key, String digestType, 
            String digest, long fileLength, LoggerInf logger)
        throws TException
    {
        File tmp = null;
        try {
            String urlS = base + "/fixity/" + node +  "?t=anvl";
            tmp = FileUtil.getTempFile("anvl", ".properties");
            Properties prop = new Properties();
            prop.setProperty("key", key);
            prop.setProperty("digestType", digestType);
            prop.setProperty("digest", digest);
            prop.setProperty("length", "" + fileLength);
            InputStream inStream = HTTPUtil.putMultipartObject(urlS, prop, null, 360000);
            
            FileUtil.stream2File(inStream, tmp);
            CloudhostFixityState state = new CloudhostFixityState(tmp);
            return state;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (tmp != null) {
                try {
                    tmp.delete();
                } catch (Exception ex) { }
            }
        }
        
    }
    
    
}

