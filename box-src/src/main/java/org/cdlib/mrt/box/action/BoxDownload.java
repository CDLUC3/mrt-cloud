/*
Copyright (c) 2005-2012, Regents of the University of California
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
**********************************************************/
/**
 *
 * @author dloy
 */
package org.cdlib.mrt.box.action;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Properties;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.json.JSONObject;

public class BoxDownload {
    
    public enum LoadStatus {ok, match, fail, fail_fixity, skip}
   
    
    private File downloadDir = null;
    private String downloadDirS = null;
    private LoggerInf logger = null;
    
    protected BoxAPIConnection api = null;
    protected int addCnt = 0;
    protected int failCnt = 0;
    protected int skipCnt = 0;
    protected long loadTimeMs = 0;
    protected long loadSize = 0;
    protected boolean skip = false;
    protected HashMap<LoadStatus,Long> typeCnt = new HashMap<>();
    
    public static void main(String args[])
        throws TException
    {
        try {
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            
        }
    } 
    
    public static BoxDownload getBoxDownload(File downloadDir, LoggerInf logger)
        throws TException
    {
        try {
            if (downloadDir == null) {
                throw new TException.INVALID_OR_MISSING_PARM("downloadDir not set");
            }
            if (logger == null) {
                throw new TException.INVALID_OR_MISSING_PARM("logger not set");
            }
            BoxDownload downloadBox = new BoxDownload(downloadDir, logger);
            return downloadBox;
           
        } catch (TException tex) {
            System.out.println("DownloadBox TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("DownloadBox TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
            
    
    private BoxDownload(File downloadDir, LoggerInf logger)
        throws TException
    {
        try {
            
            if (!downloadDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM("downloadDir not found");
            }
            this.downloadDir = downloadDir;
            this.downloadDirS = downloadDir.getCanonicalPath() + "/";
            this.logger = logger;
           
        } catch (TException tex) {
            System.out.println("DownloadBox TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("DownloadBox TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public BoxMeta process(BoxFile boxFile, String path)
        throws Exception
    {
        
        try {
            Long startMs = System.currentTimeMillis();
            LoadStatus status = downloadFile(boxFile, path);
            Long processMs = System.currentTimeMillis() - startMs;
            BoxMeta meta = downloadMeta(boxFile, path, status, processMs) ;       
            return meta;
 
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            throw ex;
            
        }
    }
    
    private LoadStatus downloadFile(BoxFile boxFile, String path)
        throws TException
    {
        try {
            BoxFile.Info fileInfo = boxFile.getInfo();
            String outPath = downloadDirS + "data/" + path + fileInfo.getName();
            // System.out.println("outPath=" + outPath);
            File outFile = new File(outPath);
            if (outFile.exists()) {
                if (outFile.length() == fileInfo.getSize()) {
                    return LoadStatus.match;
                }
            }
            outFile.createNewFile();
            try {
                FileOutputStream fileOutStream = new FileOutputStream(outFile);
                boxFile.download(fileOutStream);
                fileOutStream.close();
                FixityTests fixity = new FixityTests(outFile, "sha1", logger);
                FixityTests.FixityResult result = fixity.validateSizeChecksum(fileInfo.getSha1(), "sha1", fileInfo.getSize());
                if (!result.checksumMatch || !result.fileSizeMatch) {
                    logger.logMessage(result.dump(path),
                        1, true);
                    return LoadStatus.fail_fixity;
                }
                return LoadStatus.ok;
                
            } catch (Exception ex) {
                System.out.println("FAIL:"
                    + " - Exception:" + ex + "\n"
                    + " - getName:" + fileInfo.getName() + "\n"
                    + " - size:" + fileInfo.getSize() + "\n"
                    + " - path:" + outPath + "\n"
                );
                ex.printStackTrace();
                return LoadStatus.fail;
            }
 
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }
    
    private BoxMeta downloadMeta(BoxFile boxFile, String path, LoadStatus status, Long processMs)
        throws TException
    {
        BoxMeta meta = null;
        try {
            BoxFile.Info fileInfo = boxFile.getInfo();
            String pathName = path + fileInfo.getName();
            String outPath = downloadDirS + "meta/" + pathName;
            File outFile = new File(outPath);
            if (outFile.exists()) {
                if (outFile.length() == fileInfo.getSize()) {
                    meta = new BoxMeta(fileInfo, pathName, LoadStatus.match, (Long)null);
                    return meta;
                }
            }
            meta = new BoxMeta(fileInfo, pathName, status, processMs);
            JSONObject json = meta.getJSON();
            String jsonS = json.toString();
            FileUtil.string2File(outFile, jsonS);
            return meta;
 
        } catch (TException tex) {
            System.out.println("main Exception:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(ex);
        }
    }

    public String getDownloadDirS() {
        return downloadDirS;
    }

    public LoggerInf getLogger() {
        return logger;
    }
    
    public static class BoxMeta {
        private BoxFile.Info fileInfo = null;
        private String id = null;
        private String name = null;
        private String pathName = null;
        private Long size = null;
        private String sha1 = null;
        private Long processMs = null;
        private LoadStatus status = null;
 
        public BoxMeta(BoxFile.Info fileInfo, String pathName, LoadStatus status, Long processMs)
            throws TException
        {
            if (fileInfo == null) {
                throw new TException.INVALID_OR_MISSING_PARM("fileInfo null");
            }
            this.fileInfo = fileInfo;
            this.id = fileInfo.getID();
            this.name = fileInfo.getName();
            this.pathName = pathName;
            this.sha1 = fileInfo.getSha1();
            this.size = fileInfo.getSize();
            this.status = status;
            this.processMs = processMs;
        }

        public BoxMeta(JSONObject jin)
            throws TException
        {
            if (jin == null) {
                throw new TException.INVALID_OR_MISSING_PARM("jin null");
            }
            try {
                this.fileInfo = null;
                this.id = jin.getString("id");
                this.name = jin.getString("name");
                this.pathName = jin.getString("pathName");
                this.sha1 = jin.getString("sha1");
                this.size = jin.getLong("size");
                String statusS = jin.getString("status");
                this.status = LoadStatus.valueOf(statusS);
                try {
                    this.processMs = jin.getLong("processMs");
                } catch (Exception ex) { }
            
            } catch (Exception ex) {
                System.out.println("main Exception:" + ex);
                ex.printStackTrace();
                throw new TException.GENERAL_EXCEPTION(ex);

            }
        }

        public BoxMeta(Properties prop)
            throws TException
        {
            if (prop == null) {
                throw new TException.INVALID_OR_MISSING_PARM("prop null");
            }
            try {
                this.fileInfo = null;
                this.id = prop.getProperty("id");
                this.name = prop.getProperty("name");
                this.pathName = prop.getProperty("pathName");
                this.sha1 = prop.getProperty("sha1");
                String sizeS = prop.getProperty("size");
                this.size = Long.parseLong(sizeS);
                
                String statusS = prop.getProperty("status");
                this.status = LoadStatus.valueOf(statusS);
                String processMsS = prop.getProperty("processMs");
                if (processMsS != null) {
                    this.processMs = Long.parseLong(processMsS);
                }
            
            } catch (Exception ex) {
                System.out.println("main Exception:" + ex);
                ex.printStackTrace();
                throw new TException.GENERAL_EXCEPTION(ex);

            }
        }
        
        public String getMessage()
        {
            String message = status
                            + "|" + fileInfo.getName()
                            + "|" + fileInfo.getSize()
                            + "|sha1|" + fileInfo.getSha1()
                            + "|" + processMs;
            return message;
        }
        
        public JSONObject getJSON()
            throws TException
        {
            try {
                JSONObject jobj = new JSONObject();
                jobj.put("id", id);
                jobj.put("name", name);
                jobj.put("pathName", pathName);
                jobj.put("sha1", sha1);
                jobj.put("size", size);
                if (status != null) {
                    jobj.put("status", status.toString());
                }
                if (processMs != null) {
                    jobj.put("processMs", processMs);
                }
                return jobj;

            } catch (Exception ex) {
                System.out.println("main Exception:" + ex);
                ex.printStackTrace();
                throw new TException.GENERAL_EXCEPTION(ex);

            }
        }
        
        public Properties getProp()
            throws TException
        {
            try {
                Properties prop = new Properties();
                prop.setProperty("id", id);
                prop.setProperty("name", name);
                prop.setProperty("pathName", pathName);
                prop.setProperty("sha1", sha1);
                prop.setProperty("size", "" + size);
                if (status != null) {
                    prop.setProperty("status", status.toString());
                }
                if (processMs != null) {
                    prop.setProperty("processMs", "" + processMs);
                }
                
                return prop;

            } catch (Exception ex) {
                System.out.println("main Exception:" + ex);
                ex.printStackTrace();
                throw new TException.GENERAL_EXCEPTION(ex);

            }
        }


        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getPathName() {
            return pathName;
        }

        public Long getSize() {
            return size;
        }

        public String getSha1() {
            return sha1;
        }

        public Long getProcessMs() {
            return processMs;
        }

        public LoadStatus getStatus() {
            return status;
        }
        
    }
}
