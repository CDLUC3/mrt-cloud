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
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.StringUtil;

public class DownloadBox {
    
    public enum LoadStatus {ok, match, fail, fail_fixity}
   
    public static void main(String args[])
        throws TException
    {
        try {
            DownloadBox downloadBox = getDownloadBoxProp();
            
            downloadBox.process();
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            
        }
    } 
    
    public static DownloadBox getDownloadBoxProp()
        throws TException
    {
        try {
            String runDirPath = System.getenv("BOXRUN");
            if (runDirPath == null) {
                throw new TException.INVALID_OR_MISSING_PARM("BOXRUN not set");
            }
            File runDir = new File(runDirPath);
            if (!runDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM("BOXRUN file not found");
            }
            File confFile = new File(runDir, "boxconfig.prop");
            if (!confFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM("boxconfig.prop file not found");
            }
            Properties confProp = new Properties();
            FileInputStream confIS = new FileInputStream(confFile);
            confProp.load(confIS);
            DownloadBox downloadBox = new DownloadBox(runDir, confProp);
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
    
    private String privateAccess = null;
    private String downloadURL = null;
    private String downloadDirS = null;
    
    private DateState startTime = null;
    private DateState endTime = null;
    
    private File downloadDir = null;
    
    private LoggerInf logger = null;
    protected BoxAPIConnection api = null;
    protected int dumpFreq = 1;
    protected int loadCnt = 0;
    protected int failCnt = 0;
    protected long loadTimeMs = 0;
    protected long loadSize = 0;
    protected HashMap<LoadStatus,Long> typeCnt = new HashMap<>();
            
    
    private DownloadBox(File downloadDir, Properties prop)
        throws TException
    {
        try {
            
            if (!downloadDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM("downloadDir not found");
            }
            this.downloadDir = downloadDir;
            downloadDirS = downloadDir.getAbsolutePath() + "/";
            privateAccess = prop.getProperty("privateAccess");
            downloadURL = prop.getProperty("downloadURL");
            
            String dumpFreqS = prop.getProperty("dumpFreq");
            if (dumpFreqS != null) {
                try {
                    dumpFreq = Integer.parseInt(dumpFreqS);
                } catch (Exception iex) {
                    throw new TException.INVALID_OR_MISSING_PARM("dumpFreq invalid");
                }
            }
            
            startTime = new DateState();
            verify();
            setup();
            
            logger.logMessage("Start"
                    + " - start:" + startTime.getIsoDate() 
                    + " - downloadURL:" + downloadURL
                    + " - downloadDirS:" + downloadDirS,
                2, true);
            api = new BoxAPIConnection(privateAccess);
           
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
    
    private void verify()
        throws TException
    {
        if (privateAccess == null) {
            throw new TException.INVALID_OR_MISSING_PARM("config privateAccess not found");
        }
        if (downloadURL == null) {
            throw new TException.INVALID_OR_MISSING_PARM("config downloadURL not found");
        }
        if (downloadDirS == null) {
            throw new TException.INVALID_OR_MISSING_PARM("config downloadDir not found");
        }
       
    }
    
    private void setup()
        throws Exception
    {
        addDir(downloadDirS + "data/");
        logger = setLogger(downloadDirS);
    }
    
    public void process()
    {
        String path = "";
        
        try {
            
            BoxItem.Info boxItem = BoxFolder.getSharedItem(api, downloadURL);
            String itemID = boxItem.getID();
            path = downloadDirS + "data/";
            addDir(path);
            processItem(api, itemID, path, logger);
            
            endTime = new DateState();
            dumpState();
            logger.logMessage("End process"
                + " - time:" + endTime.getIsoDate() 
                + " - downloadURL:" + downloadURL
                + " - downloadDirS:" + downloadDirS,
            2, true);
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            
        }
    } 
    
    public void processItem(BoxAPIConnection api, String id, String path, LoggerInf logger)
        throws Exception
    {
       
        try {
            //System.out.println("** id=" + id);
            BoxFolder folder = new BoxFolder(api, id);
            int i = 0;
            for (BoxItem.Info itemInfo : folder) {
                //if (loadCnt > 150) break;
                if (itemInfo instanceof BoxFile.Info) { 
                    String localPath = path;
                    BoxFile boxFile = new BoxFile(api, itemInfo.getID());
                    BoxFile.Info fileInfo = boxFile.getInfo();
                    long start = System.currentTimeMillis();
                    LoadStatus status = downloadFile(boxFile, localPath);
                    long tmpcnt = typeCnt.containsKey(status) ? typeCnt.get(status) : 0;
                    typeCnt.put(status, tmpcnt + 1);
                    long end = System.currentTimeMillis();
                    long durMs = end - start;
    
                    loadCnt++;
                    
                    if (status == LoadStatus.ok) {
                        loadTimeMs += durMs;
                        loadSize += fileInfo.getSize();
                    } else {
                        failCnt++;
                    }
                    
                    logger.logMessage(status
                            + "|" + fileInfo.getName()
                            + "|" + fileInfo.getSize()
                            + "|sha1|" + fileInfo.getSha1()
                            + "|" + durMs,
                        8, true);
                    if ((loadCnt % dumpFreq) == 0)
                        dumpState();
                    
                } else if (itemInfo instanceof BoxFolder.Info) {
                    String localPath = path;
                    BoxFolder.Info folderInfo = (BoxFolder.Info) itemInfo;
                    localPath = localPath + itemInfo.getName();
                    File dir = addDir(localPath);
                    localPath = localPath + "/";
                    String folderId = folderInfo.getID();
                    System.out.println("dump folder - "
                            + " - folderId:" + folderId
                            + " - localPath:" + localPath
                    );
                    processItem(api, folderId, localPath, logger);
                }
            }
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            ex.printStackTrace();
            
        }
    }
    
    public void dumpState()
    {
        if (loadTimeMs == 0) {
            logger.logMessage("state(" + loadCnt + ") "
                + " - zero loadTimeMs",
                5, true);
            return;
        }
        double bytePerMs = 0;
        double loadSizeD = loadSize;
        double loadTimeMsD = loadTimeMs;
        bytePerMs = loadSizeD/loadTimeMsD;
        logger.logMessage("state(" + loadCnt + ") "
                + " - loadTimeMs:" + loadTimeMs
                + " - loadSize:" + loadSize
                + " - bytePerMs:" + bytePerMs,
            3, true);
        Set<LoadStatus> keys = typeCnt.keySet();
        for (LoadStatus key : keys) {
            String msg = "status counts = " + key.name() + "=" + typeCnt.get(key);
            logger.logMessage(msg, 3, true);
        }
        
    }
    
    protected static File addDir(String localPath)
        throws TException
    {
        try {
            File dir = new File(localPath);
            if (!dir.exists() ) {
                System.out.println("create dir:" + localPath);
                Files.createDirectory(Paths.get(localPath));
            }
            return dir;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    
    private LoadStatus downloadFile(BoxFile boxFile, String path)
        throws Exception
    {
        try {
            BoxFile.Info fileInfo = boxFile.getInfo();
            String outPath = path + fileInfo.getName();
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
            throw ex;
            
        }
    }
    
    /**
     * set local logger to node/log/...
     * @param path String path to node
     * @return Node logger
     * @throws Exception process exception
     */
    protected LoggerInf setLogger(String path)
        throws Exception
    {
        
        Properties logprop = new Properties();
        String name = "box";
        logprop.setProperty("fileLogger.message.maximumLevel", "9");
        logprop.setProperty("fileLogger.error.maximumLevel", "10" );
        logprop.setProperty("fileLogger.name", name);
        logprop.setProperty("fileLogger.qualifier", "yyMMdd");
        if (StringUtil.isEmpty(path)) {
            throw new TException.INVALID_OR_MISSING_PARM("setCANLog: path not supplied");
        }

        File canFile = new File(path);
        File log = new File(canFile, "logs");
        if (!log.exists()) log.mkdir();
        String logPath = log.getCanonicalPath() + '/';
        
        System.out.println(PropertiesUtil.dumpProperties("LOG", logprop)
            + "\npath:" + path
            + "\nlogpath:" + logPath
        );
        LoggerInf logger = LoggerAbs.getTFileLogger(name, log.getCanonicalPath() + '/', logprop);
        return logger;
    }

    public String getPrivateAccess() {
        return privateAccess;
    }

    public String getDownloadURL() {
        return downloadURL;
    }

    public String getDownloadDirS() {
        return downloadDirS;
    }

    public LoggerInf getLogger() {
        return logger;
    }
    
}
