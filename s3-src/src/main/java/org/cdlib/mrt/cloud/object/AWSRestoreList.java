/*
Copyright (c) 2005-2018, Regents of the University of California
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
package org.cdlib.mrt.cloud.object;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.tools.CloudManifestCopy;
import org.cdlib.mrt.s3.tools.CloudObjectList;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.MessageDigestValue;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
/**
 * 
 *
 * @author replic
 */
public class AWSRestoreList 
{
    protected static final String NAME = "AWSRestoreList";
    protected static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    protected static final String tempFileS = "/apps/replic/test/aws/timing/ark1.txt";
    
    protected BufferedReader br = null;
    protected final LoggerInf logger;
    protected final File listFile;
    protected final String nodeName;
    protected final CloudManifestCopy cmc;
    protected AWSRestoreObject restoreObject = null;
    protected String inContainer = null;
    protected long inCnt = 0;
    protected long restoreCnt = 0;
    protected long copyCnt = 0;
    protected long missCnt = 0;
    protected long doneCnt = 0;
    protected long exCnt = 0;
    protected long skipCnt = 0;
    protected long stopAfterCnt = 0;
            
    public enum RestoreAction {copied, restore, missing, complete, error, end};
    
    public AWSRestoreList(
            String nodeName,
            File listFile,
            long inNode,
            long outNode,
            LoggerInf logger,
            long skipCnt,
            long stopAfterCnt)
        throws TException
    {
         try {
            this.listFile = listFile;
            this.nodeName = nodeName;
            this.logger = logger;
            this.skipCnt = skipCnt;
            this.stopAfterCnt = stopAfterCnt;
            
            FileInputStream fis = new FileInputStream(this.listFile);
            br = new BufferedReader(new InputStreamReader(fis,
                    Charset.forName("UTF-8")));
            cmc = CloudManifestCopy.getCloudManifestCopy(nodeName, inNode, outNode, logger);
            inContainer = cmc.getInContainer();
            restoreObject = new AWSRestoreObject(inContainer, logger);
            String msg =  MESSAGE + "START\n"
                    + " - listFile:" + listFile.getAbsolutePath() + "\n"
                    + " - nodeName:" + nodeName + "\n"
                    + " - inContainer:" + inContainer + "\n"
                    + " - skipCnt:" + skipCnt + "\n"
                    + " - stopAfterCnt:" + stopAfterCnt + "\n";
            System.out.println(msg);
            logger.logMessage(msg, 1, true);
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
    }
        
    public void process()
        throws TException
    {
        String line = null;
        RestoreAction restoreAction = null;
        try {
            while ((line = br.readLine()) != null) {
                if (StringUtil.isAllBlank(line)) continue;
                if (line.startsWith("#")) continue;
                if (line.length() < 4) continue;
                inCnt++;
                if (inCnt <= skipCnt) {
                    continue;
                } 
                if (inCnt > stopAfterCnt) break;
                restoreAction = copy(line);
                switch (restoreAction) {
                    case copied:
                        copyCnt++;
                        break;
                    case restore:
                        restoreCnt++;
                        break;
                    case missing:
                        missCnt++;
                        break;
                    case complete:
                        doneCnt++;
                        break;
                    case error:
                        exCnt++;
                        break;
                } 
                String msg = "Action(" + line + "):" + restoreAction;
                logger.logMessage(msg, 2, true);
                if ((inCnt % 200) == 0) {
                    dump("PARTIAL:" + line);
                }
            }
            dump("***FINAL");
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        } finally {
             try {
                 br.close();
             } catch (Exception ex) { }
        }
        
    }
    
    protected boolean done(String arkS)
            throws TException
    {    
        long timeStart = DateUtil.getEpochUTCDate();
        arkS = arkS.trim();
        if (DEBUG) System.out.println("***Process:" + arkS + "<<");
        try {
            CloudStoreInf outService = cmc.getOutService();
            String outContainer = cmc.getOutContainer();
            String manKey = arkS + "|manifest";
            Properties manProp = outService.getObjectMeta(outContainer, manKey);
            if ((manProp != null) && (manProp.size() > 0)) {
                return true;
            } else {
                return false;
            }
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            return false;
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            return false;
        }
    }
    
    protected RestoreAction copy(String arkS)
            throws TException
    {    
        long timeStart = DateUtil.getEpochUTCDate();
        arkS = arkS.trim();
        if (done(arkS)) {
            return RestoreAction.complete;
        }
        if (DEBUG) System.out.println("***Process:" + arkS + "<<");
        try {
            Identifier ark = new Identifier(arkS);
            Integer restored = restoreObject.restore(ark);
            if (restored == null) {
                if (DEBUG) System.out.println(MESSAGE + "ark not found:" + ark);
                return RestoreAction.missing;
            }
            if (restored != 0) {
                if (DEBUG) System.out.println(MESSAGE + "restore in progress:" + ark);
                return RestoreAction.restore;
            }
            cmc.copyObject(arkS);
            return RestoreAction.copied;
                
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            return RestoreAction.error;
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            return RestoreAction.error;
        }
    }
    
    public void dump(String header) 
    {
            
            String msg =  MESSAGE + header + "\n"
                    + " - listFile:" + listFile.getAbsolutePath() + "\n"
                    + " - nodeName:" + nodeName + "\n"
                    + " - inContainer:" + inContainer + "\n"
                    + " - skipCnt:" + skipCnt + "\n"
                    + " - stopAfterCnt:" + stopAfterCnt + "\n"
                    + " - inCnt:" + inCnt + "\n"
                    + " - restoreCnt:" + restoreCnt + "\n"
                    + " - copyCnt:" + copyCnt + "\n"
                    + " - missCnt:" + missCnt + "\n"
                    + " - doneCnt:" + doneCnt + "\n";
            
            System.out.println(msg);
            logger.logMessage(msg, 1, true);
    }
    
}
