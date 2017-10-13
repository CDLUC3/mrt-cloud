/******************************************************************************
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
*******************************************************************************/
package org.cdlib.mrt.cloud.main;

import java.io.File;
import java.util.Properties;
import org.cdlib.mrt.cloud.object.AWSRestoreList;
import org.cdlib.mrt.utility.GetProp;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.TFrame;

/**
 * Run fixity
 * @author dloy
 */
public class RunRestore
{

    protected static final String NAME = "RunRestore";
    protected static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    protected LoggerInf logger = null;
    protected String nodeName = null;
    protected File listFile = null;
    
    protected long inNode = 0;
    protected long outNode = 0;
    protected long skipCnt = 0;
    protected long stopAfterCnt = 0;
    protected AWSRestoreList restoreList = null;
    
    public enum ResetAction {convert, match, error, end};
    
    
    public static void main(String args[])
    {

        TFrame tFrame = null;
        try {
            String propertyList[] = {
                "resources/RunRestore.properties"};
            tFrame = new TFrame(propertyList, "FlipStorageClass");
            Properties runProp  = tFrame.getProperties();
            RunRestore runRestore =  RunRestore.getRunRestore(runProp);
            runRestore.process();

        } catch(Exception e) {
                e.printStackTrace();
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } 
    }
    
    public  RunRestore(Properties runProp)
        throws TException
    {
        try {
            System.out.println(PropertiesUtil.dumpProperties(NAME, runProp));
            String logName = GetProp.getEx(runProp, "logName");
            logger = new TFileLogger(logName, "logs/", runProp);
            String nodeName = GetProp.getEx(runProp, "nodeName");
            String listFileS = GetProp.getEx(runProp, "listFile");
            File listFile = new File(listFileS);
            if (!listFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "listFile does not exist:" + listFileS);
            }
            inNode = GetProp.getNumLongEx(runProp, "inNode");
            outNode = GetProp.getNumLongEx(runProp, "outNode");
            skipCnt = GetProp.getNumLongEx(runProp, "skip");
            stopAfterCnt = GetProp.getNumLongEx(runProp, "stop");
            restoreList = new AWSRestoreList(
                    nodeName,
                    listFile,
                    inNode,
                    outNode,
                    logger,
                    skipCnt,
                    stopAfterCnt);
            
            //service.shutdown();
        } catch(TException tex) {
                tex.printStackTrace();
                throw tex;

        } catch(Exception ex) {
                ex.printStackTrace();
                throw new TException(ex);
        }
    }
    
    public void process()
        throws TException
    {
        restoreList.process();
    }
    
    public static RunRestore getRunRestore(Properties runProp)
        throws TException
    {
        return new RunRestore(runProp);
    }
}

