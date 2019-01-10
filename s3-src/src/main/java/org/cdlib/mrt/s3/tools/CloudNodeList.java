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
**********************************************************/
package org.cdlib.mrt.s3.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import org.cdlib.mrt.utility.TException;
import java.io.PrintWriter;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;
/**
 *
 * @author replic
 */
public class CloudNodeList 
{
    private static final String NAME = "CloudNodeList";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    
 
    private LoggerInf logger = null;
    private File testDir = null;
    private File runFile = null;
    private BufferedReader reader = null;
    private PrintWriter writer = null;
    
    
            
    public static void main(String[] args)
        throws TException
    {
        
        LoggerInf logger = new TFileLogger("tcloud", 0, 50);
        String testDirS = "/apps/replic/test/minio/181213-inittest";
        String testName = "inittest";
        
        CloudNodeList cloudNodeList = getCloudNodeList(
            testDirS, 
            testName,
            logger);
        try {
            if (true) cloudNodeList.run();

        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    public static CloudNodeList getCloudNodeList(
            String testDirS, 
            String testName,
            LoggerInf logger)
        throws TException
    {
        return new CloudNodeList(testDirS, testName, logger);
    }
    protected CloudNodeList(
            String testDirS, 
            String testName,
            LoggerInf logger) 
        throws TException
    {
        try {
            this.logger = logger;
            setEnv(testDirS, testName);
 
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected void setEnv(
            String testDirS, 
            String testName)
        throws TException
    {
        try {
            System.out.println("**********\nCloudNodeList\n"
                + " - testDirS=" + testDirS + "\n"
                + " - testName=" + testName + "\n"
                + "**********\n"
            );
            
            
            if (StringUtil.isAllBlank(testDirS)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "testDirS not supplied");
            }
            testDir = new File(testDirS);
            if (!testDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "testDirS not found:" + testDirS);
            }
            if (StringUtil.isAllBlank(testName)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "testName not supplied");
            }
            String processName = "./in/" + testName + ".txt";
            runFile = new File(testDir, processName);
            
            if (!runFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "runFile not found:" + runFile.getCanonicalPath());
            }
            
            reader = new BufferedReader(new FileReader(runFile));
            
            String outName = "./out/" + "out." + testName + ".txt";
            File outFile = new File(testDir, outName);
            outFile.createNewFile();
            writer = new PrintWriter(outFile, "UTF-8");
            writer.println("*****" + testName + "*****");
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
          

    public void run()
            throws TException
    {
       
        try {
            String readLine = "";

            while ((readLine = reader.readLine()) != null) {
                if (readLine.length() < 8) continue;
                if (readLine.substring(0,1).equals("#"))  continue;
                List<CloudNodeTest.Test> processList = process(readLine);
                if (processList == null) {
                    System.out.println("***ProcessList null");
                    continue;
                }
                setResults(readLine, processList);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            
        } finally {
            try {
                writer.close();
            } catch (Exception ex) { }
        }
    }
    
    private List<CloudNodeTest.Test> process(String readLine)
        throws TException
    {
        try {
            //System.out.println("PROCESS:" + readLine);
            String [] parts = readLine.split("\\s*\\|\\s*");
            if (parts.length != 4) {
                System.out.println("Invalid line:" + readLine);
                return null;
            }
            String nodeIOName = parts[0]; 
            String nodeNums = parts[1]; 
            String keyName = parts[2]; 
            String dataName = parts[3]; 
            System.out.println("CloudNodeList:"
                    + " - nodeIOName=" + nodeIOName
                    + " - nodeNums=" + nodeNums
                    + " - keyName=" + keyName
                    + " - dataName=" + dataName
            );
            CloudNodeTest cloudNodeTest = CloudNodeTest.getCloudNodeTest(
                testDir, 
                nodeIOName, 
                nodeNums, 
                keyName, 
                dataName,
                logger);
            cloudNodeTest.runTest();
            return cloudNodeTest.getTests();
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    
    public void setResults(String readLine, List<CloudNodeTest.Test> processList) 
        throws TException
    {    try {;
            if (processList == null) return;
            writer.println("\n***" + readLine);
            for (CloudNodeTest.Test entry : processList) {
                String outLine = entry.nodeNumber 
                        + " | " + entry.operation
                        + " | " + entry.ok
                        + " | " + entry.durration;
                if (entry.exception != null) {
                    outLine += "|" + entry.exception.toString();
                }
                writer.println(outLine);
            }
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
}
