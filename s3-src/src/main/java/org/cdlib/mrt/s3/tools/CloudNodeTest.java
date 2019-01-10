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

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.TException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.s3.service.NodeService;
/**
 *
 * @author replic
 */
public class CloudNodeTest 
{
    private static final String NAME = "CloudNodeTest";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    
 
    //private String nodeName = null;
    private boolean DEBUG_STANDALONE = false;
    private NodeIO nodeIO = null;
    private LoggerInf logger = null;
    private File testDir = null;
    private String testName = null;
    private File dataFile = null;
    private String key = null;
    private ArrayList<Long> nodes = new ArrayList<>();
    private Properties runProp = null;
    private ArrayList<Test> tests = new ArrayList<>();
    
    
            
    public static void main(String[] args)
        throws TException
    {
        
        LoggerInf logger = new TFileLogger("tcloud", 0, 50);
        String testDirS = "/apps/replic/test/minio/181213-inittest";
        String nodeIOName = "nodes-test-minio";
        //String nodeNums = "5001;9001;9501";
        //String nodeNums = "9001";
        String nodeNums = "9501";
        //String keyName = "ascii";
        String dataName = "big";
        //String keyName = "utf8";
        String keyName = "utf8";
        //String dataName = "big";
            if (StringUtil.isAllBlank(testDirS)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Properties name not supplied");
            }
        File testDir = new File(testDirS);
        if (!testDir.exists()) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Properties name not found:" 
                    + testDirS);
        }
        CloudNodeTest testCloudNode = getCloudNodeTest(
            testDir, 
            nodeIOName, 
            nodeNums, 
            keyName, 
            dataName,
            logger);
        try {
            testCloudNode.runTest();

        } catch (TException tex) {
            tex.printStackTrace();
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    public static CloudNodeTest getCloudNodeTest(
            File testDir, 
            String nodeIOName, 
            String nodeNums, 
            String keyName, 
            String dataName,
            LoggerInf logger)
        throws TException
    {
        return new CloudNodeTest(testDir, 
                nodeIOName,
                nodeNums,
                keyName,
                dataName,
                logger);
    }
    protected CloudNodeTest(
            File testDir, 
            String nodeIOName, 
            String nodeNums, 
            String keyName, 
            String dataName,
            LoggerInf logger) 
        throws TException
    {
        try {
            this.testDir = testDir;
            this.logger = logger;
            setEnv(nodeIOName, nodeNums, keyName, dataName);
            //setProcess(testDir, testName, runProp, logger);
 
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected void setEnv(
            String nodeIOName, 
            String nodeNums, 
            String keyName, 
            String dataName)
        throws TException
    {
        try {
            if (DEBUG_STANDALONE) System.out.println("**********\nTestcloudNode\n"
                + " - testDir=" + testDir.getCanonicalPath() + "\n"
                + " - nodeIOName=" + nodeIOName + "\n"
                + " - nodeNums=" + nodeNums + "\n"
                + " - nodeIOName=" + nodeIOName + "\n"
                + " - keyName=" + keyName + "\n"
                + " - dataName=" + dataName + "\n"
                + "**********\n"
            );
            if (testDir == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "testDir not supplied");
            }
            String propFileS = "All.properties";
            File propFile = new File(testDir, propFileS);
            if (!propFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE 
                        + "Properties name not found:" + propFileS
                        + " - canonical:" + testDir.getCanonicalPath()
                );
            }
            runProp = PropertiesUtil.loadFileProperties(propFile);
            PropertiesUtil.dumpProperties("TEST", runProp);
            
            // set nodeIO
            if (StringUtil.isAllBlank(nodeIOName)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeIO name not supplied");
            }
            nodeIO = NodeIO.getNodeIO(nodeIOName, logger);
            if (nodeIO == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE 
                        + "NodeIO name not found:" + nodeIOName);
            }
            
            // set key
            if (StringUtil.isAllBlank(keyName)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "keyName not supplied");
            }
            key = runProp.getProperty("KEY." + keyName);
            if (StringUtil.isAllBlank(key)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "key not found in properties:" + key);
            }
            
            //set nodes
            
            if (StringUtil.isAllBlank(nodeNums)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeNums not supplied");
            }
            String [] nodesS = nodeNums.split("\\s*\\;\\s*");
            for (String nodeVal : nodesS) {
                Long node = null;
                try {
                    node = Long.parseLong(nodeVal);
                } catch (Exception ex) {
                    throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Node not valid:" + nodeVal);
                }
                nodes.add(node);
            }
            if (nodes.size() == 0) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "No nodes found:" + nodeNums);
            }
            
            //data name
            if (StringUtil.isAllBlank(dataName)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "dataName name not supplied");
            }
            String dataFileS = runProp.getProperty("DATA." + dataName);
            if (StringUtil.isAllBlank(dataFileS)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "key not found in properties:" + key);
            }
            dataFile = new File(testDir, dataFileS);
            
            if (DEBUG) System.out.println("**********\nProcess\n"
                + PropertiesUtil.dumpProperties("runProp", runProp)+ "\n"
                + " - nodeIOName=" + nodeIOName + "\n"
                + " - key=" + key + "\n"
                + " - dataFile=" + dataFile.getCanonicalPath() + "\n"
                + "**********\n"
            );
            for (long nodeVal : nodes) {
               if (DEBUG) System.out.println("node=" + nodeVal);
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
          

    public void runTest()
            throws TException
    {
       
        try {
            for (long nodeNum : nodes) {
                testService(nodeNum);
            }
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }

    public void testService(long nodeNum) 
        throws TException
    {
      
        NodeService service = null;
        
        try {
            if (DEBUG_STANDALONE) {
                System.out.println("\n*************************************");
                System.out.println("Test:"
                    + " - nodeNum=" + nodeNum + "\n"
                    + " - key=" + key + "\n"
                    + " - dataFile=" + dataFile.getCanonicalPath()
                );
                System.out.println("*************************************\n");
            }
            service = NodeService.getNodeService(nodeIO, nodeNum, logger);
            if (service == null) {
                System.out.println("***Service not found:" 
                        + " - nodeNum=" + nodeNum
                );
                return;
            }
            
            Boolean isAlive = testIsAlive(service);
            if (!isAlive) {
                System.out.println("***Service not available:"
                        + " - nodeNum=" + nodeNum
                );
                return;
            }
            if (false) return;
            if (!testServiceState(service)) return;
            testDeleteState(service);
            if (!testAddState(service)) return;
            if (!testMetadataState(service)) return;
            if (!testFixityState(service)) return;
            if (true) return;
            /*
            testMetadataState(service, key, logger);
            testDeleteState(service, key, logger);
            testMetadataState(service, key, logger);
            testAddState(service, key, testFile, logger);
            testMetadataState(service, key, logger);
            testData(service, logger);
            testServiceState(logger);
            testDeleteState(service, logger);
            testMetadataState(service, logger);
            testAddState(service, logger);
            testMetadataState(service, logger);
            testData(service, logger);
            testFixityState(logger);
                    */

        } catch (TException tex) {
            tex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
            
        } finally {
            //testDeleteState(service);
        }
    }      
    
    public Boolean testServiceState(NodeService service)
        throws TException
    {
        Test serviceTest = getTest(service, "State");
        if (DEBUG) System.out.println("\n\n>>>testServiceState>>>\n");
        try {
            StateHandler.RetState retstat = service.getState();
            if (DEBUG) System.out.println(retstat.dump(service.getBucket()));
            serviceTest.ok = retstat.getOk();
            
            
        } catch (Exception ex) {
            ex.printStackTrace();
            serviceTest.exception = ex;
            
        } finally {
            return retTest(serviceTest.set());
        }
    }     
    
    public Boolean testIsAlive(NodeService service)
        throws TException
    {
        if (DEBUG) System.out.println("\n\n>>>testIsAlive>>>\n");
        try {
            Boolean isAlive = service.isAlive();
            if (DEBUG) System.out.println("***Alive:" + isAlive);
            if (isAlive == null) return true;
            return isAlive;
            
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
        
    public Boolean testMetadataState(NodeService service)
        throws TException
    {
        if (DEBUG) System.out.println("\n\n>>>testMetadataState>>>\n");
        Test serviceTest = getTest(service, "Meta");
        try {
            Properties prop = service.getObjectMeta(key);
            if (prop == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("testMetadataState prop null");
            }
            if (prop.size() == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("testMetadataState prop empty");
            }
            serviceTest.ok = true;
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("testMetadataState", prop));
            String sizeS = prop.getProperty("size");
            if (sizeS != null) {
                serviceTest.size = Long.parseLong(sizeS);
            }
            serviceTest.sha256 = prop.getProperty("sha256");
            
        } catch (Exception ex) {
            ex.printStackTrace();
            serviceTest.exception = ex;
            
        } finally {
            return retTest(serviceTest.set());
        }
    }
        
    public Boolean testAddState(NodeService service)
        throws TException
    {
        Test serviceTest = getTest(service, "Add");
        try {
            if (DEBUG) System.out.println("\n\n>>>testAddState>>>\n");
            //String key = "ark:/88888/trythisoutagain|1|producer|test.pdf";
            if (DEBUG) System.out.println("Name:" + dataFile.getAbsolutePath());
            if (DEBUG) System.out.println("Key:" + key);
            if (DEBUG) System.out.println("testFile.Exists:" + dataFile.exists());
            if (DEBUG) System.out.println("testFile.Length:" + dataFile.length());
            CloudResponse response =  service.putObject(key, dataFile);
            if (DEBUG) System.out.println("Add status:" + response.getStatus().toString());
            serviceTest.setOK(response);
            serviceTest.size = dataFile.length();
            
            
        } catch (Exception ex) {
            ex.printStackTrace();
            serviceTest.exception = ex;
            
        } finally {
            return retTest(serviceTest.set());
        }
    }
        
    public Boolean testGet(NodeService service, File outFile)
        throws TException
    {
        
        Test serviceTest = getTest(service, "Get");
        try {
            CloudResponse response = CloudResponse.get(service.getBucket(), key);
            service.getObject(key, outFile, response);
            if (DEBUG) System.out.println("testGet:\n"
                    + " - name:" + outFile.getCanonicalPath() + "\n"
                    + " - length:" + outFile.length() + "\n"
            );
            serviceTest.size = outFile.length();
            if (outFile.length() == dataFile.length()) {
                serviceTest.ok = true;
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
            serviceTest.exception = ex;
            
        } finally {
            return retTest(serviceTest.set());
        }
    }
        
    public Boolean testDeleteState(NodeService service)
        throws TException
    {
        if (DEBUG) System.out.println("\n\n>>>testDeleteState>>>\n");
        Test serviceTest = getTest(service, "Delete");
        try {
            CloudResponse response =  service.deleteObject(key);
            //System.out.println(response.dump("***DELETE***"));
            serviceTest.setOK(response);
            if (DEBUG) System.out.println("Delete status:" + response.getStatus().toString());
            //serviceTest.ok = response.
            
        } catch (Exception ex) {
            ex.printStackTrace();
            serviceTest.exception = ex;
            
        } finally {
            return retTest(serviceTest.set());
        }
    }
        
    public boolean testFixityState(NodeService service)
        throws TException
    {
        if (DEBUG) System.out.println("\n\n>>>testFixityState>>>\n");
        Test serviceTest = getTest(service, "Fixity");
        File outFile = FileUtil.getTempFile("tmpFixity", ".txt");
        try {
            Properties prop = service.getObjectMeta(key);
            if (prop == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("testMetadataState prop null");
            }
            if (prop.size() == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("testMetadataState prop empty");
            }
            String dataFileSha256 = CloudUtil.getDigestValue("sha256", dataFile, logger);
            long dataFileSize = dataFile.length();
            String digestType = "sha256";
            if (!testGet(service, outFile)) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Unable to get test file");
            }
            if (DEBUG) System.out.println("outFile length=" + outFile.length());
            FixityTests fixTests = new FixityTests(outFile, digestType, logger);
            FixityTests.FixityResult result = fixTests.validateSizeChecksum(dataFileSha256, digestType, dataFileSize);
            if (result.checksumMatch && result.fileSizeMatch) {
                serviceTest.ok = true;
            }
            serviceTest.size = fixTests.getInputSize();
            serviceTest.sha256 = fixTests.getChecksum();
            if (DEBUG) System.out.println(result.dump("result"));
            
        } catch (Exception ex) {
            serviceTest.exception = ex;
            
        } finally {
            try {
                if (outFile != null) {
                    outFile.delete();
                }
            } catch (Exception ex) {}
            return retTest(serviceTest.set());
        }
    }
    
    boolean retTest(Test serviceTest) 
    {
        boolean ok = true;
        if (serviceTest == null) return false;
        if (serviceTest.exception != null) ok = false;
        if (!serviceTest.ok) ok = false;
        if (DEBUG_STANDALONE) System.out.println(serviceTest.dump(">>>>retTest")
        );
        tests.add(serviceTest.set());
        return ok;
    }
    
    public static File format(StateInf responseState, LoggerInf logger)
        throws TException
    {
           String str = formatANVL(responseState, logger);
           System.out.println("*** ANVL ***\n" + str);
           File tmp = FileUtil.getTempFile("anvl", ".properties");
           FileUtil.string2File(tmp, str);
           return tmp;
    }
    
    public static String formatXML(StateInf responseState, LoggerInf logger)
        throws TException
    {
        
           FormatterInf xml = FormatterAbs.getXMLFormatter(logger);
           return formatIt(xml, responseState);
    }
    
    public static String formatJSON(StateInf responseState, LoggerInf logger)
        throws TException
    {
        
           FormatterInf xml = FormatterAbs.getJSONFormatter(logger);
           return formatIt(xml, responseState);
    }
    
    public static String formatANVL(StateInf responseState, LoggerInf logger)
        throws TException
    {
        
           FormatterInf xml = FormatterAbs.getANVLFormatter(logger);
           return formatIt(xml, responseState);
    }

    public static String formatIt(
            FormatterInf formatter,
            StateInf responseState)
    {
        try {
           ByteArrayOutputStream outStream = new ByteArrayOutputStream(5000);
           PrintStream  stream = new PrintStream(outStream, true, "utf-8");
           formatter.format(responseState, stream);
           stream.close();
           byte [] bytes = outStream.toByteArray();
           String retString = new String(bytes, "UTF-8");
           return retString;

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            System.out.println("Trace:" + StringUtil.stackTrace(ex));
            return null;
        }
    }
    
    private Test getTest(NodeService service, String operation)
    {
        Test test = new Test();
        test.serviceType = service.getServiceType();
        test.nodeIOName = service.getNodeName();
        test.bucket = service.getBucket();
        test.nodeNumber = service.getNode();
        test.operation = operation;
        return test;
    }
    public static class Test {
        public Boolean ok = false;
        public String serviceType = null;
        public String nodeIOName = null;
        public String bucket = null;
        public Long nodeNumber = null;
        public String operation = null;
        public Exception exception = null;
        public Long start = System.nanoTime();
        public Long durration = 0L;
        public Long size = null;
        public String sha256 = null;
        
        public String dump(String header)
        {
            String retVal = header
                    + " - nodeIOName=" + nodeIOName
                    + " - nodeNumber=" + nodeNumber
                    + " - operation=" + operation
                    + " - ok=" + ok
                    + " - durration=" + durration
                    ;
            if (size != null) {
                retVal += " - size=" + size;
            }
            if (sha256 != null) {
                retVal += " - sha256=" + sha256;
            }
            return retVal;
        }
        
        public void setOK(CloudResponse response)
        {
            if (DEBUG) System.out.println("Add status:" + response.getStatus().toString());
            switch (response.getStatus()) {
                case fail: 
                    ok = false;
                    break;
                case ok: 
                    ok = true;
                    break;
                default:
                    ok = null;
                    break;
            }
        }

        public Test set() {
            if (start != null) {
                this.durration = System.nanoTime() - start;
            }
            return this;
        }
        
    }

    public ArrayList<Test> getTests() {
        return tests;
    }
    
}
