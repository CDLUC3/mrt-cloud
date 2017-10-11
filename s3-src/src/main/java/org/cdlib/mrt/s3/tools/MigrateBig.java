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
package org.cdlib.mrt.s3.tools;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.io.IOException;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
/**
 * 
 *
 * @author replic
 */
public class MigrateBig 
{
    protected static final String NAME = "MigrateBig";
    protected static final String MESSAGE = NAME + ": ";
    //protected static final String testFileS = "/apps/replic/tomcat-28080/webapps/test/big/producer/AS020-VTQ0173-M.mp4";
 
    
    protected String nodeName = null;
    protected LoggerInf logger = null;
    
    public MigrateBig(String nodeName, LoggerInf logger)
        throws TException
    {
        this.nodeName = nodeName;
        this.logger = logger;
    }
    
    public void process(int nodeIn, int nodeOut, File inFile, File testFile, String key, long size, String digestValue)
        throws TException
    {
        try {
            processIn(nodeIn, key, inFile);
            validate("IN", inFile, key, size, digestValue);
            processOut(nodeOut, key, inFile, testFile);
            validate("OUT", testFile, key, size, digestValue);
                
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public void processIn(int nodeIn, String key, File inFile)
        throws TException
    {
        NodeService service = NodeService.getNodeService(nodeName, nodeIn, logger);
        try {
            System.out.println("Process"
                +   " - bucket:" + service.getBucket()
                +   " - key:" + key
                +   " - inFile:" + inFile.getCanonicalPath()
            );
            CloudResponse response = new CloudResponse(service.getBucket(), key);
            service.getObject(key, inFile, response);
            if (response.getException() != null) {
                throw response.getException();
            }
        
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public void validate(String header, File validateFile, String key, long testSize, String testChecksum)
        throws TException
    {
        try {
            
            System.out.println("TEST " + header + '\n'
                +   " - key:" + key + '\n'
                +   " - validateFile:" + validateFile.getCanonicalPath() + '\n'
                +   " - testSize:" + testSize + '\n'
                +   " - testChecksum:" + testChecksum + '\n'
            );
            FixityTests fixity = new FixityTests(validateFile, "sha256", logger);
            System.out.println("FIXITY " + header + '\n'
                +   " - key:" + key + '\n'
                +   " - fixitySize:" + fixity.getInputSize() + '\n'
                +   " - fixityChecksum:" + fixity.getChecksum() + '\n'
            );
            if (testSize != fixity.getInputSize()) {
                throw new TException.INVALID_DATA_FORMAT("Fixity size failure:"
                    +   " - key:" + key
                    +   " - testSize:" + testSize + '\n'
                    +   " - testChecksum:" + testChecksum + '\n'
                    +   " - fixitySize:" + fixity.getInputSize() + '\n'
                    +   " - fixityChecksum:" + fixity.getChecksum() + '\n'
                );
            }
            if (!testChecksum.equals(fixity.getChecksum())) {
                throw new TException.INVALID_DATA_FORMAT("Fixity checksum failure:"
                    +   " - key:" + key
                    +   " - testSize:" + testSize + '\n'
                    +   " - testChecksum:" + testChecksum + '\n'
                    +   " - fixitySize:" + fixity.getInputSize() + '\n'
                    +   " - fixityChecksum:" + fixity.getChecksum() + '\n'
                );
            }
            System.out.println("***" + header + " - MATCH - key=" + key);
        
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public void processOut(int nodeOut, String key, File inFile, File testFile)
        throws TException
    {
        NodeService service = NodeService.getNodeService(nodeName, nodeOut, logger);
        try {
            System.out.println("ProcessOut"
                +   " - bucket:" + service.getBucket()
                +   " - nodeOut:"  + nodeOut
                +   " - key:" + key
                +   " - inFile:" + inFile.getCanonicalPath()
                +   " - testFile:" + testFile.getCanonicalPath()
            );
            CloudResponse response = service.deleteObject(key);
            System.out.println("***DELETE - key=" + key);
            response = service.putObject(key, inFile);
            if (response.getException() != null) {
                throw response.getException();
            }
            System.out.println("***POST - key=" + key);
            service.getObject(key, testFile, response);
            if (response.getException() != null) {
                throw response.getException();
            }
            System.out.println("***REPLACE - key=" + key);
                
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    
    public static void main(String[] args) 
            throws IOException,TException 
    {
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        String NODE_NAME = "nodes-prod-class";
        String IN_FILE
            = "/apps/replic/test/aws/testbig/class-file.txt";
        File inFile = new File(IN_FILE);
        String TEST_FILE
            = "/apps/replic/test/aws/testbig/class-test.txt";
        File testFile = new File(TEST_FILE);
        int IN_NODE = 5001;
        int OUT_NODE = 4001;
        MigrateBig migrateBig = new MigrateBig(NODE_NAME, logger);
        
        //String KEY = "ark:/13030/qt6tx194sd|3|producer/content/supp/HWyckoffKF373.W9A35access.wav";
        String KEY = "ark:/13030/m5k695dk|1|producer/udfr.tar.gz";
        long SIZE = 9188553797L;
        String DIGEST = "c1ec51eae9993198458d0db2bd4b833fc8d2b535833cf4bcc617e4c9f4225275";
        
        try {
            migrateBig.process(IN_NODE, OUT_NODE, inFile, testFile, KEY, SIZE, DIGEST);
            //migrateBig.process(NODE_NUMBER, KEY, SIZE, DIGEST);
            migrateBig.validate("IN", inFile, KEY, SIZE, DIGEST);
            migrateBig.processOut(OUT_NODE, KEY, inFile, testFile);
            migrateBig.validate("OUT", testFile, KEY, SIZE, DIGEST);
                
        } catch (Exception ex) {
            System.out.println(">>>Exception:" + ex) ;
        }
    }
    
    
    public static void main_save(String[] args) 
            throws IOException,TException 
    {
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        String NODE_NAME = "nodes-prod-class";
        String IN_FILE
            = "/apps/replic/test/aws/testbig/class-file.txt";
        File inFile = new File(IN_FILE);
        String TEST_FILE
            = "/apps/replic/test/aws/testbig/class-test.txt";
        File testFile = new File(TEST_FILE);
        int IN_NODE = 5001;
        int OUT_NODE = 4001;
        MigrateBig migrateBig = new MigrateBig(NODE_NAME, logger);
        String KEY = "ark:/13030/qt2zw5t11r|4|producer/content/supp/CardiffF868.S3C3access.wav";
        long SIZE = 7729299536L;
        String DIGEST = "7ae4f58b4ac3896b89e3192b5de0b353deb4b4781914472c69ab6cbbdeb19870";
        try {
            //migrateBig.process(NODE_NUMBER, KEY, SIZE, DIGEST);
            migrateBig.validate("IN", inFile, KEY, SIZE, DIGEST);
            migrateBig.processOut(OUT_NODE, KEY, inFile, testFile);
            migrateBig.validate("OUT", testFile, KEY, SIZE, DIGEST);
                
        } catch (Exception ex) {
            System.out.println(">>>Exception:" + ex) ;
        }
    }
}

