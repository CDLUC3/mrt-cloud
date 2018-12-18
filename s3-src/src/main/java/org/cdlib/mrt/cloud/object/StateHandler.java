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
package org.cdlib.mrt.cloud.object;
//import org.cdlib.mrt.s3.service.*;



import org.cdlib.mrt.s3.aws.*;
import org.cdlib.mrt.s3.service.NodeService;

import java.io.File;
import java.io.InputStream;
import java.net.Socket;
import java.net.URL;
import java.util.Properties;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.core.MessageDigest;

import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Specific SDSC Storage Cloud handling
 * @author dloy
 */
public class StateHandler
{
    protected static final String NAME = "StateHandler";
    protected static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    
    private static final String testS = "this is a test file\n";
    private static final String checksumType = "md5";
    //private static final String checksum = "62659bdd975c7e7a0857d69dec1e42fe";
    private static final String checksum = "4221d002ceb5d3c9e9137e495ceaa647"; //ok
    private static final long testLength = testS.length();
    public static final String KEY = "ark:/99999/test|1|prod/test";
    private final CloudStoreInf manager;
    private final String bucket;
    private final String key;
    private final LoggerInf logger;
    
    private RetState retState = null;
    
    
    protected FileContent fileContent = null;
    protected String error = null;
    private int forceTest = 0;
    private long epochDate = 0;
    
    public static StateHandler getStateHandler(
            CloudStoreInf manager,
            String bucket,
            String key,
            LoggerInf logger)
        throws TException
    {
        return new StateHandler(manager, bucket, key, logger);
    }
    
    public static StateHandler getStateHandler(
            CloudStoreInf manager,
            String bucket,
            LoggerInf logger)
        throws TException
    {
        return new StateHandler(manager, bucket, KEY, logger);
    }
    
    protected StateHandler(
            final CloudStoreInf manager,
            final String bucket,
            final String inKey,
            final LoggerInf logger)
        throws TException
    {
        this.manager = manager;
        this.logger = logger;
        this.bucket = bucket;
        epochDate = DateUtil.getEpochUTCDate();
        key = inKey + epochDate;
        retState = new RetState(bucket, key);
        
        if (DEBUG) System.out.println(retState.dump("Input"));
    }

    public RetState process()
        throws TException
    {
        try {
            if (!testIsAlive()) {
                return retState;
            }
            if (false && !initialMeta(key)) {
                return retState;
            }
            if (!add(key)) {
                return retState;
            }
            
            if (DEBUG) meta(key);
            
            if (!content(key)) {
                return retState;
            }
            if (DEBUG && !fixity(key, checksumType, checksum, testLength)) {
                return retState;
            }
            if (!delete(key)) {
                return retState;
            }
            System.out.println("Delete performed:" + key);
            if (forceTest == 5) {
                retState.setOk(false);
                return retState;
            }

            retState.setOk(true);
            long duration = DateUtil.getEpochUTCDate() - epochDate;
            retState.setDuration(duration);
            return retState;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            error = "process exception:" + ex.toString();
            setError(MESSAGE + ex.toString());
            return retState;
        }
    }

    protected boolean setError(String retError)
    {
        retState.setError(retError);
        System.out.println(retError);
        retState.setOk(false);
        return false;
    }
    
    private boolean isError(CloudResponse response)
    {
        if (response.getException() != null) {
            System.out.println("******isError1:" + response.getException().toString());
            response.getException().printStackTrace();
            return true;
        }
        String errMsg = response.getErrMsg();
        if (StringUtil.isAllBlank(errMsg)) {
            return false;
        }
        System.out.println("isError2:" + response.getErrMsg());
        return true;
    }
    
    protected boolean testIsAlive() 
        throws TException
    {
        Boolean isAlive = manager.isAlive(bucket);
        if (DEBUG) System.out.println("***" + manager.getType() + "(" + bucket + ")manager.isAlive:" + isAlive
        );
        if (isAlive == null) return true;
        if (isAlive) return true;
        error = "Service tested and not alive"
                                + " - bucket:" + bucket
                                ;
        return setError(error);
    }
    
    protected boolean initialMeta(String key)
        throws TException
    {
        Properties prop = manager.getObjectMeta(bucket, key);
        if (prop == null) {
            return false;
        }
        if (prop.size() == 0) {
            return true;
        }
        try {
            CloudResponse response = manager.deleteObject(bucket, key);
            Exception exception = response.getException();
            if (exception != null) {
                String metaError = exception.toString();
                if (metaError != null) {
                    if (metaError.contains("REQUESTED_ITEM_NOT_FOUND")) {
                        if (DEBUG) System.out.println("initialMeta: No initial delete file");
                    } else {
                        error = "Initial Meta fail - Exception on meta request"
                                + " - bucket:" + bucket
                                + " - key:" + key
                                + " - error:" + metaError
                                ;
                        return setError(error);
                    }
                }
                return false;
            }
            return true;
            
        } catch (Exception ex) {
            error = "InitialMeta Catch Error - Unable to add content"
                        + " - bucket:" + bucket
                        + " - key:" + key
                        + " - error:" + ex.toString();
            
            ex.printStackTrace();
            return setError(error);
                
        }
    }
    
    protected boolean meta(String key)
        throws TException
    {;
        try {    
            Properties prop = manager.getObjectMeta(bucket, key);
            if (prop == null) {
                    error = "Meta fail - Exception on meta request"
                            + " - bucket:" + bucket
                            + " - key:" + key
                            ;
                    return setError(error);
            }
            if (prop.size() == 0) {
                    error = "404 - No meta found for"
                            + " - bucket:" + bucket
                            + " - key:" + key
                            ;
                    return setError(error);
            }
            
            System.out.println(PropertiesUtil.dumpProperties("meta output", prop));
            return true;
            
        } catch (Exception ex) {
            error = "Meta Catch Error - Unable to add content"
                        + " - bucket:" + bucket
                        + " - key:" + key
                        + " - error:" + ex.toString();
            return setError(error);
                
        }
    }
    
    protected boolean fixity(String key, String digestType, String testDigest, long testLength)
        throws TException
    {
        try {
            MessageDigest digest = new MessageDigest(testDigest, digestType);
            CloudResponse response = manager.validateDigest(bucket, key, digest, testLength);
            if (isError(response)) {
                return setError(response.getErrMsg());
            };
            return response.isMatch();
            
        } catch (Exception ex) {
            error = "Fixity Catch Error - Unable to add content"
                        + " - bucket:" + bucket
                        + " - key:" + key
                        + " - error:" + ex.toString();
            return setError(error);
                
        }
    }
        
    protected boolean add(String key) 
        throws TException
    {
        File testF = FileUtil.getTempFile("test", ".txt");
        try {
            FileUtil.string2File(testF, testS);
            
            CloudResponse response = manager.putObject(bucket, key, testF);
            if (isError(response)) {
                return setError(response.getErrMsg());
            };
            return true;
            
        } catch (Exception ex) {
            error = "Add Catch Error - Unable to add content"
                        + " - bucket:" + bucket
                        + " - key:" + key
                        + " - error:" + ex.toString();
            ex.printStackTrace();
            return setError(error);
                
        } finally {
            try {
                testF.delete();
            } catch(Exception ex) { }
        }
    }
        
    protected boolean content(String key) 
        throws TException
    {
        try {
            CloudResponse response = new CloudResponse(bucket, key);
            InputStream inStream  = manager.getObject(bucket, key, response);
            if (isError(response)) {
                return setError(response.getErrMsg());
            };
            
            String retTest = StringUtil.streamToString(inStream, "utf8");
            if (retTest.equals(testS)) {
                if (DEBUG) System.out.println("Match content");
                return true;
            } else {
                error = "Content match fails:"
                        + " - retTest:" + retTest
                        + " - testS:" + testS
                        ;
                return setError(error);
            }
            
        } catch (Exception ex) {
            
            ex.printStackTrace();
            error = "Content Catch Error - Unable to add content"
                        + " - bucket:" + bucket
                        + " - key:" + key
                        + " - error:" + ex.toString();
            return setError(error);
                
        }
    }

    protected boolean delete(String key)
        throws TException
    {
        try {;
            CloudResponse response = manager.deleteObject(bucket, key);
            if (isError(response)) {
                return setError(response.getErrMsg());
            };
            return true;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            error = "Delete Catch Error - Unable to add content"
                        + " - bucket:" + bucket
                        + " - key:" + key
                        + " - error:" + ex.toString();
            return setError(error);
                
        }
    }

    public StateHandler setForceTest(int forceTest) {
        this.forceTest = forceTest;
        System.out.println("***FORCETEST=" + forceTest);
        return this;
    }
    
    public static RetState getError(String bucket, String key, String error)
    {
        return new RetState(bucket, key, error);
    }
    
    public static class RetState
    {
        private String bucket = null;
        private String key = null;
        private String error = null;
        private Boolean ok = null;
        private Boolean isAlive = null;
        private long duration = 0;
        public RetState(String bucket, String key, String error) 
        {
            this.bucket = bucket;
            this.key = key;
            this.error = error;
            if(error != null) {
                this.ok = false;
            }
            if (this.key == null) {
                this.key = KEY;
            }
        }
        public RetState(String bucket, String key) 
        {
            this.bucket = bucket;
            this.key = key;
        }

        public String getBucket() {
            return bucket;
        }

        public void setBucket(String bucket) {
            this.bucket = bucket;
        }
        
        public String dump(String header)
        {
            return header + ":\n"
                    + " - bucket:" + bucket + "\n"
                    + " - key:" + key + "\n"
                    + " - duration:" + duration + "\n"
                    + " - ok:" + ok + "\n"
                    + " - error:" + error + "\n";
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public Boolean getOk() {
            return ok;
        }

        public void setOk(Boolean ok) {
            this.ok = ok;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public long getDuration() {
            return duration;
        }

        public void setDuration(long duration) {
            this.duration = duration;
        }
    }
}

