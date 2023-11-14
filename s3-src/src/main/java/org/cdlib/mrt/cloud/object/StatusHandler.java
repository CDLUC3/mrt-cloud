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



import java.util.ArrayList;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;

/**
 * Specific SDSC Storage Cloud handling
 * @author dloy
 */
public class StatusHandler
{
    protected static final String NAME = "StatusHandler";
    protected static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    
    private final CloudStoreInf service;
    private final String bucket;
    private final long nodeNumber;
    private final int keyCnt;
    private final boolean failCnt;
    
    private RetStatus retStatus = null;
    
    
    protected FileContent fileContent = null;
    protected String error = null;
    
    public static StatusHandler.RetStatus runStatusHandler(
            long nodeNumber,
            CloudStoreInf service,
            String bucket,
            int keyCnt,
            boolean failCnt)
    {
        StatusHandler statusHandler = new StatusHandler(nodeNumber, service, bucket, keyCnt, failCnt);
        statusHandler.testScan();
        return statusHandler.retStatus;
    }
    
    public static StatusHandler getStatusHandler(
            long nodeNumber,
            CloudStoreInf service,
            String bucket,
            int keyCnt,
            boolean failCnt)
    {
        return new StatusHandler(nodeNumber, service, bucket, keyCnt, failCnt);
    }
    
    protected StatusHandler(
            final long nodeNumber,
            final CloudStoreInf service,
            String bucket,
            int keyCnt,
            boolean failCnt)
    {
        this.nodeNumber = nodeNumber;
        this.service = service;
        this.bucket = bucket;
        this.keyCnt = keyCnt;
        this.failCnt = failCnt;
        this.retStatus = new RetStatus(nodeNumber, bucket);
        
    }
    
    public void testScan()
    {
        
        long startGetTime = System.currentTimeMillis();
        try {
            CloudResponse response = service.getObjectListAfter(bucket, null, keyCnt);
            if (response == null) {
                retStatus.ok = false;
                retStatus.error = "Error occurred processing bucket:" + bucket;
                return ;
            }
            if (response.getException() != null) {
                retStatus.ok = false;
                retStatus.error = "Error occurred processing exception:" + response.getException().toString();
                return;
            }
            CloudList cloudList = response.getCloudList();
            ArrayList<CloudList.CloudEntry> list = cloudList.getList();
            retStatus.returnCnt = list.size();
            if (DEBUG) {
                for (CloudList.CloudEntry entry: list) {
                    System.out.println("Entry:"
                            + " - nodeNumber:" + nodeNumber
                            + " - bucket:" + entry.container
                            + " - key:" + entry.key
                            + " - size:" + entry.size
                            + " - date:" + entry.lastModified
                    );
                }
            }
            if (failCnt && (retStatus.returnCnt != keyCnt)) {
                retStatus.ok = false;
                retStatus.error = "Error count != limit:"
                        + " - returnCnt=" + retStatus.returnCnt
                        + " - keyCnt=" + keyCnt;
                return;
            }
            retStatus.ok = true;
            
            return;
            
        } catch (TException tex) {
            retStatus.duration = System.currentTimeMillis() - startGetTime;
            retStatus.ok = false;
            retStatus.error = "Error occurred processing exception:" + tex.toString();
            
        } catch (Exception ex) {
            retStatus.duration = System.currentTimeMillis() - startGetTime;
            retStatus.ok = false;
            retStatus.error = "Error occurred processing exception:" + ex.toString();
            
        }  finally {
            retStatus.duration = System.currentTimeMillis() - startGetTime;
        }
    }
    
    public static class RetStatus
    {
        public String bucket = null;
        public long nodeNumber = 0;
        public String error = null;
        public Boolean ok = null;
        public long duration = 0;
        public int returnCnt = 0;
        public RetStatus(long nodeNumber, String bucket) 
        {
            this.nodeNumber = nodeNumber;
            this.bucket = bucket;
        }
        public RetStatus(String bucket) 
        {
            this.bucket = bucket;
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
                    + " - duration:" + duration + "\n"
                    + " - ok:" + ok + "\n"
                    + " - returnCnt:" + returnCnt + "\n"
                    + " - error:" + error + "\n";
        }
        
        public String dumpline(String header)
        {
            return header + ":"
                    + " - bucket:" + bucket 
                    + " - duration:" + duration
                    + " - ok:" + ok
                    + " - returnCnt:" + returnCnt + "\n"
                    + " - error:" + error;
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

        public long getDuration() {
            return duration;
        }

        public void setDuration(long duration) {
            this.duration = duration;
        }
    }
}

