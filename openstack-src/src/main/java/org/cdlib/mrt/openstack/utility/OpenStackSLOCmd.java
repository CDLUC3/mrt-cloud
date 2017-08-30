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
package org.cdlib.mrt.openstack.utility;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.cdlib.mrt.cloud.CloudProperties;
import static org.cdlib.mrt.openstack.utility.OpenStackCmdAbs.getHttpClient;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

public class OpenStackSLOCmd {
    protected static final String NAME = "OpenStackCmd";
    protected static final String MESSAGE = NAME + ": ";
    protected static boolean DEBUG = false;
    
    protected static final long SEGSIZE = 1L*1024L*1024L*1024L;
    
    public static XValues getAuth(String user, String pwd, String host, int timeout)
        throws TException
    {
        try {
            String requestURL = host + "/auth/v1.0";
            HttpClient httpclient = getHttpClient(timeout);
            HttpGet httpget = new HttpGet(requestURL);
            httpget.setHeader("X-Storage-User", user);
            httpget.setHeader("X-Storage-Pass", pwd);
            HttpResponse response = httpclient.execute(httpget);
            Properties responseProp = HTTPUtil.response2Property(response);
            return new XValues(responseProp);
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public static ResponseValues retrieve(
            XValues xValues,
            String container, 
            String key,
            int timeout)
        throws TException
    {
        return retrieve(xValues, container, key, true, timeout);
    }
    
    public static ResponseValues retrieve(
            XValues xValues,
            String container, 
            String key,
            boolean setInputStream,
            int timeout)
        throws TException
    {
        try {
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8");
            
            System.out.println("main..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    + " - key:" + key
                    + " - requestURL:" + requestURL
                    );
            HttpClient httpclient = getHttpClient(timeout);
            HttpGet httpget = new HttpGet(requestURL);
            httpget.setHeader("X-Auth-Token", xValues.xAuthToken);
            HttpResponse response = httpclient.execute(httpget);
            HttpEntity entity = response.getEntity();
	    int responseCode = response.getStatusLine().getStatusCode();
            if (entity != null && (responseCode >= 200 && responseCode < 300)) {
                return new ResponseValues(response, setInputStream);
            }
            if (responseCode == 404) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                    "HTTPUTIL: getObject- Error during HttpClient processing"
                    + " - timeout:" + timeout
                    + " - URL:" + requestURL
                    + " - responseCode:" + responseCode
                    );
            }
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                    "HTTPUTIL: getObject- Error during HttpClient processing"
                    + " - timeout:" + timeout
                    + " - URL:" + requestURL
                    + " - responseCode:" + responseCode
                    );
            
        } catch (TException ex) {
            throw ex;
            
        } catch (Exception ex) {
            if (DEBUG) System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public static ResponseValues getMeta(
            XValues xValues,
            String container, 
            String key,
            int timeout)
        throws TException
    {
        try {
            
            System.out.println("getMeta..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    + " - key:" + key
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8");
            HttpClient httpclient = getHttpClient(timeout);
            HttpHead httphead = new HttpHead(requestURL);
            httphead.setHeader("X-Auth-Token", xValues.xAuthToken);
            HttpResponse response = httpclient.execute(httphead);
            HttpEntity entity = response.getEntity();
	    int responseCode = response.getStatusLine().getStatusCode();
            System.out.println("getMeta response=" + responseCode);
            if ((responseCode >= 200) && (responseCode < 300)) {
                return new ResponseValues(response, false);
            }
            if (responseCode == 404) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                    "HTTPUTIL: getObject- Error during HttpClient processing"
                    + " - timeout:" + timeout
                    + " - URL:" + requestURL
                    + " - responseCode:" + responseCode
                    );
            }
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                    "HTTPUTIL: getObject- Error during HttpClient processing"
                    + " - timeout:" + timeout
                    + " - URL:" + requestURL
                    + " - responseCode:" + responseCode
                    );
            
        } catch (TException ex) {
            throw ex;
            
        } catch (Exception ex) {
            if (DEBUG) System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public static ResponseValues delete(
            XValues xValues,
            String container, 
            String key, 
            int timeout)
        throws TException
    {
        try {
            
            System.out.println("main..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    + " - key:" + key
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8")
                    + "?multipart-manifest=delete"
                    ;
            System.out.println("Delete:" + requestURL);
            HttpClient httpclient = getHttpClient(timeout);
            HttpDelete httpdelete = new HttpDelete(requestURL);
            httpdelete.setHeader("X-Auth-Token", xValues.xAuthToken);
            HttpResponse response = httpclient.execute(httpdelete);
	    int responseCode = response.getStatusLine().getStatusCode();
            return new ResponseValues(response);
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public static ResponseValues upload(
            XValues xValues,
            String container, 
            String key,
            InputStream inputStream,
            String eTag,
            Properties metaProp,
            long size,
            int timeout)
        throws TException
    {
        CloudProperties cloudProperties = null;
        System.out.println("***After retrieveValues");
        try {
            
            System.out.println("main..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    + " - key:" + key
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8");
            HttpClient httpclient = getHttpClient(timeout);
            HttpPut httpput = new HttpPut(requestURL);
            httpput.setHeader("X-Auth-Token", xValues.xAuthToken);
            httpput.setHeader("Content-Type", "application/octet-stream");
            if (!StringUtil.isAllBlank(eTag)) {
                httpput.setHeader("ETag", eTag);
            }
            if ((metaProp != null) && (metaProp.size() > 0)) {
                cloudProperties = new CloudProperties(metaProp);
                Properties headProp = cloudProperties.buildMetaProperties();
                for (Object pkeyO : headProp.keySet()) {
                    String pkey = (String)pkeyO;
                    String pvalue = headProp.getProperty(pkey);
                    httpput.setHeader(pkey, pvalue);
                    System.out.println("Add " + pkey + ": " + pvalue);
                }
            }
            ContentType contentType = ContentType.create("application/octet-stream");
            if (size > 0) {
                httpput.setEntity( new InputStreamEntity(inputStream, size, contentType ) );
            }
            HttpResponse response = null;
            try {
                response = httpclient.execute(httpput);
            } catch (org.apache.http.client.ClientProtocolException cpe) {
                throw new TException.REMOTE_IO_SERVICE_EXCEPTION("remote IO error:" + cpe);
            }
            HttpEntity entity = response.getEntity();
            
	    int responseCode = response.getStatusLine().getStatusCode();
            if (entity != null && (responseCode >= 200 && responseCode < 300)) {
                return new ResponseValues(response);
            }
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                    "HTTPUTIL: getObject- Error during HttpClient processing"
                    + " - timeout:" + timeout
                    + " - URL:" + requestURL
                    + " - responseCode:" + responseCode
                    );
            
        } catch (TException ex) {
            throw ex;
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception ex) {
                    
                }
            }
        }
    }
    
    public static ResponseValues uploadBigManifest(
            XValues xValues,
            String container, 
            String key,
            Properties metaProp,
            SegmentValues values,
            int timeout)
        throws TException
    {
        CloudProperties cloudProperties = null;
        System.out.println("***After retrieveValues");
        try {
            
            System.out.println("main..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    + " - key:" + key
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8")
                    + "?multipart-manifest=put";
            String manifestHeader = URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8")
                    + "/";
            HttpClient httpclient = getHttpClient(timeout);
            HttpPut httpput = new HttpPut(requestURL);
            httpput.setHeader("X-Auth-Token", xValues.xAuthToken);
            //httpput.setHeader("X-Object-Manifest", manifestHeader);
            if ((metaProp != null) && (metaProp.size() > 0)) {
                cloudProperties = new CloudProperties(metaProp);
                Properties headProp = cloudProperties.buildMetaProperties();
                for (Object pkeyO : headProp.keySet()) {
                    String pkey = (String)pkeyO;
                    String pvalue = headProp.getProperty(pkey);
                    httpput.setHeader(pkey, pvalue);
                    System.out.println("Add " + pkey + ": " + pvalue);
                }
            }
            ContentType contentType = ContentType.create("application/octet-stream");
            String json = values.getJson();
            byte [] bytes = json.getBytes("utf-8");
            System.out.println("uploadBigManifest:"
                    + " - bytes:" + bytes.length
                    + " - json:\n" + json
                    + "\n - requestURL:" + requestURL
                    );
            httpput.setEntity( new ByteArrayEntity(bytes) );
            HttpResponse response = null;
            try {
                response = httpclient.execute(httpput);
            } catch (org.apache.http.client.ClientProtocolException cpe) {
                throw new TException.REMOTE_IO_SERVICE_EXCEPTION("remote IO error:" + cpe);
            }
            HttpEntity entity = response.getEntity();
            
	    int responseCode = response.getStatusLine().getStatusCode();
            if (entity != null && (responseCode >= 200 && responseCode < 300)) {
                return new ResponseValues(response, false);
            }
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                    "HTTPUTIL: getObject- Error during HttpClient processing"
                    + " - timeout:" + timeout
                    + " - URL:" + requestURL
                    + " - responseCode:" + responseCode
                    );
            
        } catch (TException ex) {
            throw ex;
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
            
        }
    }
    
    protected static String getMetaHeader(String key, String value)
    {
        key = StringUtil.upperCaseFirst(key);
        return "X-Object-Meta-" + key + ": " + value;
    }
    
    public static ResponseValues uploadTest(
            XValues xValues,
            String container, 
            String key,
            InputStream inputStream,
            String eTag,
            Properties metaProp,
            long size,
            int timeout)
        throws TException
    {
        failPresent(xValues, container, key, timeout);
        return upload(xValues, container, key, inputStream, eTag, metaProp, size, timeout);
    } 
    public static int uploadBigTest(
            String user, String pwd, String host, 
            String container, 
            String key,
            InputStream inputStream,
            Properties metaProp,
            long size,
            int timeout)
        throws TException
    {
        
        XValues xValues =  OpenStackSLOCmd.getAuth(
                    user, pwd, host, CloudConst.LONG_TIMEOUT);
        failPresent(xValues, container, key, timeout);
        return uploadBig(user, pwd, host, container, key, inputStream, metaProp, size, timeout);
    
    }
    
    public static void failPresent(
            XValues xValues,
            String container, 
            String key,
            int timeout)
        throws TException
    {
        try {
            System.out.println("***Before failPresent");
            ResponseValues retrieveValues = getMeta(xValues, container, key, timeout);
            if (retrieveValues.getSize() != null) {
                throw new TException.REQUEST_ITEM_EXISTS(MESSAGE + "upload item exists"
                        + " - container=" + container
                        + " - key=" + key
                        );
            }
        } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
        }
    }
   
    public static int uploadBig(
            String user, String pwd, String host, 
            String container, 
            String key,
            InputStream bigStream,
            Properties metaProp,
            long size,
            int timeout)
        throws TException
    {
        // upload segments
        File tmpFile = null;
        int segmentCnt = 1;
        long addSize = 0;
        try {
           tmpFile = FileUtil.getTempFile("tmp", ".txt");
           long tmpFileLen = 1;
           int maxtries = 3;
           SegmentValues values = new SegmentValues(container);
           while(true) {
                tmpFileLen = setFile(bigStream, tmpFile, SEGSIZE);
                if (tmpFileLen != tmpFile.length()) {
                    throw new TException.INVALID_ARCHITECTURE("temp file length != moved length "
                            + " - tmpFileLen=" + tmpFileLen
                            + " - tmpFile.length=" + tmpFile.length()
                            );
                }
                System.out.println("tmpfile length=" + tmpFile.length());
                if (tmpFileLen == 0) break;
                String padCnt = getPadCnt(segmentCnt);
                String localKey = key + "/" + padCnt;
                ResponseValues loadResponse = null;
                XValues xValues =  OpenStackSLOCmd.getAuth(
                    user, pwd, host, CloudConst.LONG_TIMEOUT);
                for (int retryCnt=0; retryCnt < maxtries; retryCnt++) {
                    InputStream localInputStream = null;
                    try {
                        localInputStream = new FileInputStream(tmpFile);
                        loadResponse = upload(
                           xValues,
                           container,
                           localKey,
                           localInputStream,
                           null,
                           null,
                           tmpFile.length(),
                           timeout);
                        break;
                        
                    } catch (TException.REMOTE_IO_SERVICE_EXCEPTION retryEx) {
                        if (retryCnt >= (maxtries - 1)) {
                            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("retries exceeded:" + retryCnt + " - Exception:" + retryEx);
                        }
                        System.out.println("WARNING retry exception:" + retryEx);
                        continue;
                        
                    } finally {
                        if (localInputStream != null) {
                            try {
                                localInputStream.close();
                            } catch (Exception ex) { }
                            localInputStream = null;
                        }
                    }
                }
                String etag = loadResponse.getEtag();
                long segSize = loadResponse.getSize();
                values.add(etag, segSize, localKey);
                addSize += tmpFile.length();
                System.out.println("Big upload"
                    + " - status=" + loadResponse.statusCode
                    + " - segment=" + segmentCnt
                    + " - localKey=" + localKey
                    + " - padCnt=" + padCnt
                    + " - tmpFile.length=" + tmpFile.length()
                    );

                segmentCnt++;
            }
           
            // upload Big Object manifes
            XValues xValues =  OpenStackSLOCmd.getAuth(
                        user, pwd, host, CloudConst.LONG_TIMEOUT);
            ResponseValues loadResponse = uploadBigManifest(
                    xValues,
                    container,
                    key,
                    metaProp,
                    values,
                    timeout);
            
            System.out.println("Big final"
                    + " - status=" + loadResponse.statusCode
                    + " - segment=" + segmentCnt
                    + " - key=" + key
                    + " - addSize=" + addSize
                    + " - size=" + size
                    );
            return segmentCnt;
        
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        } finally {
            
            if (tmpFile != null) {
                try {
                    tmpFile.delete();
                } catch (Exception ex) { }
            }
        }
    }
    
    public static String getPadCnt(int segmentCnt)
    {
        String formatted = String.format("%07d", segmentCnt);
        return formatted;
    }
    
    protected static long setFile(InputStream inputStream, File outFile, long segmentLength)
        throws TException
    {
        long totalRead = 0;
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(outFile);
            byte[] buffer = new byte[1024 * 128]; // 128k buffer 
            while(totalRead <= segmentLength) { // go on reading while total bytes read is
                int bytesRead = inputStream.read(buffer);
                if (bytesRead < 0) break;
                if (bytesRead == 0) continue;
                totalRead += bytesRead;
                out.write(buffer, 0, bytesRead);
            }
            return totalRead;
            
        } catch (Exception ex) {
            System.out.println("setFile Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception ex) { }
                
            }
        }
 
    }
    
    public static String getContainer(
            XValues xValues,
            String container, 
            int timeout)
        throws TException
    {
        try {
            
            System.out.println("main..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "?format=xml";
            HttpClient httpclient = getHttpClient(timeout);
            HttpGet httpget = new HttpGet(requestURL);
            httpget.setHeader("X-Auth-Token", xValues.xAuthToken);
            HttpResponse response = httpclient.execute(httpget);
            ResponseValues values = new ResponseValues(response, true);
            InputStream inStream = values.inputStream;
            return StringUtil.streamToString(inStream, "utf-8");
            
        } catch (TException ex) {
            throw ex;
            
        } catch (Exception ex) {
            if (DEBUG) System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
}
