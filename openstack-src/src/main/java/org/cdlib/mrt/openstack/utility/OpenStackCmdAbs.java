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
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.CloudProperties;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

public abstract class OpenStackCmdAbs {
    protected static final String NAME = "OpenStackCmdAbs";
    protected static final String MESSAGE = NAME + ": ";
    protected static boolean DEBUGLOW = false;
    protected static boolean DEBUGHIGH = false;
    
    public static final long SEGSIZE = 1L*1024L*1024L*1024L;
    protected static final long MAX_SINGLE_SIZE = 2L*1024L*1024L*1024L;
    protected OpenStackAuth auth = null;
    
    public OpenStackCmdAbs(OpenStackAuth auth) 
    {
        this.auth = auth;
    }
    
    public OpenStackCmdAbs(String user,String pwd,String host)
        throws TException
    {
        this.auth = new OpenStackAuth(user, pwd, host);
    }
    
    public OpenStackCmdAbs(Properties prop)
        throws TException
    {
        this.auth = new OpenStackAuth(prop);
    }
    
    public abstract ResponseValues uploadBigManifest(
            XValues xValues,
            String container, 
            String key,
            Properties metaProp,
            SegmentValues values,
            int timeout)
        throws TException;
    
    public abstract SegmentValues deleteBig(
            XValues xValues,
            String container, 
            String key, 
            int timeout)
        throws TException;
    
    public XValues getAuth()
        throws TException
    {
        return auth.getXValuesRetry(5);
    }
    
    public OpenStackAuth getOpenStackAuth()
        throws TException
    {
        return auth;
    }
    
    public ResponseValues retrieveRetry(
            String container, 
            String key,
            int timeout,
            int retry)
        throws TException
    {
        TException saveTex = null;
        for (int itry=0; itry < retry; itry++) {
            try {
                return retrieve(container, key, timeout);
                
            } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
                throw rinf;
                
            } catch (TException tex) {
                saveTex = tex;
            }
            String msg = handleRetry(itry, "retrieveRetry", saveTex, 15000);
            System.out.println(msg);
        }
        throw saveTex;
    }
    
    public ResponseValues retrieve(
            String container, 
            String key,
            int timeout)
        throws TException
    {
        return retrieve(getAuth(), container, key, true, timeout);
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
        HttpResponse response = null;

        //System.out.println("!!!!:" + MESSAGE + "getSerializeObject.requestURL=" + requestURL);
        try {
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8");
            
            if (DEBUGHIGH) System.out.println("retrieve..." 
                    + " - container:" + container
                    + " - key:" + key
                    + " - requestURL:" + requestURL
                    );
            HttpClient httpclient = getHttpClient(timeout);
            HttpGet httpget = new HttpGet(requestURL);
            httpget.setHeader("X-Auth-Token", xValues.xAuthToken);
            response = httpclient.execute(httpget);
	    int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode >= 500) {
                dumpResponse("retrieve", response);
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                        "HTTPUTIL: getObject- Error during HttpClient processing"
                        + " - timeout:" + timeout
                        + " - URL:" + requestURL
                        + " - responseCode:" + responseCode
                        );
            }
            HttpEntity entity = response.getEntity();
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
            System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public ResponseValues retrieveShort(
            String container, 
            String key,
            long startByte,
            long endByte,
            int timeout)
        throws TException
    {
        return retrieveShort(getAuth(), container, key, startByte, endByte, timeout);
    }
    
    public static ResponseValues retrieveShort(
            XValues xValues,
            String container, 
            String key,
            long startByte,
            long endByte,
            int timeout)
        throws TException
    {
        HttpResponse response = null;

        //System.out.println("!!!!:" + MESSAGE + "getSerializeObject.requestURL=" + requestURL);
        try {
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8");
            
            if (DEBUGHIGH) System.out.println("retrieve..." 
                    + " - container:" + container
                    + " - key:" + key
                    + " - requestURL:" + requestURL
                    );
            HttpClient httpclient = getHttpClient(timeout);
            HttpGet httpget = new HttpGet(requestURL);
            httpget.setHeader("X-Auth-Token", xValues.xAuthToken);
            //Range: bytes=10-15
            String endByteS = "";
            if (endByte > 0) endByteS = "-" + endByte;
            httpget.setHeader("Range", "bytes=" + startByte + endByteS );
            response = httpclient.execute(httpget);
	    int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode >= 500) {
                dumpResponse("retrieve", response);
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                        "HTTPUTIL: getObject- Error during HttpClient processing"
                        + " - timeout:" + timeout
                        + " - URL:" + requestURL
                        + " - responseCode:" + responseCode
                        );
            }
            
            /*
            Header [] headers = response.getAllHeaders();
            for (Header header : headers) {
                System.out.println("HEADER - " + header.getName() + ":" + header.getValue());
            }
            */
            HttpEntity entity = response.getEntity();
            if (entity != null && (responseCode >= 200 && responseCode < 300)) {
                return new ResponseValues(response, true);
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
            System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public ResponseValues getMetaRetry(
            String container, 
            String key,
            int timeout,
            int retry)
        throws TException
    {
        TException saveTex = null;
        for (int itry=0; itry < retry; itry++) {
            try {
                return getMeta(container, key, timeout);
                
            } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
                throw rinf;
                
            } catch (TException tex) {
                saveTex = tex;
            }
            String msg = handleRetry(itry, "getMeta", saveTex, 5000);
            System.out.println(msg);
        }
        throw saveTex;
    }
    
    protected String handleRetry(int i, String msg, Exception saveExc, int sleep)
    {
        if (saveExc.toString().contains("responseCode:503")) {
            sleep += 15000;
        }
        int sleepTime = (i+1) * sleep;
        try {
            Thread.sleep(sleepTime);
        } catch (Exception ex) { }
        DateState date = new DateState();
        String name = Thread.currentThread().getName();
        return new String("***WARNING RETRY(" + i + ") " + msg + ": "
                + " - Date:" + date.getIsoDate()
                + " - sleep:" + sleepTime
                + " - name:" + name
                + " - Ex:" + saveExc
                );
    }
    
    public ResponseValues getMeta(
            String container, 
            String key,
            int timeout)
        throws TException
    {
        return getMeta(getAuth(), container, key, timeout);
    }
    
    public static ResponseValues getMeta(
            XValues xValues,
            String container, 
            String key,
            int timeout)
        throws TException
    {
        
        HttpResponse response = null;
        try {
            
            if (DEBUGLOW) System.out.println("getMeta..." 
                    + " - container:" + container
                    + " - key:" + key
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8");
            HttpClient httpclient = getHttpClient(timeout);
            HttpHead httphead = new HttpHead(requestURL);
            httphead.setHeader("X-Auth-Token", xValues.xAuthToken);
            response = httpclient.execute(httphead);
	    int responseCode = response.getStatusLine().getStatusCode();
            if (DEBUGLOW) System.out.println("getMeta response=" + responseCode);
            if (responseCode >= 500) {
                dumpResponse("meta", response);
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                        "HTTPUTIL: getObject- Error during HttpClient processing"
                        + " - timeout:" + timeout
                        + " - URL:" + requestURL
                        + " - responseCode:" + responseCode
                        );
            }
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
            if (DEBUGLOW) System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public static ResponseValues getContainerMeta(
            XValues xValues,
            String container, 
            int timeout)
        throws TException
    {
        
        HttpResponse response = null;
        try {
            
            if (DEBUGLOW) System.out.println("getMeta..." 
                    + " - container:" + container
                    );
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8");
            HttpClient httpclient = getHttpClient(timeout);
            HttpHead httphead = new HttpHead(requestURL);
            httphead.setHeader("X-Auth-Token", xValues.xAuthToken);
            response = httpclient.execute(httphead);
	    int responseCode = response.getStatusLine().getStatusCode();
            if (DEBUGLOW) System.out.println("getMeta response=" + responseCode);
            if (responseCode >= 500) {
                dumpResponse("meta", response);
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                        "HTTPUTIL: getObject- Error during HttpClient processing"
                        + " - timeout:" + timeout
                        + " - URL:" + requestURL
                        + " - responseCode:" + responseCode
                        );
            }
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
            if (DEBUGLOW) System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    
    public ResponseValues createContainerRetry(
            String container, 
            int timeout,
            int retry)
        throws TException
    {
        TException saveTex = null;
        for (int itry=0; itry < retry; itry++) {
            try {
                return createContainer(container, timeout);
                
            } catch (TException tex) {
                saveTex = tex;
            }
            String msg = handleRetry(itry, "createContainer", saveTex, 5000);
            System.out.println(msg);
        }
        throw saveTex;
    }
    
    public ResponseValues createContainer(
            String container, 
            int timeout)
        throws TException
    {
        return createContainer(getAuth(), container, timeout);
    }
    
    public static ResponseValues createContainer(
            XValues xValues,
            String container, 
            int timeout)
        throws TException
    {
        
        HttpResponse response = null;
        try {
            
            if (DEBUGLOW) System.out.println("getMeta..." 
                    + " - container:" + container
                    );
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8");
            HttpClient httpclient = getHttpClient(timeout);
            HttpPut httpput = new HttpPut(requestURL);
            httpput.setHeader("X-Auth-Token", xValues.xAuthToken);
            response = httpclient.execute(httpput);
	    int responseCode = response.getStatusLine().getStatusCode();
            if (DEBUGLOW) System.out.println("getMeta response=" + responseCode);
            if (responseCode >= 500) {
                dumpResponse("meta", response);
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                        "HTTPUTIL: getObject- Error during HttpClient processing"
                        + " - timeout:" + timeout
                        + " - URL:" + requestURL
                        + " - responseCode:" + responseCode
                        );
            }
            
            /*
            if ((responseCode == 202)) {
                throw new TException.REQUEST_ITEM_EXISTS("Container exists:" + container);
            }
                    */
            if ((responseCode >= 200) && (responseCode < 300)) {
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
            if (DEBUGLOW) System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public SegmentValues delete(
            String container, 
            String key, 
            int timeout)
        throws TException
    {
        return delete(container, key, true, timeout);
    }
    
    public SegmentValues delete(
            String container, 
            String key,
            boolean missingMsg,
            int timeout)
        throws TException
    {
        
        try {
            if (DEBUGLOW) System.out.println("delete..." 
                    + " - container:" + container
                    + " - key:" + key
                    );
            
            ResponseValues responseValues = getMetaRetry(
                    container,
                    key,
                    CloudConst.LONG_TIMEOUT,
                    3);
            
            long size = responseValues.getSize();
            XValues xValues = getAuth();
            if (size <= MAX_SINGLE_SIZE) {
                if (DEBUGLOW) System.out.println("Delete single - size=" + size);
                ResponseValues deleteValues = deleteSingle(
                    xValues, 
                    container,
                    key,
                    CloudConst.LONG_TIMEOUT);
                if (DEBUGLOW) {
                    System.out.println(deleteValues.dump("deleteSingle"));
                }
                SegmentValues values = new SegmentValues(container);
                String etag = responseValues.getEtag();
                long segSize = responseValues.getSize();
                values.add(etag, segSize, key);
                return values;
                
            } else {
                if (DEBUGLOW) System.out.println("Delete singleBig - size=" + size);
                return deleteBig(
                    xValues, 
                    container,
                    key,
                    CloudConst.LONG_TIMEOUT);
            }
            
        } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
            if (missingMsg) System.out.println("Delete item not found:" + container + "-" + key);
            throw rinf;
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    
    public static ResponseValues deleteSingle(
            XValues xValues,
            String container, 
            String key, 
            int timeout)
        throws TException
    {
        HttpResponse response = null;
        try {
            
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8")
                    //+ "?multipart-manifest=delete"
                    ;
            if (DEBUGHIGH) System.out.println("deleteSingle..." 
                    + " - container:" + container
                    + " - key:" + key
                    + " - requestURL:" + requestURL
                    );
            HttpClient httpclient = getHttpClient(timeout);
            HttpDelete httpdelete = new HttpDelete(requestURL);
            httpdelete.setHeader("X-Auth-Token", xValues.xAuthToken);
            response = httpclient.execute(httpdelete);
            ResponseValues responseValues = new ResponseValues(response);
            if (responseValues.statusCode != 204) {
                System.out.println("deleteSingle(" + responseValues.statusCode + "):" + requestURL);
            }
            return responseValues;
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            dumpResponse("deleteSingle", response);
            throw new TException(ex);
        }
    }
    
    public SegmentValues upload( 
            String container, 
            String key,
            InputStream inputStream,
            long size,
            Properties metaProp,
            int timeout)
        throws TException
    {
        return upload(
                container,
                key,
                inputStream,
                null,
                size,
                metaProp,
                timeout);
        
    }
    
    public SegmentValues upload(
            String container, 
            String key,
            InputStream inputStream,
            String eTag,
            long size,
            Properties metaProp,
            int timeout)
        throws TException
    {
        
        try {
            if (size <= 0) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "upload - size required");
            }
            // upload Big Object manifes
            XValues xValues =  getAuth();
            
            failPresent(container, key, timeout);
            
            if (DEBUGLOW) System.out.println("upload..." 
                    + " - container:" + container
                    + " - key:" + key
                    + " - size:" + size
                    );
            if (size <= MAX_SINGLE_SIZE) {
                ResponseValues responseValues = uploadSingle(
                        xValues,
                        container, 
                        key,
                        inputStream,
                        eTag,
                        size,
                        metaProp,
                        timeout);
                SegmentValues values = new SegmentValues(container);
                String etag = responseValues.getEtag();
                long segSize = responseValues.getSize();
                values.add(etag, segSize, key);
                return values;
                
            } else {
                if (DEBUGLOW) System.out.println("Delete singleBig - size=" + size);
                return uploadBig(
                        container, 
                        key,
                        inputStream,
                        size,
                        metaProp,
                        timeout);
            }
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public SegmentValues uploadFile(
            String container, 
            String key,
            File inputFile,
            String eTag,
            Properties metaProp,
            int timeout)
        throws TException
    {
        return uploadFile(
                container, key, inputFile, eTag, metaProp, true, timeout);
    }
    
    public SegmentValues uploadFile(
            String container, 
            String key,
            File inputFile,
            String eTag,
            Properties metaProp,
            boolean deleteIfPresent,
            int timeout)
        throws TException
    {
        if ((inputFile == null) || !inputFile.exists()) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "uploadSingleRetry-file missing");
        }
        InputStream inputStream = null;
        try {
            long size = inputFile.length();
            
            if (deleteIfPresent) {
                deletePresentNoTest(container,  key, timeout);
                
            } else {
                failPresent(container, key, timeout);
            }
            
            if (DEBUGHIGH) System.out.println("uploadFile..." 
                    + " - container:" + container
                    + " - key:" + key
                    + " - size:" + size
                    );
            if (size <= MAX_SINGLE_SIZE) {
                ResponseValues responseValues = uploadSingleRetry(
                        container, 
                        key,
                        inputFile,
                        eTag,
                        metaProp,
                        timeout,
                        3);
                SegmentValues values = new SegmentValues(container);
                String etag = responseValues.getEtag();
                long segSize = responseValues.getSize();
                values.add(etag, segSize, key);
                return values;
                
            } else {
                if (DEBUGLOW) System.out.println("Delete singleBig - size=" + size);
                inputStream = new FileInputStream(inputFile);
                return uploadBig(
                        container, 
                        key,
                        inputStream,
                        size,
                        metaProp,
                        timeout);
            }
            
        } catch (Exception ex) {
            if (DEBUGHIGH) {
                System.out.println("Exception:" + ex + "for key=" + key);
                ex.printStackTrace();
            }
            throw new TException(ex);
            
        } finally {
            if (inputStream != null) {
                try { 
                    inputStream.close();
                } catch (Exception ex) { }
            }
        }
    }
    
    public ResponseValues uploadSingleRetry(
            String container, 
            String key,
            File inputFile,
            String eTag,
            Properties metaProp,
            int timeout,
            int maxtries)
        throws TException
    {
 
        if ((inputFile == null) || !inputFile.exists()) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "uploadSingleRetry-file missing");
        }
  
        long size = inputFile.length();
        ResponseValues loadResponse = null;
        InputStream localInputStream = null;
        Exception runException = null;
        for (int retryCnt=0; retryCnt < maxtries; retryCnt++) {
            localInputStream = null;
            try {
                XValues xValues = getAuth();
                localInputStream = new FileInputStream(inputFile);
                return loadResponse = uploadSingle(
                   xValues,
                   container,
                   key,
                   localInputStream,
                   eTag,
                   inputFile.length(),
                   metaProp,
                   timeout);

            } catch (Exception ex) {
                runException = ex;
                String msg = handleRetry(retryCnt, "uploadSingleRetry", runException, 5000);
                System.out.println(msg);
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
        
        if (runException instanceof TException) {
            throw (TException)runException;
        } else {
            throw new TException(runException);
        }
    }
    
    public static ResponseValues uploadSingle(
            XValues xValues,
            String container, 
            String key,
            InputStream inputStream,
            String eTag,
            long size,
            Properties metaProp,
            int timeout)
        throws TException
    {
        CloudProperties cloudProperties = null;
        try {
            
            if (DEBUGHIGH) System.out.println("uploadSingle..." 
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
            setMetaProp(httpput, metaProp);
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
	    int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode >= 500) {
                dumpResponse("uploadSingle", response);
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                        "HTTPUTIL: getObject- Error during HttpClient processing"
                        + " - timeout:" + timeout
                        + " - URL:" + requestURL
                        + " - responseCode:" + responseCode
                        );
            }
            HttpEntity entity = response.getEntity();
            
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
   
    public SegmentValues uploadBig(
            String container, 
            String key,
            InputStream bigStream,
            long size,
            Properties metaProp,
            int timeout)
        throws TException
    {
        if (DEBUGHIGH) System.out.println("uploadBig..." 
                + " - container:" + container
                + " - key:" + key
                );
        try {
           
            SegmentValues segmentValues = uploadBigSegments(
                container, 
                key,
                bigStream,
                metaProp,
                size,
                timeout);
        
            // upload Big Object manifes
            XValues xValues =  getAuth();
            ResponseValues loadResponse = uploadBigManifest(
                    xValues,
                    container,
                    key,
                    metaProp,
                    segmentValues,
                    timeout);
            
            if (DEBUGHIGH) System.out.println("Big final"
                    + " - status=" + loadResponse.statusCode
                    + " - segment=" + segmentValues.cnt()
                    + " - key=" + key
                    + " - addSize=" + segmentValues.size()
                    + " - size=" + size
                    );
            return segmentValues;
        
        } catch (Exception ex) {
            try {
                delete(container, key, timeout);
                
            } catch (Exception delEx) {
                System.out.println("WARNING: exception in delete:" + delEx);
            }
            if (ex instanceof TException) {
                throw (TException) ex;
            } else throw new TException(ex);
        }
    }
    
    public static CloudProperties setMetaProp(HttpPut httpput, Properties metaProp)
    {
        CloudProperties cloudProperties = null;
        if ((metaProp != null) && (metaProp.size() > 0)) {
            cloudProperties = new CloudProperties(metaProp);
            Properties headProp = cloudProperties.buildMetaProperties();
            for (Object pkeyO : headProp.keySet()) {
                String pkey = (String)pkeyO;
                String pvalue = headProp.getProperty(pkey);
                httpput.setHeader(pkey, pvalue);
                if (DEBUGLOW) System.out.println("Add " + pkey + ": " + pvalue);
            }
        }
        return cloudProperties;
    }
    
    protected static String getMetaHeader(String key, String value)
    {
        key = StringUtil.upperCaseFirst(key);
        return "X-Object-Meta-" + key + ": " + value;
    }
    
    public void failPresent(
            String container, 
            String key,
            int timeout)
        throws TException
    {
        try {
            ResponseValues retrieveValues = getMetaRetry(container, key, timeout, 4);
            if (retrieveValues.getSize() != null) {
                throw new TException.REQUEST_ITEM_EXISTS(MESSAGE + "upload item exists"
                        + " - container=" + container
                        + " - key=" + key
                        );
            }
        } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
        }
    }
    
    public void deletePresent(
            String container, 
            String key,
            int timeout)
        throws TException
    {
        try {
            ResponseValues retrieveValues = getMetaRetry(container, key, timeout, 10);
            if (retrieveValues.getSize() != null) {
                try {
                    delete(
                        container, 
                        key, 
                        timeout);
                    System.out.println("WARNING - deletePresent item deleted:" + key);
                    //getLocation();
                    
                } catch (Exception ex) {
                    System.out.println("WARNING - deletePresent exception:" + ex);
                }
            }
        } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
            return;
        }
    }
    
    public void deletePresentNoTest(
            String container, 
            String key,
            int timeout)
        throws TException
    {
        try {
            delete(
                container, 
                key,
                false,
                timeout);
            System.out.println("WARNING - deletePresent item deleted:" + key);
            //getLocation();
            
        } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) { 

        } catch (Exception ex) {
            System.out.println("WARNING - deletePresent exception:" + ex);
        }
    }
    
    protected void getLocation()
    {
        try {
            throw new TException.GENERAL_EXCEPTION("test");
        } catch (Exception tex) {
            System.out.println("GET LOCATION");
            tex.printStackTrace();
        }
    }
    
    protected static String getSegKey(String key)
    {
        return "seg-" + key;
    }
    
    protected static String getSegKey(String key, int segmentCnt)
    {
        String preKey = getSegKey(key);
        String padCnt = getPadCnt(segmentCnt);
        return preKey + "/" + padCnt;
    }
   
    public SegmentValues uploadBigSegments(
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
        SegmentValues values = new SegmentValues(container);
        try {
           tmpFile = FileUtil.getTempFile("tmp", ".txt");
           long tmpFileLen = 1;
           int maxtries = 3;
           while(true) {
                tmpFileLen = setFile(bigStream, tmpFile, SEGSIZE);
                if (tmpFileLen != tmpFile.length()) {
                    throw new TException.INVALID_ARCHITECTURE("temp file length != moved length "
                            + " - tmpFileLen=" + tmpFileLen
                            + " - tmpFile.length=" + tmpFile.length()
                            );
                }
                if (DEBUGLOW) System.out.println("tmpfile length=" + tmpFile.length());
                if (tmpFileLen == 0) break;
                String localKey = getSegKey(key,segmentCnt);
                ResponseValues loadResponse = null;
                loadResponse = uploadSingleRetry(
                           container,
                           localKey,
                           tmpFile,
                           null,
                           null,
                           timeout,
                           3);
                String etag = loadResponse.getEtag();
                long segSize = loadResponse.getSize();
                values.add(etag, segSize, localKey);
                addSize += tmpFile.length();
                System.out.println("Big upload"
                    + " - status=" + loadResponse.statusCode
                    + " - segment=" + segmentCnt
                    + " - localKey=" + localKey
                    + " - tmpFile.length=" + tmpFile.length()
                    );

                segmentCnt++;
            }
            
            if (DEBUGHIGH) System.out.println("Big final"
                    + " - segment=" + segmentCnt
                    + " - key=" + key
                    + " - addSize=" + addSize
                    + " - size=" + size
                    );
            return values;
        
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
        String formatted = String.format("%06d", segmentCnt);
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
            
            if (DEBUGLOW) System.out.println("main..." 
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
            System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public String getList(
            String container, 
            String startMarker,
            String endMarker,
            int timeout)
        throws TException
    {
        return getList(getAuth(), container, startMarker, endMarker, timeout);
    }
    
    public CloudList getList(
            String container, 
            String prefix,
            int timeout)
        throws TException
    {
        return getList(getAuth(), container, prefix, timeout);
    }
    
    public CloudList getList(
            String container, 
            String marker,
            int limit,
            int timeout)
        throws TException
    {
        return getList(getAuth(), container, marker, limit, timeout);
    }
    
    public static String getList(
            XValues xValues,
            String container, 
            String startMarker,
            String endMarker,
            int timeout)
        throws TException
    {
        try {
            
            if (DEBUGLOW) System.out.println("main..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "?format=xml"
                    + "&marker=" + URLEncoder.encode(startMarker,"utf-8")
                    + "&end_marker=" + URLEncoder.encode(endMarker,"utf-8");
            if (DEBUGLOW) System.out.println("requestURL=" + requestURL);
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
            System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public static CloudList getList(
            XValues xValues,
            String container, 
            String prefix,
            int timeout)
        throws TException
    {
        try {
            
            if (DEBUGLOW) System.out.println("main..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "?format=xml";
            if (StringUtil.isNotEmpty(prefix)) {
                requestURL += "&prefix=" + URLEncoder.encode(prefix,"utf-8");
            }
            if (DEBUGLOW) System.out.println("requestURL=" + requestURL);
            HttpClient httpclient = getHttpClient(timeout);
            HttpGet httpget = new HttpGet(requestURL);
            httpget.setHeader("X-Auth-Token", xValues.xAuthToken);
            HttpResponse response = httpclient.execute(httpget);
            ResponseValues values = new ResponseValues(response, true);
            InputStream inStream = values.inputStream;
            String xml = StringUtil.streamToString(inStream, "utf-8");
            return getList(xml, container);
            
        } catch (TException ex) {
            throw ex;
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public CloudList getListFull(
            String container, 
            String prefix,
            String marker,
            Integer limit,
            int timeout)
        throws TException
    {
        return getListFull(getAuth(), container, prefix, marker, limit, timeout);
    }
    
    
    public static CloudList getListFull(
            XValues xValues,
            String container, 
            String prefix,
            String marker,
            Integer limit,
            int timeout)
        throws TException
    {
        try {
            
            if (DEBUGLOW) System.out.println("main..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    + " - prefix:" + prefix
                    + " - marker:" + marker
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "?format=xml";
            if (StringUtil.isNotEmpty(prefix)) {
                requestURL += "&prefix=" + URLEncoder.encode(prefix,"utf-8");
            }
            if (StringUtil.isNotEmpty(marker)) {
                requestURL += "&marker=" + URLEncoder.encode(marker,"utf-8");
            }
            if (limit > 0) {
                requestURL += "&limit=" + limit;
            }
            if (DEBUGLOW) System.out.println("requestURL=" + requestURL);
            HttpClient httpclient = getHttpClient(timeout);
            HttpGet httpget = new HttpGet(requestURL);
            httpget.setHeader("X-Auth-Token", xValues.xAuthToken);
            HttpResponse response = httpclient.execute(httpget);
            ResponseValues values = new ResponseValues(response, true);
            testStandardException("getListFull", values, requestURL);
            InputStream inStream = values.inputStream;
            String xml = StringUtil.streamToString(inStream, "utf-8");
            return getList(xml, container);
            
        } catch (TException ex) {
            throw ex;
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected static ResponseValues testStandardException (String header, ResponseValues response, String requestURL)
        throws TException
    {
        try {
            int responseCode = response.statusCode;
            if ((responseCode >= 200) && (responseCode < 300))  {
                return response;
            }
            if (responseCode == 404) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(
                        header
                        + " - URL:" + requestURL
                        + " - responseCode:" + responseCode
                        );
            }
            if (responseCode >= 500) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(
                       header
                        + " - URL:" + requestURL
                        + " - responseCode:" + responseCode
                        );
            }
            InputStream inStream = response.inputStream;
            String exMsg = "";
            if (inStream != null) {
                exMsg = " - Exception:" + StringUtil.streamToString(inStream, "utf-8");
            }
            throw new TException.GENERAL_EXCEPTION(
                       header + "Unsupported status exception"
                        + " - URL:" + requestURL
                        + " - responseCode:" + responseCode
                        + exMsg
                        );
  
        } catch (TException ex) {
            throw ex;
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public static CloudList getList(
            XValues xValues,
            String container, 
            String prefix,
            int limit,
            int timeout)
        throws TException
    {
        try {
            
            if (DEBUGLOW) System.out.println("main..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "?format=xml";
            if (StringUtil.isNotEmpty(prefix)) {
                requestURL += "&marker=" + URLEncoder.encode(prefix,"utf-8");
            }
            if (limit > 0) {
                requestURL += "&limit=" + limit;
            }
            if (DEBUGLOW) System.out.println("requestURL=" + requestURL);
            HttpClient httpclient = getHttpClient(timeout);
            HttpGet httpget = new HttpGet(requestURL);
            httpget.setHeader("X-Auth-Token", xValues.xAuthToken);
            HttpResponse response = httpclient.execute(httpget);
            ResponseValues values = new ResponseValues(response, true);
            InputStream inStream = values.inputStream;
            String xml = StringUtil.streamToString(inStream, "utf-8");
            return getList(xml, container);
            
        } catch (TException ex) {
            throw ex;
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public static CloudList getList(String xml, String container)
    {
        CloudList list = new CloudList();
        int pos=0;
        int current=0;
        while (true) {
            pos = xml.indexOf("<object>", current);
            if (pos < 0) break;
            String name = get(xml, pos, "name");
            String hash = get(xml, pos, "hash");
            Long size = getLong(xml, pos, "bytes");
            String contentType = get(xml, pos, "content_type");
            String lastModified = get(xml, pos, "last_modified");
            list.add(container, name, size, hash, contentType, lastModified);
            current = pos + 1;
            if (DEBUGLOW) System.out.println("curent=" + current);
        }
        return list;
    }
    
    public static String get(String xml, int pos, String key)
    {
        if (DEBUGLOW) System.out.println("get: - pos=" + pos + " - key=" + key);
        int start = xml.indexOf("<" + key + ">", pos);
        if (start < 0) return null;
        start += key.length() + 2;
        int end = xml.indexOf("</" + key + ">", pos);
        String sub = xml.substring(start, end);
        if (DEBUGLOW) System.out.println("get: - sub=" + sub);
        return sub;
    }
    
    public static Long getLong(String xml, int pos, String key)
    {
        String sizeS = get(xml, pos, key);
        if (sizeS == null) return null;
        return Long.parseLong(sizeS);
    }
    
    public static CloudList.CloudEntry getLastEntry(CloudList list)
    {
        int inx = list.size();
        if (inx == 0) return null;
        inx--;
        return list.get(inx);
    }
    
    public static void dumpResponse(String header, HttpResponse response)
    {
        try {
            if (response == null) {
                System.out.println("dumpResponse called and response null");
                return;
            }
            Properties responseProp = HTTPUtil.response2Property(response);
            System.out.println(PropertiesUtil.dumpProperties("***WARNING DUMP " + header, responseProp));
        } catch (Exception ttex) { }
        
    }
    
    public static HttpClient getHttpClient(int timeout)
        throws Exception
    {       
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(timeout).build();
            HttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
            return httpClient;
    }
}
