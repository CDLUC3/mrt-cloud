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
package org.cdlib.mrt.openstack.utility;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.security.MessageDigest;
import org.apache.http.HttpEntity;
import org.cdlib.mrt.utility.*;
/**
 *
 * @author dloy
 * This routine is specifically designed to handle dropped connections during an http request
 */
public class HttpGetOpenStack 
    extends OpenStackCmdDLO
{
    protected static final String NAME = "HttpGetOpenStack";
    protected static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    private static final boolean DEBUG_HIGH = false;
    protected static final int BUFSIZE = 126000;
    public final static long SEGMENT = 400000000L;
    public final static long MAX_RETRY = 3; //no content length only
    protected long contentLength = 0;
    protected int timeout = 0;
    protected LoggerInf logger = null;
    protected File outFile = null;
    protected String container = null;
    protected String key = null;
    protected OpenStackAuth auth = null;
    protected ResponseValues response = null;
    protected final MessageDigest algorithm;
    protected String sha_256 = null;
    
    public static ResponseValues getFile(OpenStackAuth auth, String container, String key, File outFile, int timeout, LoggerInf logger)
        throws TException
    {
        HttpGetOpenStack get = new HttpGetOpenStack(auth, container, key, outFile, timeout, logger);
        get.build();
        return get.getResponse();
    }
    
    public static ResponseValues getValues(OpenStackAuth auth, String container, String key, int timeout, LoggerInf logger)
           throws TException
    {    try {
            File tmpFile = FileUtil.getTempFile("HttpGetTemp", ".txt");
            if (DEBUG_HIGH) System.out.println("temp:"  + tmpFile.getCanonicalPath());
            HttpGetOpenStack httpGetOpenStack =  new HttpGetOpenStack(auth, container, key, tmpFile, timeout, logger);
            httpGetOpenStack.build();
            ResponseValues response = httpGetOpenStack.getResponse();
            DeleteOnCloseFileInputStream inStream = new DeleteOnCloseFileInputStream(tmpFile);
            response.inputStream = inStream; // set special temp delete inStream
            return response;
            
        } catch (TException tex) {
            throw tex;
        
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static InputStream getStream(OpenStackAuth auth, String container, String key, int timeout, LoggerInf logger)
        throws TException
    {
        return setTempGet(auth, container, key, timeout, logger);
    }
    
    public static DeleteOnCloseFileInputStream setTempGet(OpenStackAuth auth, String container, String key, int timeout, LoggerInf logger)
        throws TException
    {
        try {
            File tmpFile = FileUtil.getTempFile("HttpGetTemp", ".txt");
            HttpGetOpenStack get =  new HttpGetOpenStack(auth, container, key, tmpFile, timeout, logger);
            get.build();
            DeleteOnCloseFileInputStream inStream = new DeleteOnCloseFileInputStream(tmpFile);
            return inStream;
            
        } catch (TException tex) {
            throw tex;
        
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public HttpGetOpenStack(OpenStackAuth auth, String container, String key, File outFile, int timeout, LoggerInf logger)
        throws TException
    {
        super(auth);
        this.container = container;
        this.key = key;
        this.timeout = timeout;
        this.logger = logger;
        this.outFile = outFile;
        try {
            algorithm = 
                    MessageDigest.getInstance("SHA-256");
            algorithm.reset();
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    
    
    public void build()
        throws TException
    { 
        try {
            if (DEBUG) System.out.println(MESSAGE + "start build"
                    + " - outFile.length()=" + outFile.length()
            );
            if (outFile.exists() && (outFile.length() > 0)) {
                outFile.delete();
            }
            response = retrieve();
            contentLength = response.contentLength;
            
            if (DEBUG_HIGH) System.out.println(MESSAGE + "BUILD"
                        + " - container=" + container
                        + " - key=" + key
            );
            buildContentLength();
            setChecksum();
            if (DEBUG_HIGH) System.out.println(MESSAGE + "END "
                        + " - container=" + container
                        + " - key=" + key
                        + " - fileLength=" + outFile.length()
                        + " - contentLength=" + contentLength
                        + " - sha-256=" + sha_256
            );
        
        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "url2File - Exception:" + ex + " - name:" + outFile.getName();
            throw new TException.GENERAL_EXCEPTION( err);


        }
    }
            
    /**
     * This routine will perform multiple calls to fix broken connection
     * when content-length exists
     * @param entity response entity
     * @throws TException 
     */       
    public void buildContentLength()
        throws TException
    { 
        try {
            
            long length = 0;
            InputStream inStream = response.inputStream;
            int failCnt = 0;
            int startCnt = 0;
            if (contentLength == 0) {
                System.out.println("WARNING: zero lenght file:"
                        + " - container=" + container
                        + " - key=" + key
                );
                outFile.createNewFile();
                return;
            }
            while (length < contentLength) {
                startCnt++;
                long tryLength = outFile.length();
                try {
                    stream2File(inStream, outFile, true);
                    break;
                } catch (Exception ex) {
                    System.out.println("WARNING unable to copy all content:" 
                            + " - outfile.length="+ outFile.length()
                            + " - contentLength="+ contentLength
                            + " - Exception:" + ex
                                    );
                    if (outFile.length() == tryLength) {
                        failCnt++;
                        if (failCnt >= 3) {
                            throw new TException(ex);
                        }
                    } else {
                        failCnt = 0;
                    }
                }
                long startByte = outFile.length();
                long endByte = contentLength - 1;
                System.out.println("HttpGet(" + startCnt + "):"
                        + " - startByte=" + startByte
                        + " - endByte=" + endByte
                );
                inStream = toStream( startByte, endByte);
                length = outFile.length();
            }
            if (outFile.length() != contentLength) {
                throw new TException.INVALID_ARCHITECTURE(
                        MESSAGE + "File length not equal contentLength:"
                        + " - fileLength=" + outFile.length()
                        + " - contentLength=" + contentLength
                );
            }
        
        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "url2File - Exception:" + ex + " - name:" + outFile.getName();
            throw new TException.GENERAL_EXCEPTION( err);

        }
    }
    
    
    /**
     * Create a file from a stream
     * @param inStream stream used to create file
     * @param outFile file to create
     * @throws org.cdlib.mrt.utility.MException
     */
    public void stream2File(InputStream inStream, File outFile, boolean append)
        throws TException
    {

        FileOutputStream outStream = null;
        int len = 0;
        byte [] buf = new byte[BUFSIZE];
        try {
            outStream = new FileOutputStream(outFile, append);

            int cnt = 0;
            while ((len = inStream.read(buf)) >= 0) {
                if (DEBUG && (cnt < 10)) {
                    cnt++;
                    System.out.println("len=" + len);
                }
                outStream.write(buf, 0, len);
                algorithm.update(buf, 0, len);
            }
        
        } catch(Exception ex) {
            String err = MESSAGE + "Name:" + outFile.getName();
            if (DEBUG) {
                System.out.println("final len=" + len);
                ex.printStackTrace();
            }
            throw new TException.GENERAL_EXCEPTION( err, ex);


        } finally {
            try {
                //System.out.println("***FILE CLOSED***");
                inStream.close();
                outStream.close();
                
            } catch (Exception finex) { }
        }

    }
    
    public void setChecksum() 
    {
        byte[] digest = algorithm.digest();
        StringBuffer hexString = new StringBuffer();
        for (int i=0;i<digest.length;i++) {
            String val = Integer.toHexString(0xFF & digest[i]);
            if (val.length() == 1) val = "0" + val;
            hexString.append(val);
        }
        sha_256 = hexString.toString();
        response.sha_256 = this.sha_256;
        //System.out.println(MESSAGE + "setChecksum:"  + sha_256);
    }
    
    public InputStream toStream( long startByte, long endByte)
        throws TException
    {
        try {
            ResponseValues localResponse = retrieveShortRetry(container, key, startByte, endByte, timeout, 4);
            return localResponse.inputStream;

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "url2File - Exception:" + ex + " - name:" + outFile.getName();
            throw new TException.GENERAL_EXCEPTION( err);


        }

    }
    
    public ResponseValues retrieve()
        throws TException
    {
        try {
            response = retrieveRetry(container, key, CloudConst.LONG_TIMEOUT, 4);
            return response;

        } catch (TException fe) {
            throw fe;

        } catch(Exception ex) {
            String err = MESSAGE + "url2File - Exception:" + ex + " - name:" + outFile.getName();
            throw new TException.GENERAL_EXCEPTION( err);


        }

    }

    public ResponseValues getResponse() {
        return response;
    }
    
    
    public ResponseValues retrieveShortRetry(
            String container, 
            String key,
            long startByte,
            long endByte,
            int timeout,
            int retry)
        throws TException
    {
        TException saveTex = null;
        for (int itry=0; itry < retry; itry++) {
            try {
                return retrieveShort(container, key, startByte, endByte, timeout);
                
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

    public String getSha_256() {
        return sha_256;
    }

    public void setSha_256(String sha_256) {
        this.sha_256 = sha_256;
    }
}
