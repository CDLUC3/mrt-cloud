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
import java.io.InputStream;
import java.util.Properties;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.Header;
import org.cdlib.mrt.cloud.CloudProperties;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;


    
public class ResponseValues
{
    public InputStream inputStream = null;
    public Properties responseProp = null;
    public int responseCode = 0;
    public int statusCode = 0;
    public long contentLength = 0;
    public String sha_256 = null;
    
    public ResponseValues(InputStream inputStream, Properties resultProp)
    {
        this.inputStream = inputStream;
        this.responseProp = resultProp;
    }
    
    public ResponseValues(HttpResponse response)
        throws TException
    {
        set(response, false);
    }

    public ResponseValues(HttpResponse response, boolean setInputStream)
        throws TException
    {
        set(response, setInputStream);
    }

    public void set(HttpResponse response, boolean setInputStream)
        throws TException
    {
        try {
            responseProp = new Properties();
            StatusLine statusLine = response.getStatusLine();
            if (response == null) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("HTTPUtil.response2Property - No response");
            }
            statusCode = statusLine.getStatusCode();
            responseProp.setProperty("response.status", "" + statusCode);
            responseProp.setProperty("response.statusline", "" + statusLine);
            Header [] headers = response.getAllHeaders();
            for (Header header : headers) {
                responseProp.setProperty(
                        "header." + header.getName(),
                        header.getValue());
            }

            HttpEntity entity = response.getEntity();
            if (entity != null) {
                contentLength = entity.getContentLength();
            }
            responseCode = response.getStatusLine().getStatusCode();
            
            if (setInputStream && (entity != null) && (responseCode >= 200 && responseCode < 300)) {
                this.inputStream = entity.getContent();
            }

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public Long getSize()
    {
        String sizeS = responseProp.getProperty("header.Content-Length");
        if (sizeS == null) return null;
        return Long.parseLong(sizeS);
    }

    public String getEtag()
    {
        return responseProp.getProperty("header.Etag");
    }
    
    public CloudProperties getCloudProperties()
    {
            CloudProperties cloudProp = new CloudProperties();
            cloudProp.setFromMetaProperties(responseProp);
            return cloudProp;
    }

    public String getSha_256() {
        return sha_256;
    }

    public void setSha_256(String sha_256) {
        this.sha_256 = sha_256;
    }

    public String dump(String header)
    {
        StringBuffer buf = new StringBuffer();
        buf.append("***" + header + "***");
        if (inputStream == null) buf.append("inputStream null\n");
        else buf.append("inputStream not null\n");
        buf.append(PropertiesUtil.dumpProperties("ResponseValues dump", responseProp, responseCode) + "\n");
        buf.append("responseCode=" + responseCode + "\n");
        return buf.toString();
    }
}
