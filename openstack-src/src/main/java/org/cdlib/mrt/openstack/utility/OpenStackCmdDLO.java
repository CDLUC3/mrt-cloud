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
import java.util.Properties;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.cdlib.mrt.cloud.CloudProperties;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

public class OpenStackCmdDLO 
        extends OpenStackCmdAbs
{
    protected static final String NAME = "OpenStackCmdDLO";
    protected static final String MESSAGE = NAME + ": ";
    protected static boolean DEBUG = false;
    
    public OpenStackCmdDLO(OpenStackAuth auth)
    {
        super(auth);
                
    } 
    
    public OpenStackCmdDLO(String user,String pwd,String host)
        throws TException
    {
        super(user, pwd, host);
    } 
    
    public OpenStackCmdDLO(Properties prop)
        throws TException
    {
        super(prop);
    }
    
    public SegmentValues deleteBig(
            XValues xValues,
            String container, 
            String key, 
            int timeout)
        throws TException
    {
        try {
            
            SegmentValues values = new SegmentValues(container);
            System.out.println("main..." 
                    + " - xAuthToken:" + xValues.xAuthToken
                    + " - xStorageUrl:" + xValues.xStorageUrl
                    + " - container:" + container
                    + " - key:" + key
                    );
            try {
                ResponseValues deleteValues = deleteSingle(
                        xValues, 
                        container,
                        key,
                        CloudConst.LONG_TIMEOUT);
            } catch (Exception ex) {
                System.out.println("WARNING: deletion manifest fails:" + key);
            }
            String segName = "seg-" + key;
            for (int i=1; true; i++) {
                String pad = OpenStackCmdDLO.getPadCnt(i);
                String name = segName + "/" + pad;
                ResponseValues responseValues = null;
                try {
                    responseValues = OpenStackCmdDLO.getMeta(
                            xValues, 
                            container,
                            name,
                            CloudConst.LONG_TIMEOUT);
                } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
                    break;
                }
                
                String etag = responseValues.getEtag();
                long segSize = responseValues.getSize();
                responseValues = deleteSingle(
                        xValues, 
                        container,
                        name,
                        CloudConst.LONG_TIMEOUT);
                values.add(etag, segSize, name);
                if (DEBUGHIGH) System.out.println(responseValues.dump("seg:" + name)
                        + " - size=" + responseValues.getSize()
                        );
            }
            return values;
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            throw new TException(ex);
        }
    }
    
    public ResponseValues uploadBigManifest(
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
            
            if (DEBUGLOW) System.out.println("uploadBigManifest..." 
                    + " - container:" + container
                    + " - key:" + key
                    );
            
            String requestURL = xValues.xStorageUrl 
                    + "/"+ URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(key, "utf-8");
            String segKey = getSegKey(key);
            String manifestHeader = URLEncoder.encode(container, "utf-8")
                    + "/"+ URLEncoder.encode(segKey, "utf-8")
                    + "/";
            HttpClient httpclient = getHttpClient(timeout);
            HttpPut httpput = new HttpPut(requestURL);
            httpput.setHeader("X-Auth-Token", xValues.xAuthToken);
            httpput.setHeader("X-Object-Manifest", manifestHeader);
            setMetaProp(httpput, metaProp);
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
}
