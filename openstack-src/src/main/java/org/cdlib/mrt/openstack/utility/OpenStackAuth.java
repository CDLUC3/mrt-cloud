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
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import static org.cdlib.mrt.openstack.utility.OpenStackCmdAbs.getHttpClient;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

public class OpenStackAuth {
    protected static final String NAME = "OpenStackAuth";
    protected static final String MESSAGE = NAME + ": ";
    protected static boolean DEBUG = false;
    
    protected String user = null;
    protected String pwd = null;
    protected String host = null;
    
    public OpenStackAuth(String user,String pwd,String host)
        throws TException
    {
        this.user = user;
        this.pwd = pwd;
        this.host = host;
        XValues test = getAuth(user, pwd, host, CloudConst.LONG_TIMEOUT);
    }
    
    public OpenStackAuth(Properties prop)
        throws TException
    {
        user = prop.getProperty("access_key");
        pwd = prop.getProperty("secret_key");
        host = prop.getProperty("host");
        XValues test = getAuth(user, pwd, host, CloudConst.LONG_TIMEOUT);
    }
    
    public XValues getXValues()
        throws TException
    {
        XValues auth = getAuth(user, pwd, host, CloudConst.LONG_TIMEOUT);
        if (DEBUG) System.out.println(MESSAGE + "getXValues"
                + " - xAuthToken" + auth.xAuthToken
                + " - xStorageUrl" + auth.xStorageUrl
                );
        return auth;
    }
    
    public XValues getXValuesRetry(int retry)
        throws TException
    {
        TException.USER_NOT_AUTHENTICATED authTException = null;
        for (int itry=1; itry <= retry; itry++) {
            try {
                XValues auth = getAuth(user, pwd, host, CloudConst.LONG_TIMEOUT);
                if (DEBUG) System.out.println(MESSAGE + "getXValues"
                        + " - xAuthToken" + auth.xAuthToken
                        + " - xStorageUrl" + auth.xStorageUrl
                        );
                return auth;

            } catch (TException.USER_NOT_AUTHENTICATED una) {
                authTException = una;
                long sleep = 60000 * itry;
                System.out.println("WARNING(" + itry + ") authentication fails - sleep=" + sleep);
                try {
                    Thread.sleep(sleep);
                } catch (Exception ex) { }
            }
        }
        throw authTException;
    }
    
    public static XValues getAuth(String user, String pwd, String host, int timeout)
        throws TException
    {
        if (!host.startsWith("http")) {
            host = "https://" + host;
        }
        if (DEBUG) System.out.println(MESSAGE + "getXValues parms"
                + " - user=" + user
                + " - host=" + host
                + " - timeout=" + timeout
                );
        if (StringUtil.isAllBlank(user)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "geAuth - user missing");
        }
        if (StringUtil.isAllBlank(pwd)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "geAuth - pwd missing");
        }
        if (StringUtil.isAllBlank(host)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "geAuth - host missing");
        }

        //System.out.println("!!!!:" + MESSAGE + "getSerializeObject.requestURL=" + requestURL);
        try {
            String requestURL = host + "/auth/v1.0";
            if (DEBUG) System.out.println(MESSAGE + "requestURL=" + requestURL);
            HttpClient httpclient = getHttpClient(timeout);
            HttpGet httpget = new HttpGet(requestURL);
            httpget.setHeader("X-Storage-User", user);
            httpget.setHeader("X-Storage-Pass", pwd);
            HttpResponse response = httpclient.execute(httpget);
            Properties responseProp = HTTPUtil.response2Property(response);
            return new XValues(responseProp);
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);

            ex.printStackTrace();
            throw new TException.USER_NOT_AUTHENTICATED(MESSAGE 
                + " - user=" + user
                + " - host=" + host
                + " - timeout=" + timeout
                + " - Exception:" + ex
                 );
        }
    }

    public String getOpenStackHost() {
        return host;
    }
}
