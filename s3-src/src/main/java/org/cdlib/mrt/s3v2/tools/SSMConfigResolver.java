/*
Copyright (c) 2005-2020, Regents of the University of California
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

package org.cdlib.mrt.s3v2.tools;

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;

import org.cdlib.mrt.utility.TException;

/**
 *
 * @author DLoy
 * Class used to
 *
 */
public class SSMConfigResolver extends DefaultConfigResolver
{
    private AWSSimpleSystemsManagement ssm = null;
    private Exception serviceException = null;

    public SSMConfigResolver(String prefix)
    {
    	super(prefix);
        setSSM();
    }

    public SSMConfigResolver()
    {
        super();
        setSSM();
    }

    private void setSSM()
    {
        if (System.getenv("SSM_SKIP_RESOLUTION") != null) {
            return;
        }
        try {
            ssm = AWSSimpleSystemsManagementClientBuilder.defaultClient();
        } catch (Exception ex) {
            ssm = null;
            serviceException = ex;
        }
    }

    public String getResolvedValue(String parameterName)
        throws TException
    {
        if (ssm == null) {
            throw new TException.EXTERNAL_SERVICE_UNAVAILABLE("SSM service not available");
        }
        GetParameterRequest request = new GetParameterRequest();
        String searchName = getKey(parameterName);
        request.setName(searchName);
        request.setWithDecryption(true);
        return ssm.getParameter(request).getParameter().getValue();
    }

    public Exception getServiceException() {
        return serviceException;
    }

}
