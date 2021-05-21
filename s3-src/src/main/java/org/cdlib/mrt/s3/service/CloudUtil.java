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
package org.cdlib.mrt.s3.service;

/**
 * Cloud storage utilities
 * @author dloy
 */


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.StringUtil;
import java.io.File;


import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TRuntimeException;

/**
 * Cloud Utilities
 */
public class CloudUtil {
    
    protected static final String NAME = "CloudUtil";
    protected static final String MESSAGE = NAME + ": ";
    public static final String KEYDELIM = "|";
    public static final String SPLITKEY = "\\|";
    public static final String HEXPREFIX = "-.";
    public static final String HEXSUFFIX = ".-";
    private static final boolean DEBUG = false;
    
    public static String getKey(
            Identifier objectID,
            Integer versionID,
            String fileID,
            boolean alphaNumeric)
        throws TException
    {
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getKey - objectID not provided");
        }
        String key = objectID.getValue();
        if (versionID != null) {
            key = key + KEYDELIM + versionID;
            if (!StringUtil.isAllBlank(fileID)) {
                key = key + KEYDELIM + fileID;
            }
        }
        return encodeElement(key, alphaNumeric);
    }
 
    
    public static String getManifestKey(
            Identifier objectID,
            boolean alphaNumeric)
        throws TException
    {
        if (DEBUG) System.out.println("getManifestKey");
        StringBuffer buf = new StringBuffer();
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getManifestKey - objectID not provided");
        }
        buf.append(objectID.getValue());
        buf.append(KEYDELIM);
        buf.append("manifest");
        
        return encodeElement(buf.toString(), alphaNumeric);
    }
    
    public static String encodeElement(String element, boolean alphaNumeric)
    {
        if (!alphaNumeric) return element;
        String alphanumeric = ""
                + "abcdefghijklmnopqrstuvwxyz"
                + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + ".-";
        StringBuffer buf = new StringBuffer();
        byte[] ba = null;
        for (int i=0; i < element.length(); i++) {
            String s = element.substring(i,i+1);
            if (alphanumeric.contains(s)) {
                buf.append(s);
                continue;
            }
            try {
                ba = s.getBytes("utf-8");
            } catch (Exception ex) {
                throw new TRuntimeException.INVALID_DATA_FORMAT("encodeElment excpetion:" + s);
            }
            buf.append(HEXPREFIX);
            for (byte b : ba) {
                buf.append(Integer.toHexString(0xFF & b));
            }
            buf.append(HEXSUFFIX);
        }
        return buf.toString();
    }
    
    public static String decodeElement(String element)
    {
        StringBuffer buf = new StringBuffer();
        int barPos = element.indexOf('|');
        if (barPos >= 0) return element;
        try {
            int start = 0;
            while (true) {
                int pos = element.substring(start).indexOf(HEXPREFIX);
                if (pos < 0) {
                    buf.append(element.substring(start));
                    break;
                }
                if (DEBUG) System.out.println(""
                        + " - start=" + start
                        + " - pos=" + pos
                        );
                if (pos > 0) {
                    buf.append(element.substring(start, start + pos));
                }
                int end = element.substring(start + pos + 2).indexOf(HEXSUFFIX);
                if (end < 0) {
                    buf.append(element.substring(start)); // error treat as data
                    break;
                }
                start += pos + 2;
                String encode = element.substring(start, start + end);
                String decode = hex2Char(encode);
                buf.append(decode);
                start += end + 2;
                if (start > element.length()) break;
                if (DEBUG) System.out.println("start=" + start + " - length=" + element.length());
            }
            return buf.toString();
            
        } catch (Exception ex) {
            System.out.println("Exception=" + ex);
            ex.printStackTrace();
            throw new TRuntimeException.GENERAL_EXCEPTION(ex);
        }
    }
    
    public static String decodeElementDotDot(String element)
    {
        StringBuffer buf = new StringBuffer();
        try {
            String[] parts = element.split("\\.\\.");
            if (parts.length == 1) return element;
            boolean hexFlag = false;
            for (String part : parts) {
                if (DEBUG) System.out.println("hexFlag=" + hexFlag + " - part=\"" + part + "\"");
                if (!hexFlag) {
                    buf.append(part);
                    hexFlag = true;
                } else {
                    String val = hex2Char(part);
                    buf.append(val);
                    hexFlag = false;
                }
                
            }
            return buf.toString();
            
        } catch (Exception ex) {
            throw new TRuntimeException.GENERAL_EXCEPTION(ex);
        }
    }
    
    public static byte[] hex2Byte(String str)
    {
       byte[] bytes = new byte[str.length() / 2];
       for (int i = 0; i < bytes.length; i++)
       {
          bytes[i] = (byte) Integer
                .parseInt(str.substring(2 * i, 2 * i + 2), 16);
       }
       return bytes;
    }
    
    public static String hex2Char(String part)
    {
        byte[] bytes = hex2Byte(part);
        String str = null;
        try {
            return new String(bytes, "utf-8");
        } catch (Exception ex) {
            throw new TRuntimeException.INVALID_DATA_FORMAT("hex2Char excpetion:" + part);
        }
    }
    
    public static String getDigestValue(
            File testFile,
            LoggerInf logger)
        throws TException
    {
        try {
            FixityTests fixityTest = new FixityTests(testFile, "md-5", logger);
            if (DEBUG) System.out.println("md5Hex=" + fixityTest.getChecksum());
            return fixityTest.getChecksum();

        } catch (Exception ex) {
            throw new TException(ex);

        }
    }
    
    public static String getDigestValue(
            String type,
            File testFile,
            LoggerInf logger)
        throws TException
    {
        try {
            FixityTests fixityTest = new FixityTests(testFile, type, logger);
            if (DEBUG) System.out.println(type + "=" + fixityTest.getChecksum());
            return fixityTest.getChecksum();

        } catch (Exception ex) {
            throw new TException(ex);

        }
    }
    
    public static KeyElements getKeyElements(String key)
        throws TException
    {
        try {
            if (StringUtil.isAllBlank(key)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getKeyElements - empty key");
            }
            KeyElements ele = new KeyElements();
            String keySave = new String(key);
            key = decodeElement(key);
            if (DEBUG) System.out.println("getKeyElements: key=" + key);
            String [] parts = key.split(SPLITKEY);
            if (DEBUG) System.out.println("parts length=" + parts.length);
            if (parts.length > 3) {
                for (int p=3; p < parts.length; p++) {
                    parts[2] += '|' + parts[p];
                }
            }
            ele.objectID = new Identifier(parts[0]);
            if (parts.length >= 2) {
                if (parts[1].startsWith("manifest")) ele.versionID = null;
                else {
                    try {
                        int version = Integer.parseInt(parts[1]);
                        ele.versionID = version;
                    } catch (Exception ex) {
                        throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getKeyElements - invalid version:" + parts[2]);
                    }
                }
            }
            if (parts.length > 2) {
                ele.fileID = parts[2];
            }
            return ele;
            
        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);

        }
        
    }
    
    public static KeyElements getManifestKeyElements(String key)
        throws TException
    {
        try {
            if (StringUtil.isAllBlank(key)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getManifestKeyElements - empty key");
            }
            KeyElements ele = new KeyElements();
            key = decodeElement(key);
            String [] parts = key.split(SPLITKEY);
            if ((parts.length < 2) || (parts.length > 3)) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getManifestKeyElements - split invalid - length=" + parts.length);
            }
            if (!parts[1].equals("manifest")) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getManifestKeyElements - manifest part missing- parts[1]=" + parts[1]);
            }
            
            ele.objectID = new Identifier(parts[0]);
            if (parts.length > 2) {
                try {
                    int version = Integer.parseInt(parts[2]);
                    ele.versionID = version;
                } catch (Exception ex) {
                    throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getManifestKeyElements - invalid version:" + parts[2]);
                }
            }
            return ele;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);

        }
        
    }
    
    public static class KeyElements {
        public Identifier objectID = null;
        public Integer versionID = null;
        public String fileID = null;
        public String dump(String header) {
            StringBuffer buf = new StringBuffer();
            buf.append("**" + header + "**");
            if (objectID == null) {
                buf.append("EMPTY element");
            }
            buf.append(" - objectID=" + objectID.toString());
            if (versionID != null) {
                buf.append(" - versionID=" + versionID);
            }
            if (fileID != null) {
                buf.append(" - fileID=" + fileID);
            }
            return buf.toString();
        }
    }
}
