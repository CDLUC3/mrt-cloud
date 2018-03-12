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
import java.util.ArrayList;
import java.util.Properties;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.Header;
import org.cdlib.mrt.cloud.CloudProperties;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;


    
public class SegmentValues
{
    private ArrayList<SegmentValue> list = new ArrayList<>();
    private String container = null;
    
    public SegmentValues(String container)
    {
        this.container = container;
    }

    public void add(String md5, long size, String key)
    {
        SegmentValue segmentValue = new SegmentValue(md5, size, key);
        list.add(segmentValue);
    }
    
    public String getJson1() 
    {
        /*
         * json:
    [{"path": "/cont/object",
      "etag": "etagoftheobjectsegment",
      "size_bytes": 1048576}, ...]
         */
        StringBuffer buf = new StringBuffer();
        buf.append("json:");
        int segs = list.size();
        for (int seg = 0; seg < list.size(); seg++) {
            SegmentValue value = list.get(seg);
            String keyName = "/" + container + "/" + value.key;
            buf.append("{");
            buf.append("\"path\": \"" + keyName + "\",");
            buf.append("\"etag\": \"" + value.md5 + "\",");
            buf.append("\"size_bytes\": " + value.size);
            buf.append("}");
            if (seg < (list.size() - 1)) buf.append(",");
        }
        buf.append("]");
        return buf.toString();
    }
    
    public String getJson2() 
    {
        /*
         * json:
    [{"path": "/cont/object",
      "etag": "etagoftheobjectsegment",
      "size_bytes": 1048576}, ...]
         */
        StringBuffer buf = new StringBuffer();
        //buf.append("json:");
        int segs = list.size();
        
        buf.append("{\n\"json:\" [\n");
        for (int seg = 0; seg < list.size(); seg++) {
            SegmentValue value = list.get(seg);
            String keyName = "/" + container + "/" + value.key;
            buf.append("    {");
            buf.append("\"path\": \"" + keyName + "\",");
            buf.append("\"etag\": \"" + value.md5 + "\",");
            buf.append("\"size_bytes\": " + value.size);
            buf.append("}");
            if (seg < (list.size() - 1)) buf.append(",");
            buf.append("\n");
        }
        buf.append("  ]\n}");
        return buf.toString();
    }
    
    public String getJson3() 
    {
        /*
         * json:
    [{"path": "/cont/object",
      "etag": "etagoftheobjectsegment",
      "size_bytes": 1048576}, ...]
         */
        StringBuffer buf = new StringBuffer();
        //buf.append("json:");
        int segs = list.size();
        
        buf.append("{\n [\n");
        for (int seg = 0; seg < list.size(); seg++) {
            SegmentValue value = list.get(seg);
            String keyName = "/" + container + "/" + value.key;
            buf.append("    {");
            buf.append("\"path\": \"" + keyName + "\",");
            buf.append("\"etag\": \"" + value.md5 + "\",");
            buf.append("\"size_bytes\": " + value.size);
            buf.append("}");
            if (seg < (list.size() - 1)) buf.append(",");
            buf.append("\n");
        }
        buf.append("  ]\n}");
        return buf.toString();
    }
    
    public String getJson() 
    {
        /*
         * json:
    [{"path": "/cont/object",
      "etag": "etagoftheobjectsegment",
      "size_bytes": 1048576}, ...]
         */
        StringBuffer buf = new StringBuffer();
        //buf.append("json:");
        int segs = list.size();
        
        buf.append("[");
        for (int seg = 0; seg < list.size(); seg++) {
            SegmentValue value = list.get(seg);
            String keyName = "/" + container + "/" + value.key;
            buf.append("{");
            buf.append("\"path\": \"" + keyName + "\",");
            buf.append("\"etag\": \"" + value.md5 + "\",");
            buf.append("\"size_bytes\": " + value.size);
            buf.append("}");
            if (seg < (list.size() - 1)) buf.append(",");
            //buf.append("\n");
        }
        buf.append("]");
        return buf.toString();
    }
    
    public String getXML() 
    {
        /*
         <?xml version="1.0" encoding="UTF-8"?>
            <static_large_object>
                <object_segment>
                    <path>/cont/object</path>
                    <etag>etagoftheobjectsegment</etag>
                     <size_bytes>100</size_bytes>
                </object_segment>
            </static_large_object>
         */
        StringBuffer buf = new StringBuffer();
        buf.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        buf.append("<static_large_object>\n");
        int segs = list.size();
        for (int seg = 0; seg < list.size(); seg++) {
            buf.append("  <object_segment>\n");
            SegmentValue value = list.get(seg);
            String keyName = "/" + container + "/" + value.key;
            buf.append("    <path>" + keyName + "</path>\n" );
            buf.append("    <etag>" + value.md5 + "</etag>\n" );
            buf.append("    <size_bytes>" + value.size + "</size_bytes>\n" );
            buf.append("  </object_segment>\n");
        }
        buf.append("</static_large_object>\n");
        return buf.toString();
    }
    
    public int cnt()
    {
        return list.size();
    }
    
    public long size()
    {
        long size = 0;
        for (int seg = 0; seg < list.size(); seg++) {
            SegmentValue value = list.get(seg);
            size += value.size;
        }
        return size;
        
    }
    
    public static class SegmentValue
    {
        public String md5 = null;
        public long size = 0;
        public String key = null;
        public SegmentValue(String md5, long size, String key)
        {
            this.md5 = md5;
            this.size = size;
            this.key = key;
        }
        public String dump() {
            return "SegmentValue:"
                    + " - key:" + key
                    + " - md5:" + md5
                    + " - size:" + size
                    ;
        }
    }
}
